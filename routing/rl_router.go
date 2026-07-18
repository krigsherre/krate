package routing

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/krigsherre/krate/internal/sketch"
	"github.com/yalue/onnxruntime_go"
)

var (
	envMu         sync.Mutex
	activeRouters int
)

// inferenceWorker encapsulates all C-allocated memory and a dedicated ONNX session 
// for a single parallel inference pass without lock contention.
type inferenceWorker struct {
	inputData    []float32
	outputData   []float32
	inputTensor  *onnxruntime_go.Tensor[float32]
	outputTensor *onnxruntime_go.Tensor[float32]
	session      *onnxruntime_go.AdvancedSession
}

func (w *inferenceWorker) destroy() {
	if w.session != nil {
		w.session.Destroy()
	}
	if w.inputTensor != nil {
		w.inputTensor.Destroy()
	}
	if w.outputTensor != nil {
		w.outputTensor.Destroy()
	}
}

type RLPredictiveRouter struct {
	workerPool chan *inferenceWorker
	logger     *slog.Logger
	gossiper   *sketch.Gossiper
}

func NewRLPredictiveRouter(modelPath string, libPath string) (*RLPredictiveRouter, error) {
	envMu.Lock()
	if activeRouters == 0 {
		onnxruntime_go.SetSharedLibraryPath(libPath)
		if err := onnxruntime_go.InitializeEnvironment(); err != nil {
			envMu.Unlock()
			return nil, err
		}
	}
	activeRouters++
	envMu.Unlock()

	// We pre-allocate a pool of 100 independent ONNX sessions (inference workers)
	// to support 100 perfectly parallel concurrent inference requests lock-free.
	poolSize := 100
	workerPool := make(chan *inferenceWorker, poolSize)

	for i := 0; i < poolSize; i++ {
		inData := make([]float32, 5)
		outData := make([]float32, 3)
		inTensor, err1 := onnxruntime_go.NewTensor([]int64{1, 5}, inData)
		outTensor, err2 := onnxruntime_go.NewTensor([]int64{1, 3}, outData)
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("failed to allocate tensor: %v, %v", err1, err2)
		}

		session, err := onnxruntime_go.NewAdvancedSession(
			modelPath,
			[]string{"state_input"},
			[]string{"q_values"},
			[]onnxruntime_go.ArbitraryTensor{inTensor},
			[]onnxruntime_go.ArbitraryTensor{outTensor},
			nil,
		)
		if err != nil {
			// Memory cleanup on failure
			inTensor.Destroy()
			outTensor.Destroy()
			return nil, fmt.Errorf("failed to create ONNX session %d: %v", i, err)
		}
		
		workerPool <- &inferenceWorker{
			inputData:    inData,
			outputData:   outData,
			inputTensor:  inTensor,
			outputTensor: outTensor,
			session:      session,
		}
	}

	return &RLPredictiveRouter{
		workerPool: workerPool,
	}, nil
}

// 1. Init: Krate calls this during startup in limiter.go
func (r *RLPredictiveRouter) Init(g *sketch.Gossiper, logger *slog.Logger) {
	r.gossiper = g
	r.logger = logger
}

// 2. Decide: Krate calls this on every rate limit check
// Decide processes the context and executes the inference pass concurrently
func (r *RLPredictiveRouter) Decide(ctx context.Context, rc *RouteContext) (Decision, error) {
	// 1. Base case: No peers available globally
	if !rc.HasPeers {
		if rc.RedisExhausted {
			return DecisionDeny, nil
		}
		return DecisionRedis, nil
	}

	// 2. Fetch the best candidate peer Krate would theoretically route to
	topPeers := r.gossiper.TopK(1, rc.Key)
	if len(topPeers) == 0 {
		if rc.RedisExhausted {
			return DecisionDeny, nil
		}
		return DecisionRedis, nil
	}

	bestPeer := topPeers[0]

	// --- 3. ACQUIRE THREAD-SAFE WORKER FROM POOL ---
	var worker *inferenceWorker
	select {
	case worker = <-r.workerPool:
	case <-ctx.Done():
		return DecisionRedis, ctx.Err()
	}

	// Always return the worker to the pool when we are done
	defer func() {
		r.workerPool <- worker
	}()

	// --- 4. MAP LIVE DATA TO THE NEURAL NETWORK ---
	
	// Feature 0: The requested amount of tokens
	worker.inputData[0] = float32(rc.Need)
	
	// Feature 1: Check Redis health
	if rc.RedisExhausted {
		worker.inputData[1] = 1.0
	} else {
		worker.inputData[1] = 0.0
	}

	// Feature 2: Best Peer's Estimated Surplus
	worker.inputData[2] = float32(bestPeer.Surplus)

	// Feature 3: Staleness (Delta T in seconds)
	staleness := time.Since(bestPeer.LastUpdated).Seconds()
	if staleness < 0 {
		staleness = 0
	}
	worker.inputData[3] = float32(staleness)

	// Feature 4: Velocity Proxy
	worker.inputData[4] = 0.5

	// --- 5. EXECUTE THE NEURAL NETWORK ---
	if err := worker.session.Run(); err != nil {
		if r.logger != nil {
			r.logger.Warn("RL prediction crash, reverting to safe Redis fallback", "error", err)
		}
		return DecisionRedis, nil
	}

	// --- 6. EXTRACT BEST ACTION (Argmax) ---
	bestAction := 0
	maxQ := worker.outputData[0]
	for i := 1; i < len(worker.outputData); i++ {
		if worker.outputData[i] > maxQ {
			maxQ = worker.outputData[i]
			bestAction = i
		}
	}

	// --- 7. MAP TO KRATE ROUTING LOGIC ---
	switch bestAction {
	case 0, 1: 
		return DecisionPeer, nil
	case 2: 
		if rc.RedisExhausted {
			return DecisionDeny, nil
		}
		return DecisionRedis, nil
	default:
		return DecisionRedis, nil
	}
}

// 3. EvictKeys: Krate's background janitor calls this
func (r *RLPredictiveRouter) EvictKeys(keys []string) {}

// Close: We must call this manually on shutdown to free C-memory
func (r *RLPredictiveRouter) Close() {
	// Drain the pool and destroy every pre-allocated C-tensor and session
	close(r.workerPool)
	for worker := range r.workerPool {
		worker.destroy()
	}

	envMu.Lock()
	activeRouters--
	if activeRouters == 0 {
		onnxruntime_go.DestroyEnvironment()
	}
	envMu.Unlock()
}