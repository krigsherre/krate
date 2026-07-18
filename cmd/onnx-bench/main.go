package main

import (
	"fmt"
	"log"
	"time"

	"github.com/yalue/onnxruntime_go"
)

func main() {
	// Homebrew Apple Silicon path
	onnxruntime_go.SetSharedLibraryPath("/opt/homebrew/lib/libonnxruntime.dylib")

	err := onnxruntime_go.InitializeEnvironment()
	if err != nil {
		log.Fatalf("Failed to init ONNX: %v", err)
	}
	defer onnxruntime_go.DestroyEnvironment()

	// 1. Create Tensors FIRST (Before the session)
	inputData := []float32{100.0, 15.0, 5.0, 200.0, 35.0}
	outputData := make([]float32, 3)

	inputTensor, err := onnxruntime_go.NewTensor([]int64{1, 5}, inputData)
	if err != nil {
		log.Fatalf("Input tensor error: %v", err)
	}
	defer inputTensor.Destroy()

	outputTensor, err := onnxruntime_go.NewTensor([]int64{1, 3}, outputData)
	if err != nil {
		log.Fatalf("Output tensor error: %v", err)
	}
	defer outputTensor.Destroy()

	// 2. Bind Tensors into the Session Constructor
	session, err := onnxruntime_go.NewAdvancedSession(
		"ml/dummy_router.onnx", // Path relative to root directory
		[]string{"state_input"},
		[]string{"q_values"},
		[]onnxruntime_go.ArbitraryTensor{inputTensor},
		[]onnxruntime_go.ArbitraryTensor{outputTensor},
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to load session: %v", err)
	}
	defer session.Destroy()

	iterations := 10000
	start := time.Now()

	for i := 0; i < iterations; i++ {
		// 3. session.Run() now takes zero arguments!
		// It automatically reads from inputData and writes to outputData
		err = session.Run()
		if err != nil {
			log.Fatalf("Inference failed: %v", err)
		}
	}

	elapsed := time.Since(start)
	avgLatency := elapsed / time.Duration(iterations)

	fmt.Printf("Total time for %d runs: %v\n", iterations, elapsed)
	fmt.Printf("Average latency per request: %v\n", avgLatency)
}