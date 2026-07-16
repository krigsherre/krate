package peer

import (
	"context"
	"log/slog"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"

	kratev1 "github.com/krigsherre/krate/peer/peerpb"
)

type TokenServer struct {
	kratev1.UnimplementedKratePeerServiceServer

	instanceID      string
	port            string
	logger          *slog.Logger
	tokenAccessor   func(key string, requested uint64) (uint64, error)
	tokenTransferer func(key string, amount uint64) (uint64, error)
	gossipHandler   func(originID string, consumed map[string]uint64, borrowed map[string]uint64) error

	server *grpc.Server
	lis    net.Listener
	mu     sync.Mutex
}

func NewTokenServer(instanceID, port string, logger *slog.Logger) *TokenServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &TokenServer{
		instanceID: instanceID,
		port:       port,
		logger:     logger,
	}
}

func (s *TokenServer) SetTokenAccessor(fn func(key string, requested uint64) (uint64, error)) {
	s.tokenAccessor = fn
}

func (s *TokenServer) SetTokenTransferer(fn func(key string, amount uint64) (uint64, error)) {
	s.tokenTransferer = fn
}

func (s *TokenServer) SetGossipHandler(fn func(originID string, consumed map[string]uint64, borrowed map[string]uint64) error) {
	s.gossipHandler = fn
}

func (s *TokenServer) Start() error {
	lis, err := net.Listen("tcp", s.port)
	if err != nil {
		return err
	}
	s.lis = lis

	s.server = grpc.NewServer()
	kratev1.RegisterKratePeerServiceServer(s.server, s)

	go func() {
		if err := s.server.Serve(lis); err != nil {
			s.logger.Error("gRPC serve error", "error", err)
		}
	}()

	s.logger.Info("token server started", "addr", lis.Addr().String())
	return nil
}

func (s *TokenServer) Addr() string {
	if s.lis != nil {
		return s.lis.Addr().String()
	}
	return ""
}

func (s *TokenServer) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}

func (s *TokenServer) TransferTokens(ctx context.Context, req *kratev1.TransferRequest) (*kratev1.TransferResponse, error) {
	if err := ValidateTransferRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if req.OriginId == s.instanceID {
		return nil, status.Error(codes.FailedPrecondition, "cycle detected")
	}

	if s.tokenAccessor == nil || s.tokenTransferer == nil {
		return nil, status.Error(codes.Internal, "token functions not configured")
	}

	surplus, err := s.tokenAccessor(req.LimitKey, req.Requested)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if surplus == 0 {
		return &kratev1.TransferResponse{Granted: 0}, nil
	}

	amount := req.Requested
	if amount > surplus {
		amount = surplus
	}

	granted, err := s.tokenTransferer(req.LimitKey, amount)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &kratev1.TransferResponse{Granted: granted}, nil
}

func (s *TokenServer) Ping(ctx context.Context, req *kratev1.PingRequest) (*kratev1.PingResponse, error) {
	return &kratev1.PingResponse{}, nil
}

func (s *TokenServer) Gossip(ctx context.Context, req *kratev1.GossipRequest) (*kratev1.GossipResponse, error) {
	if s.gossipHandler != nil {
		if err := s.gossipHandler(req.OriginId, req.GetConsumed(), req.GetBorrowed()); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &kratev1.GossipResponse{}, nil
}
