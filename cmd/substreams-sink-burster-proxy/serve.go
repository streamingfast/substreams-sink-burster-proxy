package main

import (
	"fmt"
	proto "github.com/streamingfast/substreams-sink-burster-proxy/proto/gen"
	"github.com/streamingfast/substreams-sink-burster-proxy/ring"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"google.golang.org/grpc"
	"net"
)

type server struct {
	proto.StreamServer
}

func ServeFunc(listeningPort string) error {

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", listeningPort))
	if err != nil {
		return fmt.Errorf("listening: %w", err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterStreamServer(grpcServer, &server{})

	zlog.Info(fmt.Sprintf("Burster is running on port %s ...", listeningPort))
	if err := grpcServer.Serve(listener); err != nil {
		return fmt.Errorf("serving: %w", err)
	}

	return nil
}

func (s *server) StreamBuffer(req *proto.Request, stream proto.Stream_StreamBufferServer) error {

	// Get burst amount
	blocksFromHead := req.Burst
	_ = blocksFromHead

	// Create a new channel subscription
	// This channel will not be consumed until live.
	newBlockInRing := make(chan *pbsubstreams.ModuleOutput, 1)
	ring.NewSubscription(newBlockInRing)
	// Defer a close at end of connection or on timeout.

	// Potential problems
	// If two seconds hasn't been consumed, unblock and drop the block.
	// Or clear and drop subscription entirely. (more extreme) could be a flag

	// Get live block, go .Prev() x times and start consuming from buffer until
	// match liveblock, then get from chan

	// Go through buff until live
	/*for {
		if curBlock == newBlockInRing {
			break
		} else {
			stream.Send(curBlock)
			curBlock = curBlock.Next()
		}
	}*/

	return nil
}
