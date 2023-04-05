package main

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	proto "github.com/streamingfast/substreams-sink-burster-proxy/proto/gen"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
)

func bursterBurstCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "burst <blocks_from_head> <listening_port>",
		Short: "connect to the blockstream",
		Args:  cobra.RangeArgs(0, 2),
		RunE:  bursterBurstE,
	}

	return cmd
}

func init() {
	rootCmd.AddCommand(bursterBurstCmd())
}

func bursterBurstE(cmd *cobra.Command, args []string) error {
	blocksFromHead := args[0]
	_ = blocksFromHead

	listeningPort := args[1]

	zlog.Info(fmt.Sprintf("connecting to substreams sink at port %s...", listeningPort))
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%s", listeningPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		zlog.Error("failed to connect to substreams sink", zap.Error(err))
		return fmt.Errorf("failed to connect to substreams sink: %w", err)
	}

	zlog.Info("creating stream client...")
	client := proto.NewStreamClient(conn)

	zlog.Info("starting stream",
		zap.String("port", listeningPort),
		zap.String("blocks_from_head", blocksFromHead),
	)

	streamFromRing(client)

	return nil
}

func streamFromRing(client proto.StreamClient) error {
	ctx := context.Background()
	stream, err := client.StreamBuffer(ctx, &proto.Request{

		// Burst irrelevant for now
		// TODO change to a flag
		Burst: 1,
	})
	if err != nil {
		return fmt.Errorf("stream buffer: %w", err)
	}
	defer stream.CloseSend()

	for {
		buffer, err := stream.Recv()
		if buffer == nil {
			continue
		}
		if err == io.EOF {
			return fmt.Errorf("stream ended: %w", err)
		}
		if err != nil {
			return fmt.Errorf("stream receiving: %w", err)
		}

		// Print the value received from the server
		fmt.Println(buffer)
	}
}
