package sinker

import (
	"context"
	"fmt"
	"github.com/streamingfast/substreams-sink-burster-proxy/ring"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams/client"

	"github.com/streamingfast/substreams/manifest"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
)

const (
	BLOCK_PROGRESS      = 1000
	LIVE_BLOCK_PROGRESS = 1
)

type MemorySinker struct {
	*shutter.Shutter

	Pkg              *pbsubstreams.Package
	OutputModule     *pbsubstreams.Module
	OutputModuleName string
	OutputModuleHash manifest.ModuleHash
	ClientConfig     *client.SubstreamsClientConfig

	UndoBufferSize  int
	LivenessTracker *sink.LivenessChecker

	sink       *sink.Sinker
	lastCursor *sink.Cursor

	blockRange *bstream.Range

	logger *zap.Logger
	tracer logging.Tracer
}

type Config struct {
	UndoBufferSize     int
	LiveBlockTimeDelta time.Duration

	BlockRange       string
	Pkg              *pbsubstreams.Package
	OutputModule     *pbsubstreams.Module
	OutputModuleName string
	OutputModuleHash manifest.ModuleHash
	ClientConfig     *client.SubstreamsClientConfig
}

// New Create a new memorySinker
func New(config *Config, logger *zap.Logger, tracer logging.Tracer) (*MemorySinker, error) {
	s := &MemorySinker{
		Shutter: shutter.New(),
		logger:  logger,
		tracer:  tracer,

		Pkg:              config.Pkg,
		OutputModule:     config.OutputModule,
		OutputModuleName: config.OutputModuleName,
		OutputModuleHash: config.OutputModuleHash,
		ClientConfig:     config.ClientConfig,
		blockRange:       bstream.NewOpenRange(0),
		lastCursor:       sink.NewBlankCursor(),

		UndoBufferSize:  config.UndoBufferSize,
		LivenessTracker: sink.NewLivenessChecker(config.LiveBlockTimeDelta),
	}

	s.OnTerminating(func(err error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.Stop(ctx, err)
	})

	return s, nil
}

// Stop the memorySinker
func (s *MemorySinker) Stop(ctx context.Context, err error) {
	if s.lastCursor == nil || err != nil {
		return
	}

}

// Start the memorySinker
func (s *MemorySinker) Start(ctx context.Context) error {
	return s.Run(ctx)
}

// Run the memorySinker
func (s *MemorySinker) Run(ctx context.Context) error {
	var err error
	s.sink, err = sink.New(
		sink.SubstreamsModeProduction,
		s.Pkg.Modules,
		s.OutputModule,
		s.OutputModuleHash,
		s.handleBlockScopeData,
		s.ClientConfig,
		[]pbsubstreams.ForkStep{
			pbsubstreams.ForkStep_STEP_NEW,
			pbsubstreams.ForkStep_STEP_UNDO,
			pbsubstreams.ForkStep_STEP_IRREVERSIBLE,
		},
		s.logger,
		s.tracer,
	)
	if err != nil {
		return err
	}
	if err != nil {
		return fmt.Errorf("unable to create sink: %w", err)
	}

	s.sink.OnTerminating(s.Shutdown)
	s.OnTerminating(func(err error) {
		s.logger.Info("terminating sink")
		s.sink.Shutdown(err)
	})

	if err := s.sink.Start(ctx, s.blockRange, s.lastCursor); err != nil {
		return fmt.Errorf("sink failed: %w", err)
	}

	return nil
}

func (s *MemorySinker) handleBlockScopeData(ctx context.Context, cursor *sink.Cursor, data *pbsubstreams.BlockScopedData) error {

	for _, output := range data.Outputs {
		if output.Name != s.OutputModuleName {
			continue
		}

		// update live block to see if client is up-to-date
		ring.SetPotentialLiveBlock(output)

		// update block data buffer and move marker
		ring.BlockDataBuffer.Value = output
		ring.BlockDataBuffer = ring.BlockDataBuffer.Next()

		// if buffer ring is full, unlink the oldest block & ++
		if ring.BlockDataBufferCount >= ring.BlockDataBuffer.Len() {
			ring.BlockDataBuffer.Unlink(1)
		}
		ring.BlockDataBufferCount++
	}

	s.lastCursor = cursor

	return nil
}

func (s *MemorySinker) batchBlockModulo(blockData *pbsubstreams.BlockScopedData) uint64 {
	if s.LivenessTracker.IsLive(blockData) {
		return LIVE_BLOCK_PROGRESS
	}
	return BLOCK_PROGRESS
}
