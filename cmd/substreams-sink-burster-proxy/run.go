package main

import (
	"context"
	"fmt"
	"github.com/spf13/viper"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/derr"
	"github.com/streamingfast/substreams-sink-burster-proxy/sinker"
	"github.com/streamingfast/substreams/client"
	"github.com/streamingfast/substreams/manifest"
	"go.uber.org/zap"
	"time"

	"github.com/spf13/cobra"
	"github.com/streamingfast/shutter"
)

func bursterRunCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run <endoint> <spkg> <listening_port>",
		Short: "run the burster substream sink",
		Args:  cobra.ExactArgs(3),
		RunE:  bursterRunE,
	}

	return cmd
}

func init() {
	rootCmd.AddCommand(bursterRunCmd())
}

func bursterRunE(cmd *cobra.Command, args []string) error {

	app := shutter.New()

	ctx, cancelApp := context.WithCancel(cmd.Context())
	app.OnTerminating(func(_ error) {
		cancelApp()
	})

	endpoint := args[0]
	spkgName := args[1]
	listeningPort := args[2]

	// Check valid spkg containing sink config
	zlog.Info("reading substreams manifest", zap.String("manifest_path", spkgName))
	pkg, err := manifest.NewReader(spkgName).Read()
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	if pkg.SinkConfig == nil {
		return fmt.Errorf("no sink config found in spkg")
	}

	// Check valid output module name
	graph, err := manifest.NewModuleGraph(pkg.Modules.Modules)
	if err != nil {
		return fmt.Errorf("create substreams moduel graph: %w", err)
	}

	outputModuleName := pkg.SinkModule
	zlog.Info("validating output store", zap.String("output_store", outputModuleName))
	module, err := graph.Module(outputModuleName)
	if err != nil {
		return fmt.Errorf("get output module %q: %w", outputModuleName, err)
	}
	if module.GetKindMap() == nil {
		return fmt.Errorf("ouput module %q is *not* of  type 'Mapper'", outputModuleName)
	}

	apiToken := readAPIToken()

	liveBlockTimeDelta, err := time.ParseDuration(fmt.Sprintf("%ds", viper.GetInt("run-live-block-time-delta")))
	if err != nil {
		return fmt.Errorf("parsing live-block-time-delta: %w", err)
	}
	hashes := manifest.NewModuleHashes()
	outputModuleHash := hashes.HashModule(pkg.Modules, module, graph)

	// Establish config for sinker
	config := &sinker.Config{
		UndoBufferSize:     viper.GetInt("run-undo-buffer-size"),
		LiveBlockTimeDelta: liveBlockTimeDelta,

		Pkg:              pkg,
		OutputModule:     module,
		OutputModuleName: outputModuleName,
		OutputModuleHash: outputModuleHash,
		ClientConfig: client.NewSubstreamsClientConfig(
			endpoint,
			apiToken,
			viper.GetBool("run-insecure"),
			viper.GetBool("run-plaintext"),
		),
	}

	// Create our memorySinker
	memorySinker, err := sinker.New(config, zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to create sinker: %w", err)
	}
	memorySinker.OnTerminating(app.Shutdown)

	go func() {
		if err := ServeFunc(listeningPort); err != nil {
			zlog.Error("serving burster func failed", zap.Error(err))
			memorySinker.Shutdown(err)
		}
	}()

	go func() {
		if err := memorySinker.Start(ctx); err != nil {
			zlog.Error("sinker start failed", zap.Error(err))
			memorySinker.Shutdown(err)
		}
	}()

	// Handle signals and wait for termination
	signalHandler := derr.SetupSignalHandler(0 * time.Second)
	zlog.Info("ready, waiting for signal to quit")
	select {
	case <-signalHandler:
		zlog.Info("received termination signal, quitting application")
		go app.Shutdown(nil)
	case <-app.Terminating():
		NoError(app.Err(), "application shutdown unexpectedly, quitting")
	}

	zlog.Info("waiting for app termination")
	select {
	case <-app.Terminated():
	case <-ctx.Done():
	case <-time.After(30 * time.Second):
		zlog.Error("application did not terminated within 30s, forcing exit")
	}

	return nil
}
