package main

import (
	"github.com/streamingfast/logging"
)

var zlog, tracer = logging.RootLogger("sink-burster-proxy", "github.com/streamingfast/substreams-sink-burster-proxy/cmd/substreams-sink-burster-proxy")

func main() {

	if err := rootCmd.Execute(); err != nil {

	}
}

func init() {
	logging.InstantiateLoggers()
}
