package main

import "github.com/spf13/cobra"

var rootCmd = &cobra.Command{
	Use:          "substreams-sink-burster-proxy",
	Short:        "a burster for stored in buffer substreams output",
	SilenceUsage: true,
}

func init() {

}
