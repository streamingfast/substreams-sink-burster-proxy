package main

import (
	"github.com/spf13/viper"
	"os"
)

func readAPIToken() string {
	apiToken := viper.GetString("api-token")
	if apiToken != "" {
		return apiToken
	}

	apiToken = os.Getenv("SUBSTREAMS_API_TOKEN")
	if apiToken != "" {
		return apiToken
	}

	return os.Getenv("SF_API_TOKEN")
}
