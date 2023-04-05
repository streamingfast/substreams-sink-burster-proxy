package ring

import (
	"fmt"
	"github.com/streamingfast/dmetrics/ring"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

var BlockDataBufferCount = 0
var BlockDataBuffer = ring.New[*pbsubstreams.ModuleOutput](1000)

var LiveBlockData *pbsubstreams.ModuleOutput

func SetPotentialLiveBlock(block *pbsubstreams.ModuleOutput) {
	if block != LiveBlockData {
		LiveBlockData = block
	}
}

func GetRingAtBlock(r *ring.Ring[*pbsubstreams.ModuleOutput], block *pbsubstreams.ModuleOutput) (error, ring.Ring[*pbsubstreams.ModuleOutput]) {
	startRing := r
	curRing := r
	for {
		if curRing.Value == block {
			return nil, *r
		}
		if curRing.Next() == startRing {
			return fmt.Errorf("block not found in ring"), *r
		}
		curRing = curRing.Next()
	}
}

func NewSubscription(channel chan *pbsubstreams.ModuleOutput) {
	go func() {
		for {
			channel <- LiveBlockData
		}
	}()
}

/*
func CancelSubscription(channel chan *pbsubstreams.ModuleOutput) {
	close(channel)
}
*/
