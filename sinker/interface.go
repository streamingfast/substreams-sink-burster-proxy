package sinker

import (
	"context"
	sink "github.com/streamingfast/substreams-sink"
)

type Loader interface {
	Flush(ctx context.Context, cursor *sink.Cursor) (count int, err error)
	GetCursor(ctx context.Context) (*sink.Cursor, error)
	WriteCursor(ctx context.Context, c *sink.Cursor) error
}
