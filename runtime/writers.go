package runtime

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nyaruka/gocommon/aws/dynamo"
)

// DynamoWriter is the interface for DynamoDB writers (real or no-op).
type DynamoWriter interface {
	Queue(i dynamo.ItemMarshaler) (int, error)
	Start()
	Stop()
}

type Writers struct {
	Main    DynamoWriter
	History DynamoWriter
}

func newWriters(cfg *Config, cl *dynamodb.Client, spool *dynamo.Spool) *Writers {
	if cfg.DynamoTablePrefix == "" {
		return &Writers{
			Main:    &NopWriter{},
			History: &NopWriter{},
		}
	}
	return &Writers{
		Main:    dynamo.NewWriter(cl, cfg.DynamoTablePrefix+"Main", 250*time.Millisecond, 1000, spool),
		History: dynamo.NewWriter(cl, cfg.DynamoTablePrefix+"History", 250*time.Millisecond, 1000, spool),
	}
}

func (w *Writers) start() {
	w.Main.Start()
	w.History.Start()
}

func (w *Writers) stop() {
	w.Main.Stop()
	w.History.Stop()
}
