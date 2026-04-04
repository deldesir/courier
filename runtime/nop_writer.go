package runtime

import (
	"log/slog"

	"github.com/nyaruka/gocommon/aws/dynamo"
)

// NopWriter is a no-op DynamoDB writer that discards all queued items.
// Used when DynamoDB is disabled (nanoRP local-first mode).
type NopWriter struct{}

// Queue accepts an item but discards it, returning max capacity.
func (w *NopWriter) Queue(i dynamo.ItemMarshaler) (int, error) {
	return 1000, nil
}

// Start is a no-op.
func (w *NopWriter) Start() {
	slog.Info("dynamo writer disabled (nanoRP mode)")
}

// Stop is a no-op.
func (w *NopWriter) Stop() {}

// Flush is a no-op.
func (w *NopWriter) Flush() {}
