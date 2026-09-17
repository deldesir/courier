package wuzapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/nyaruka/courier/v26/core/channels"
	"github.com/nyaruka/courier/v26/core/models"
	"github.com/nyaruka/courier/v26/runtime"
	"github.com/nyaruka/courier/v26/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests cover the receive seam only: what a webhook is parsed into and what it's handled as. They need
// no database - nothing here writes the batch - just a stand-in for the WuzAPI server the handler calls back.

type wuzapiStub struct {
	*httptest.Server
	mu    sync.Mutex
	calls []string // "<path> <body>"
}

func newWuzapiStub(t *testing.T) *wuzapiStub {
	s := &wuzapiStub{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.calls = append(s.calls, r.URL.Path+" "+string(body))
		s.mu.Unlock()
		w.Write([]byte(`{"success":true}`))
	}))
	t.Cleanup(s.Close)
	return s
}

func receive(t *testing.T, body string) (*channels.Received, error, *wuzapiStub) {
	stub := newWuzapiStub(t)

	rt := &runtime.Runtime{Config: runtime.NewDefaultConfig()}
	routes := channels.NewRoutes()
	h := NewHandler(rt, routes).(*WuzapiHandler)

	ch := test.NewMockChannel("8eb23e93-5ecb-45ba-b726-3b064e0c56ab", "WZ", "50912345678", "HT", []string{"whatsapp"},
		map[string]any{"wuzapi_url": stub.URL, "wuzapi_token": "sesame"})
	clog := models.NewChannelLogForIncoming(models.ChannelLogTypeReceive, ch, nil, h.RedactValues(ch))

	req := httptest.NewRequest(http.MethodPost, "/c/wz/8eb23e93-5ecb-45ba-b726-3b064e0c56ab/receive", strings.NewReader(body))
	in := channels.NewReceived(ch).As(channels.ReceiveKindAny)

	err := h.handleWebhook(context.Background(), ch, req, in, clog)
	return in, err, stub
}

func TestRegistersOneReceiveRoute(t *testing.T) {
	routes := channels.NewRoutes()
	h := NewHandler(&runtime.Runtime{Config: runtime.NewDefaultConfig()}, routes)

	assert.Equal(t, models.ChannelType("WZ"), h.ChannelType())
	require.Len(t, routes.All(), 1)
	assert.Equal(t, http.MethodPost, routes.All()[0].Method)
	assert.Equal(t, "receive", routes.All()[0].Action)
}

func TestReceiveTextMessage(t *testing.T) {
	in, err, stub := receive(t, `{"type":"Message","event":{"Info":{"ID":"3EB0A1","Sender":"50998765432@s.whatsapp.net","Chat":"50998765432@s.whatsapp.net","PushName":"Ti Jo"},"Message":{"conversation":"bonjou"}}}`)

	require.NoError(t, err)
	assert.Equal(t, channels.ReceiveKindMsg, in.Kind())
	assert.Equal(t, 1, in.Len())

	// the message is marked read at WuzAPI as soon as it's accepted
	require.Len(t, stub.calls, 1)
	assert.True(t, strings.HasPrefix(stub.calls[0], "/chat/markread "), stub.calls[0])
	assert.Contains(t, stub.calls[0], `"3EB0A1"`)
	assert.Contains(t, stub.calls[0], `"50998765432"`)
}

func TestReceiveReadReceipt(t *testing.T) {
	in, err, stub := receive(t, `{"type":"ReadReceipt","receiptType":"read","id":"3EB0B2"}`)

	require.NoError(t, err)
	assert.Equal(t, channels.ReceiveKindStatus, in.Kind())
	assert.Equal(t, 1, in.Len())
	assert.Empty(t, stub.calls)
}

// everything here must end as an empty batch and no error: that's answered 200 "ignored", which is what stops
// WuzAPI re-queueing a webhook we'll never be able to use
func TestReceiveIgnored(t *testing.T) {
	tcs := map[string]string{
		"malformed json":      `{"type":`,
		"unknown event type":  `{"type":"Presence","event":{}}`,
		"empty message event": `{"type":"Message"}`,
		"own message":         `{"type":"Message","event":{"Info":{"ID":"A","Sender":"50912345678@s.whatsapp.net","IsFromMe":true},"Message":{"conversation":"hi"}}}`,
		"group message":       `{"type":"Message","event":{"Info":{"ID":"B","Sender":"50998765432@s.whatsapp.net","IsGroup":true},"Message":{"conversation":"hi"}}}`,
		"status broadcast":    `{"type":"Message","event":{"Info":{"ID":"C","Sender":"50998765432@s.whatsapp.net","Chat":"status@broadcast"},"Message":{"conversation":"hi"}}}`,
		"no text or media":    `{"type":"Message","event":{"Info":{"ID":"D","Sender":"50998765432@s.whatsapp.net"},"Message":{}}}`,
		"unmapped receipt":    `{"type":"ReadReceipt","receiptType":"played","id":"E"}`,
		"receipt without id":  `{"type":"ReadReceipt","receiptType":"read"}`,
	}

	for name, body := range tcs {
		t.Run(name, func(t *testing.T) {
			in, err, stub := receive(t, body)

			assert.NoError(t, err)
			assert.Equal(t, 0, in.Len())
			assert.Empty(t, stub.calls, "nothing ignored should be marked read")
		})
	}
}

func TestReceiveMissingSenderIsAnError(t *testing.T) {
	in, err, _ := receive(t, `{"type":"Message","event":{"Info":{"ID":"F"},"Message":{"conversation":"hi"}}}`)

	assert.EqualError(t, err, "missing sender")
	assert.Equal(t, 0, in.Len())
}

func TestSendableEvents(t *testing.T) {
	h := NewHandler(&runtime.Runtime{Config: runtime.NewDefaultConfig()}, channels.NewRoutes())

	evts := h.SendableEvents(nil)
	assert.Contains(t, evts, "typing_started")
	assert.Contains(t, evts, "typing_stopped")
}
