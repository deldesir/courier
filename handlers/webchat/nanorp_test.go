package webchat

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nyaruka/courier/v26/core/channels"
	"github.com/nyaruka/courier/v26/core/models"
	"github.com/nyaruka/courier/v26/runtime"
	"github.com/nyaruka/courier/v26/test"
	"github.com/stretchr/testify/assert"
)

// nanoRP runs with no S3 service at all (rt.S3 is nil). Nothing can have been uploaded in that mode, so a
// message naming an attachment is refused - without dereferencing the missing service to work out a storage
// URL prefix, and without falling back to an empty prefix that would match any URL.
func TestReceiveAttachmentWithoutS3(t *testing.T) {
	rt := &runtime.Runtime{Config: runtime.NewDefaultConfig()} // S3 deliberately nil
	h := newHandler(rt, channels.NewRoutes()).(*handler)

	ch := test.NewMockChannel("8eb23e93-5ecb-45ba-b726-3b064e0c56ab", "WCH", "", "", []string{"webchat"}, nil)
	clog := models.NewChannelLogForIncoming(models.ChannelLogTypeReceive, ch, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/receive", strings.NewReader(`{}`))
	in := channels.NewReceived(ch).As(channels.ReceiveKindMsg)

	payload := &receivePayload{
		ChatID:      "65vbbDAQCdPdEWlEhDGy4utO",
		Attachments: []string{"image/jpeg:https://example.com/anything.jpg"},
	}

	assert.NotPanics(t, func() {
		err := h.receiveMessage(context.Background(), ch, req, payload, in, clog)
		assert.EqualError(t, err, "invalid attachment: image/jpeg:https://example.com/anything.jpg")
	})
	assert.Equal(t, 0, in.Len())
}
