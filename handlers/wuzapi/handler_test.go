package wuzapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"image"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/nyaruka/courier/v26/core/channels"
	"github.com/nyaruka/courier/v26/core/models"
	. "github.com/nyaruka/courier/v26/handlers/handlertest"
	"github.com/nyaruka/courier/v26/runtime"
	"github.com/nyaruka/courier/v26/test"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/core/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	channelUUID = "8eb23e93-5ecb-45ba-b726-3b064e0c56ab"
	testToken   = "tk_sesame"
	testHMACKey = "0123456789abcdef0123456789abcdef"
)

// The outgoing tests run on the standard harness, which needs the test database. Everything else here needs no
// backing services: a handler on a test runtime, a stand-in for the bridge, and the receive function called directly.

func TestOutgoing(t *testing.T) {
	ch := test.NewMockChannel(channelUUID, "WZ", "12065551212", "US", []string{urns.WhatsApp.Prefix},
		map[string]any{
			configURL:         "http://bridge.example.com",
			configToken:       testToken,
			configListButton:  "Choose",
			configListSection: "Options",
			configListFooter:  "Reply with a number",
		})

	RunOutgoingTests(t, ch, newHandler, "testdata/outgoing.json", &OutgoingOptions{CheckRedacted: []string{testToken}})
}

// bridgeStub stands in for the bridge, recording what it's asked and answering with whatever the test sets
type bridgeStub struct {
	*httptest.Server

	mu       sync.Mutex
	requests []*bridgeRequest
	respond  func(path string) (int, string)
}

type bridgeRequest struct {
	Path string
	Auth string
	Body map[string]any
}

func newBridgeStub(t *testing.T) *bridgeStub {
	s := &bridgeStub{}
	s.respond = func(path string) (int, string) {
		return 200, `{"code":200,"data":{"Details":"Sent","Id":"3EB0ABC123","Timestamp":1758700000},"success":true}`
	}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		req := &bridgeRequest{Path: r.URL.Path, Auth: r.Header.Get("Authorization")}
		json.Unmarshal(body, &req.Body)

		s.mu.Lock()
		s.requests = append(s.requests, req)
		s.mu.Unlock()

		status, resp := s.respond(r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		w.Write([]byte(resp))
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *bridgeStub) Requests() []*bridgeRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*bridgeRequest{}, s.requests...)
}

func (s *bridgeStub) Paths() []string {
	paths := []string{}
	for _, r := range s.Requests() {
		paths = append(paths, r.Path)
	}
	return paths
}

type testEnv struct {
	handler *handler
	bridge  *bridgeStub
	channel *models.Channel
}

func newTestEnv(t *testing.T, config map[string]any) *testEnv {
	stub := newBridgeStub(t)

	cfg := runtime.NewDefaultConfig()
	cfg.MediaDomain = "example.com"
	cfg.AttachmentsDir = t.TempDir()

	h := newHandler(runtime.NewTestRuntime(cfg), channels.NewRoutes()).(*handler)

	chConfig := map[string]any{configURL: stub.URL, configToken: testToken}
	for k, v := range config {
		chConfig[k] = v
	}
	ch := test.NewMockChannel(channelUUID, "WZ", "12065551212", "US", []string{urns.WhatsApp.Prefix}, chConfig)

	return &testEnv{handler: h, bridge: stub, channel: ch}
}

// formBody encodes an event the way the bridge posts it by default: as a form whose jsonData field is the event
func formBody(event string) string {
	return url.Values{"instanceName": {"test"}, "jsonData": {event}, "userID": {"u1"}}.Encode()
}

func sign(body, key string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(body))
	return hex.EncodeToString(mac.Sum(nil))
}

// receive runs a webhook through the receive function the way the server would, returning the batch and log
func (e *testEnv) receive(t *testing.T, contentType, body string, headers map[string]string) (*channels.Received, *models.ChannelLog, error) {
	req := httptest.NewRequest(http.MethodPost, "/c/wz/"+channelUUID+"/receive", strings.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	in := channels.NewReceived(e.channel).As(channels.ReceiveKindAny)
	clog := models.NewChannelLogForIncoming(models.ChannelLogTypeReceive, e.channel, nil, e.handler.RedactValues(e.channel))

	err := e.handler.receive(context.Background(), e.channel, req, in, clog)
	return in, clog, err
}

// message parses an event the way receive does and returns the message it makes of it
func (e *testEnv) message(t *testing.T, event string) (*models.MsgIn, *models.ChannelLog, error) {
	req := httptest.NewRequest(http.MethodPost, "/receive", strings.NewReader(event))
	req.Header.Set("Content-Type", "application/json")
	payload, err := parseWebhook(req, []byte(event))
	require.NoError(t, err)

	clog := models.NewChannelLogForIncoming(models.ChannelLogTypeReceive, e.channel, nil, e.handler.RedactValues(e.channel))
	msg, err := e.handler.receiveMessage(context.Background(), e.channel, payload, clog)
	return msg, clog, err
}

func (e *testEnv) statuses(t *testing.T, event string) ([]*models.StatusUpdate, error) {
	req := httptest.NewRequest(http.MethodPost, "/receive", strings.NewReader(event))
	req.Header.Set("Content-Type", "application/json")
	payload, err := parseWebhook(req, []byte(event))
	require.NoError(t, err)

	clog := models.NewChannelLogForIncoming(models.ChannelLogTypeReceive, e.channel, nil, nil)
	return receiveReceipt(e.channel, payload, clog)
}

func logCodes(clog *models.ChannelLog) []string {
	codes := []string{}
	for _, e := range clog.Errors {
		codes = append(codes, e.Code)
	}
	return codes
}

func pngBytes() []byte {
	var buf bytes.Buffer
	png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 1, 1)))
	return buf.Bytes()
}

// an incoming text message as the bridge serializes a whatsmeow message event
const textMessageEvent = `{
	"type": "Message",
	"event": {
		"Info": {
			"Chat": "12065551212@s.whatsapp.net",
			"Sender": "12065551212:3@s.whatsapp.net",
			"IsFromMe": false,
			"IsGroup": false,
			"ID": "3EB0A1B2C3",
			"Type": "text",
			"PushName": "Jo",
			"Timestamp": "2026-09-24T14:05:00Z"
		},
		"Message": {"conversation": "bonjou"}
	}
}`

func TestRoutes(t *testing.T) {
	routes := channels.NewRoutes()
	h := newHandler(runtime.NewTestRuntime(runtime.NewDefaultConfig()), routes)

	assert.Equal(t, models.ChannelType("WZ"), h.ChannelType())
	require.Len(t, routes.All(), 1)
	assert.Equal(t, http.MethodPost, routes.All()[0].Method)
	assert.Equal(t, "receive", routes.All()[0].Action)

	assert.Contains(t, h.SendableEvents(nil), events.TypeTypingStarted)
	assert.Contains(t, h.SendableEvents(nil), events.TypeTypingStopped)
}

func TestReceiveMessage(t *testing.T) {
	env := newTestEnv(t, nil)

	in, clog, err := env.receive(t, "application/x-www-form-urlencoded", formBody(textMessageEvent), nil)
	require.NoError(t, err)
	assert.Equal(t, channels.ReceiveKindMsg, in.Kind())
	assert.Equal(t, 1, in.Len())
	assert.Empty(t, clog.Errors)

	// the message is marked read at the bridge as soon as it's accepted
	reqs := env.bridge.Requests()
	require.Len(t, reqs, 1)
	assert.Equal(t, "/chat/markread", reqs[0].Path)
	assert.Equal(t, testToken, reqs[0].Auth)
	assert.Equal(t, map[string]any{"Id": []any{"3EB0A1B2C3"}, "ChatPhone": "12065551212", "SenderPhone": "12065551212"}, reqs[0].Body)
	assert.Len(t, clog.HttpLogs, 1)

	// the same event posted as JSON, which is how a bridge configured for that posts it
	in, _, err = env.receive(t, "application/json", textMessageEvent, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, in.Len())

	msg, _, err := env.message(t, textMessageEvent)
	require.NoError(t, err)
	assert.Equal(t, "bonjou", msg.Text())
	assert.Equal(t, urns.URN("whatsapp:12065551212"), msg.URN())
	assert.Equal(t, "3EB0A1B2C3", msg.ExternalID())
	assert.Equal(t, "Jo", msg.ContactName_)
	assert.Equal(t, "2026-09-24T14:05:00Z", msg.ReceivedOn().Format("2006-01-02T15:04:05Z"))
	assert.Empty(t, msg.Attachments())
}

func TestReceiveMessageVariants(t *testing.T) {
	env := newTestEnv(t, nil)

	event := func(message string) string {
		return `{"type":"Message","event":{"Info":{"Chat":"12065551212@s.whatsapp.net","Sender":"12065551212@s.whatsapp.net","ID":"3EB0X"},"Message":` + message + `}}`
	}

	tcs := []struct {
		label string
		event string
		text  string
		urn   urns.URN
	}{
		{"extended text", event(`{"extendedTextMessage":{"text":"look at this https://example.com"}}`), "look at this https://example.com", "whatsapp:12065551212"},
		{"list reply", event(`{"listResponseMessage":{"title":"Option B","singleSelectReply":{"selectedRowID":"1"}}}`), "Option B", "whatsapp:12065551212"},
		{"button reply", event(`{"buttonsResponseMessage":{"selectedButtonID":"0","selectedDisplayText":"Yes"}}`), "Yes", "whatsapp:12065551212"},
		{"button reply without text", event(`{"buttonsResponseMessage":{"selectedButtonID":"0"}}`), "0", "whatsapp:12065551212"},
		{"template button reply", event(`{"templateButtonReplyMessage":{"selectedID":"2","selectedDisplayText":"Maybe"}}`), "Maybe", "whatsapp:12065551212"},
		{"native flow reply", event(`{"interactiveResponseMessage":{"nativeFlowResponseMessage":{"name":"quick_reply","paramsJSON":"{\"id\":\"1\",\"display_text\":\"No\"}"}}}`), "No", "whatsapp:12065551212"},
		{"ephemeral", event(`{"ephemeralMessage":{"message":{"conversation":"secret"}}}`), "secret", "whatsapp:12065551212"},
		{"view once inside ephemeral", event(`{"ephemeralMessage":{"message":{"viewOnceMessageV2":{"message":{"conversation":"once"}}}}}`), "once", "whatsapp:12065551212"},
		{"lid sender with phone alt", `{"type":"Message","event":{"Info":{"Chat":"98765432101234@lid","Sender":"98765432101234@lid","SenderAlt":"12065551212:5@s.whatsapp.net","ID":"3EB0Y"},"Message":{"conversation":"hi"}}}`, "hi", "whatsapp:12065551212"},
	}

	for _, tc := range tcs {
		t.Run(tc.label, func(t *testing.T) {
			msg, clog, err := env.message(t, tc.event)
			require.NoError(t, err)
			assert.Equal(t, tc.text, msg.Text())
			assert.Equal(t, tc.urn, msg.URN())
			assert.Empty(t, clog.Errors)
		})
	}
}

func TestReceiveMedia(t *testing.T) {
	env := newTestEnv(t, nil)
	attachmentsDir := env.handler.Runtime().Config.AttachmentsDir

	imageEvent := func(caption, inline string) string {
		return `{"type":"Message","event":{"Info":{"Chat":"12065551212@s.whatsapp.net","Sender":"12065551212@s.whatsapp.net","ID":"3EB0M"},
			"Message":{"imageMessage":{"URL":"https://mmg.whatsapp.net/v/t62.7118-24/abc?ccb=11-4","mimetype":"image/jpeg","caption":"` + caption + `",
			"mediaKey":"a2V5","directPath":"/v/t62.7118-24/abc","fileEncSHA256":"ZW5j","fileSHA256":"c2hh","fileLength":67}}}` + inline + `}`
	}
	inlinePNG := `,"base64":"` + base64.StdEncoding.EncodeToString(pngBytes()) + `","mimeType":"image/png","fileName":"3EB0M.png"`

	// media the bridge delivered inline is saved, with its type sniffed from the bytes rather than taken from the
	// message, and the caption is the message text
	msg, clog, err := env.message(t, imageEvent("regarde", inlinePNG))
	require.NoError(t, err)
	assert.Equal(t, "regarde", msg.Text())
	require.Len(t, msg.Attachments(), 1)
	assert.Regexp(t, `^image/png:https://example\.com/media/attachments/1/[0-9a-f-]{36}\.png$`, msg.Attachments()[0])
	assert.Empty(t, clog.Errors)

	saved, err := os.ReadFile(filepath.Join(attachmentsDir, "attachments", "1", filepath.Base(msg.Attachments()[0])))
	require.NoError(t, err)
	assert.Equal(t, pngBytes(), saved)

	// no caption means no text, not a placeholder
	msg, _, err = env.message(t, imageEvent("", inlinePNG))
	require.NoError(t, err)
	assert.Equal(t, "", msg.Text())
	assert.Len(t, msg.Attachments(), 1)

	// media the bridge didn't deliver inline is downloaded through it
	env.bridge.respond = func(path string) (int, string) {
		return 200, `{"code":200,"data":{"Data":"data:image/png;base64,` + base64.StdEncoding.EncodeToString(pngBytes()) + `","Mimetype":"image/png"},"success":true}`
	}
	msg, clog, err = env.message(t, imageEvent("fetched", ""))
	require.NoError(t, err)
	assert.Equal(t, "fetched", msg.Text())
	assert.Len(t, msg.Attachments(), 1)
	assert.Empty(t, clog.Errors)

	// each accepted message was marked read, and the last one downloaded first
	assert.Equal(t, []string{"/chat/markread", "/chat/markread", "/chat/downloadimage", "/chat/markread"}, env.bridge.Paths())
	assert.Equal(t, map[string]any{
		"Url": "https://mmg.whatsapp.net/v/t62.7118-24/abc?ccb=11-4", "DirectPath": "/v/t62.7118-24/abc", "MediaKey": "a2V5", "Mimetype": "image/jpeg",
		"FileEncSHA256": "ZW5j", "FileSHA256": "c2hh", "FileLength": float64(67),
	}, env.bridge.Requests()[2].Body)

	// a download the bridge refuses is recorded and the text still arrives
	env.bridge.respond = func(path string) (int, string) { return 500, `{"code":500,"error":"no session","success":false}` }
	msg, clog, err = env.message(t, imageEvent("still here", ""))
	require.NoError(t, err)
	assert.Equal(t, "still here", msg.Text())
	assert.Empty(t, msg.Attachments())
	assert.Equal(t, []string{"response_status_code", "attachment_not_decodable"}, logCodes(clog))

	// as is inline media that doesn't decode
	msg, clog, err = env.message(t, imageEvent("still here", `,"base64":"!!!!","mimeType":"image/png"`))
	require.NoError(t, err)
	assert.Equal(t, "still here", msg.Text())
	assert.Empty(t, msg.Attachments())
	assert.Equal(t, []string{"attachment_not_decodable"}, logCodes(clog))

	// and media that can't be saved because there's nowhere to save it
	env.handler.Runtime().Config.AttachmentsDir = ""
	msg, clog, err = env.message(t, imageEvent("still here", inlinePNG))
	require.NoError(t, err)
	assert.Equal(t, "still here", msg.Text())
	assert.Empty(t, msg.Attachments())
	assert.Equal(t, []string{"attachment_not_saved"}, logCodes(clog))

	// which without a caption leaves nothing to receive
	msg, clog, err = env.message(t, imageEvent("", inlinePNG))
	assert.EqualError(t, err, "ignoring message with no text or media")
	assert.Nil(t, msg)
	assert.Equal(t, []string{"attachment_not_saved"}, logCodes(clog))
	env.handler.Runtime().Config.AttachmentsDir = attachmentsDir

	// a document's type is sniffed too, and falls back to the declared type when the bytes don't say
	docEvent := `{"type":"Message","event":{"Info":{"Chat":"12065551212@s.whatsapp.net","Sender":"12065551212@s.whatsapp.net","ID":"3EB0D"},
		"Message":{"documentWithCaptionMessage":{"message":{"documentMessage":{"URL":"https://mmg.whatsapp.net/d","mimetype":"text/csv","fileName":"list.csv","caption":"the list"}}}}},
		"base64":"` + base64.StdEncoding.EncodeToString([]byte("a,b\n1,2\n")) + `","mimeType":"text/csv","fileName":"3EB0D.csv"}`
	msg, clog, err = env.message(t, docEvent)
	require.NoError(t, err)
	assert.Equal(t, "the list", msg.Text())
	require.Len(t, msg.Attachments(), 1)
	assert.Regexp(t, `^text/csv:https://example\.com/media/attachments/1/[0-9a-f-]{36}\.csv$`, msg.Attachments()[0])
	assert.Empty(t, clog.Errors)
}

func TestReceiveReceipt(t *testing.T) {
	env := newTestEnv(t, nil)

	receipt := func(state string, ids ...string) string {
		quoted := make([]string, len(ids))
		for i, id := range ids {
			quoted[i] = `"` + id + `"`
		}
		return `{"type":"ReadReceipt","state":"` + state + `","event":{"Chat":"12065551212@s.whatsapp.net","Sender":"12065551212@s.whatsapp.net","IsFromMe":false,"IsGroup":false,
			"MessageIDs":[` + strings.Join(quoted, ",") + `],"Timestamp":"2026-09-24T14:06:00Z","Type":"` + strings.ToLower(state) + `","MessageSender":"12065551212@s.whatsapp.net"}}`
	}

	in, clog, err := env.receive(t, "application/x-www-form-urlencoded", formBody(receipt("Delivered", "3EB0S1")), nil)
	require.NoError(t, err)
	assert.Equal(t, channels.ReceiveKindStatus, in.Kind())
	assert.Equal(t, 1, in.Len())
	assert.Empty(t, clog.Errors)
	assert.Empty(t, env.bridge.Requests(), "a receipt has nothing to mark read")

	statuses, err := env.statuses(t, receipt("Delivered", "3EB0S1"))
	require.NoError(t, err)
	require.Len(t, statuses, 1)
	assert.Equal(t, "3EB0S1", statuses[0].ExternalIdentifier())
	assert.Equal(t, models.MsgStatusDelivered, statuses[0].Status())

	// one update per message a receipt covers
	statuses, err = env.statuses(t, receipt("Read", "3EB0S1", "3EB0S2"))
	require.NoError(t, err)
	require.Len(t, statuses, 2)
	assert.Equal(t, "3EB0S1", statuses[0].ExternalIdentifier())
	assert.Equal(t, models.MsgStatusRead, statuses[0].Status())
	assert.Equal(t, "3EB0S2", statuses[1].ExternalIdentifier())
	assert.Equal(t, models.MsgStatusRead, statuses[1].Status())

	// our own reads of their messages aren't statuses of ours
	_, err = env.statuses(t, receipt("ReadSelf", "3EB0S1"))
	assert.EqualError(t, err, "ignoring receipt for our own read")

	_, err = env.statuses(t, receipt("Played", "3EB0S1"))
	assert.EqualError(t, err, "ignoring receipt state: Played")

	statuses, err = env.statuses(t, receipt("Read"))
	require.NoError(t, err)
	assert.Empty(t, statuses)
}

func TestReceiveSignature(t *testing.T) {
	env := newTestEnv(t, map[string]any{configHMACKey: testHMACKey})
	body := formBody(textMessageEvent)

	// signed with the channel's key, as the bridge signs the bytes it posts
	in, clog, err := env.receive(t, "application/x-www-form-urlencoded", body, map[string]string{signatureHeader: sign(body, testHMACKey)})
	require.NoError(t, err)
	assert.Equal(t, 1, in.Len())
	assert.Empty(t, clog.Errors)

	// signed with a different key
	in, clog, err = env.receive(t, "application/x-www-form-urlencoded", body, map[string]string{signatureHeader: sign(body, "not the key, not the key, not the")})
	assert.EqualError(t, err, "invalid request signature")
	assert.IsType(t, &channels.UnauthenticatedRequest{}, err)
	assert.Equal(t, 0, in.Len())
	assert.Equal(t, []string{"request_unauthenticated"}, logCodes(clog))

	// signed over different bytes
	in, _, err = env.receive(t, "application/x-www-form-urlencoded", body, map[string]string{signatureHeader: sign(body+"&x=1", testHMACKey)})
	assert.EqualError(t, err, "invalid request signature")
	assert.Equal(t, 0, in.Len())

	// not a signature at all
	in, _, err = env.receive(t, "application/x-www-form-urlencoded", body, map[string]string{signatureHeader: "xyz"})
	assert.EqualError(t, err, "invalid request signature")
	assert.Equal(t, 0, in.Len())

	// not signed
	in, clog, err = env.receive(t, "application/x-www-form-urlencoded", body, nil)
	assert.EqualError(t, err, "missing request signature")
	assert.IsType(t, &channels.UnauthenticatedRequest{}, err)
	assert.Equal(t, 0, in.Len())
	assert.Equal(t, []string{"request_unauthenticated"}, logCodes(clog))

	// nothing rejected was marked read
	reqs := env.bridge.Requests()
	assert.Len(t, reqs, 1)

	// a channel without a key accepts unsigned webhooks
	legacy := newTestEnv(t, nil)
	in, _, err = legacy.receive(t, "application/x-www-form-urlencoded", body, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, in.Len())
}

// everything here is answered as ignored: the bridge would only retry an error, and none of these will ever
// become something we can use
func TestReceiveIgnored(t *testing.T) {
	env := newTestEnv(t, nil)

	own := `{"type":"Message","event":{"Info":{"Chat":"12065551212@s.whatsapp.net","Sender":"12065550000@s.whatsapp.net","IsFromMe":true,"ID":"A"},"Message":{"conversation":"hi"}}}`
	group := `{"type":"Message","event":{"Info":{"Chat":"120363012345678901@g.us","Sender":"12065551212@s.whatsapp.net","IsGroup":true,"ID":"B"},"Message":{"conversation":"hi"}}}`
	groupChat := `{"type":"Message","event":{"Info":{"Chat":"120363012345678901@g.us","Sender":"12065551212@s.whatsapp.net","ID":"B2"},"Message":{"conversation":"hi"}}}`
	status := `{"type":"Message","event":{"Info":{"Chat":"status@broadcast","Sender":"12065551212@s.whatsapp.net","ID":"C"},"Message":{"conversation":"hi"}}}`
	noSender := `{"type":"Message","event":{"Info":{"Chat":"12065551212@s.whatsapp.net","ID":"D"},"Message":{"conversation":"hi"}}}`
	lidOnly := `{"type":"Message","event":{"Info":{"Chat":"98765432101234@lid","Sender":"98765432101234@lid","ID":"D2"},"Message":{"conversation":"hi"}}}`
	empty := `{"type":"Message","event":{"Info":{"Chat":"12065551212@s.whatsapp.net","Sender":"12065551212@s.whatsapp.net","ID":"E"},"Message":{"protocolMessage":{"type":"REVOKE"}}}}`
	noMessage := `{"type":"Message","event":{"Info":{"Chat":"12065551212@s.whatsapp.net","Sender":"12065551212@s.whatsapp.net","ID":"F"}}}`
	badEvent := `{"type":"Message","event":"not an object"}`

	tcs := []struct {
		label       string
		contentType string
		body        string
		details     string
		logCodes    []string
	}{
		{"own message", "application/json", own, "ignoring own message", nil},
		{"group message", "application/json", group, "ignoring group or broadcast message", nil},
		{"group chat", "application/json", groupChat, "ignoring group or broadcast message", nil},
		{"status broadcast", "application/json", status, "ignoring group or broadcast message", nil},
		{"no sender", "application/json", noSender, "ignoring message without a phone number sender", []string{"request_unparseable"}},
		{"lid sender without phone", "application/json", lidOnly, "ignoring message without a phone number sender", []string{"request_unparseable"}},
		{"no text or media", "application/json", empty, "ignoring message with no text or media", nil},
		{"no message", "application/json", noMessage, "ignoring event with no message", nil},
		{"unparseable message event", "application/json", badEvent, "unparseable message event", []string{"request_unparseable"}},
		{"unhandled event type", "application/json", `{"type":"Presence","event":{"From":"12065551212@s.whatsapp.net","Unavailable":true}}`, "unhandled event type: Presence", nil},
		{"malformed json", "application/json", `{"type":`, "unparseable webhook", []string{"request_unparseable"}},
		{"form without event", "application/x-www-form-urlencoded", "instanceName=test&userID=u1", "unparseable webhook", []string{"request_unparseable"}},
		{"form with malformed event", "application/x-www-form-urlencoded", formBody(`{"type":`), "unparseable webhook", []string{"request_unparseable"}},
	}

	for _, tc := range tcs {
		t.Run(tc.label, func(t *testing.T) {
			in, clog, err := env.receive(t, tc.contentType, tc.body, nil)

			assert.EqualError(t, err, tc.details)
			assert.IsType(t, &channels.IgnoredRequest{}, err)
			assert.Equal(t, 0, in.Len())
			if tc.logCodes == nil {
				assert.Empty(t, clog.Errors)
			} else {
				assert.Equal(t, tc.logCodes, logCodes(clog))
			}
		})
	}

	assert.Empty(t, env.bridge.Requests(), "nothing ignored should be marked read")
}

func (e *testEnv) send(t *testing.T, text string, attachments []string, qrs []models.QuickReply) (*channels.SendResult, *models.ChannelLog, error) {
	msg := test.NewMockMsg("0191e180-7d60-7000-aded-7d8b151cbd5b", e.channel, "whatsapp:12065551212", text, attachments)
	msg.QuickReplies_ = qrs

	res := &channels.SendResult{}
	clog := models.NewChannelLogForSend(msg, e.handler.RedactValues(e.channel))
	err := e.handler.Send(context.Background(), msg, res, clog)
	return res, clog, err
}

func TestSend(t *testing.T) {
	env := newTestEnv(t, map[string]any{configListButton: "Choose", configListSection: "Options", configListFooter: "Reply with a number"})

	res, clog, err := env.send(t, "Simple Message", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"3EB0ABC123"}, res.ExternalIDs())
	assert.Empty(t, clog.Errors)
	assert.Len(t, clog.HttpLogs, 1)

	reqs := env.bridge.Requests()
	require.Len(t, reqs, 1)
	assert.Equal(t, "/chat/send/text", reqs[0].Path)
	assert.Equal(t, testToken, reqs[0].Auth)
	assert.Equal(t, map[string]any{"Phone": "12065551212", "Body": "Simple Message"}, reqs[0].Body)

	// the token is redacted from the log
	for _, l := range clog.HttpLogs {
		assert.NotContains(t, l.Request, testToken)
	}

	// url quick replies go in the text as links
	_, _, err = env.send(t, "See", nil, []models.QuickReply{{Type: "url", Text: "Our site", Extra: "https://example.com"}})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"Phone": "12065551212", "Body": "See\n\nOur site: https://example.com"}, env.bridge.Requests()[1].Body)

	// up to three text quick replies are buttons
	_, _, err = env.send(t, "Sure?", nil, []models.QuickReply{{Type: "text", Text: "Yes"}, {Type: "text", Text: "No"}})
	require.NoError(t, err)
	req := env.bridge.Requests()[2]
	assert.Equal(t, "/chat/send/buttons", req.Path)
	assert.Equal(t, map[string]any{
		"Phone": "12065551212", "Body": "Sure?",
		"Buttons": []any{map[string]any{"type": "reply", "title": "Yes", "id": "0"}, map[string]any{"type": "reply", "title": "No", "id": "1"}},
	}, req.Body)

	// more, or any with a description, are a list - with sections named in the quick replies
	_, _, err = env.send(t, "Pick", nil, []models.QuickReply{
		{Type: "text", Text: "Fruit|Apple", Extra: "Red"}, {Type: "text", Text: "Fruit|Pear"}, {Type: "text", Text: "Veg|Kale"}, {Type: "text", Text: "Other"},
	})
	require.NoError(t, err)
	req = env.bridge.Requests()[3]
	assert.Equal(t, "/chat/send/list", req.Path)
	assert.Equal(t, map[string]any{
		"Phone": "12065551212", "Desc": "Pick", "ButtonText": "Choose", "FooterText": "Reply with a number",
		"Sections": []any{
			map[string]any{"title": "Fruit", "rows": []any{map[string]any{"title": "Apple", "desc": "Red", "rowId": "0"}, map[string]any{"title": "Pear", "desc": "", "rowId": "1"}}},
			map[string]any{"title": "Veg", "rows": []any{map[string]any{"title": "Kale", "desc": "", "rowId": "2"}}},
			map[string]any{"title": "Options", "rows": []any{map[string]any{"title": "Other", "desc": "", "rowId": "3"}}},
		},
	}, req.Body)

	// nothing to send
	_, _, err = env.send(t, "", nil, nil)
	assert.Equal(t, channels.ErrMessageInvalid, err)
	assert.Len(t, env.bridge.Requests(), 4)
}

func TestSendMedia(t *testing.T) {
	env := newTestEnv(t, nil)

	// a single attachment carries the text as its caption
	res, _, err := env.send(t, "look", []string{"image/jpeg:https://files.example.org/photo.jpg"}, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"3EB0ABC123"}, res.ExternalIDs())
	reqs := env.bridge.Requests()
	require.Len(t, reqs, 1)
	assert.Equal(t, "/chat/send/image", reqs[0].Path)
	assert.Equal(t, map[string]any{"Phone": "12065551212", "Image": "https://files.example.org/photo.jpg", "Caption": "look"}, reqs[0].Body)

	// every attachment is sent, the text as the caption of the last that can carry one
	res, _, err = env.send(t, "both", []string{"image/png:https://files.example.org/a.png", "application/pdf:https://files.example.org/b.pdf"}, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"3EB0ABC123", "3EB0ABC123"}, res.ExternalIDs())
	reqs = env.bridge.Requests()[1:]
	require.Len(t, reqs, 2)
	assert.Equal(t, "/chat/send/image", reqs[0].Path)
	assert.Equal(t, map[string]any{"Phone": "12065551212", "Image": "https://files.example.org/a.png"}, reqs[0].Body)
	assert.Equal(t, "/chat/send/document", reqs[1].Path)
	assert.Equal(t, map[string]any{"Phone": "12065551212", "Document": "https://files.example.org/b.pdf", "FileName": "b.pdf", "Caption": "both"}, reqs[1].Body)

	// audio can't carry a caption so the text goes on its own
	_, _, err = env.send(t, "listen", []string{"audio/ogg:https://files.example.org/voice.ogg"}, nil)
	require.NoError(t, err)
	reqs = env.bridge.Requests()[3:]
	require.Len(t, reqs, 2)
	assert.Equal(t, "/chat/send/audio", reqs[0].Path)
	assert.Equal(t, map[string]any{"Phone": "12065551212", "Audio": "https://files.example.org/voice.ogg"}, reqs[0].Body)
	assert.Equal(t, "/chat/send/text", reqs[1].Path)
	assert.Equal(t, map[string]any{"Phone": "12065551212", "Body": "listen"}, reqs[1].Body)

	// video, and text that has to be interactive goes after the attachments
	_, _, err = env.send(t, "watch?", []string{"video/mp4:https://files.example.org/clip.mp4"}, []models.QuickReply{{Type: "text", Text: "Yes"}})
	require.NoError(t, err)
	reqs = env.bridge.Requests()[5:]
	require.Len(t, reqs, 2)
	assert.Equal(t, "/chat/send/video", reqs[0].Path)
	assert.Equal(t, map[string]any{"Phone": "12065551212", "Video": "https://files.example.org/clip.mp4"}, reqs[0].Body)
	assert.Equal(t, "/chat/send/buttons", reqs[1].Path)

	// unless it's an image, which a button message carries as its header
	_, _, err = env.send(t, "this one?", []string{"image/jpeg:https://files.example.org/photo.jpg"}, []models.QuickReply{{Type: "text", Text: "Yes"}})
	require.NoError(t, err)
	reqs = env.bridge.Requests()[7:]
	require.Len(t, reqs, 1)
	assert.Equal(t, "/chat/send/buttons", reqs[0].Path)
	assert.Equal(t, "https://files.example.org/photo.jpg", reqs[0].Body["Image"])

	// a URL without a host is completed against the media domain, which is where the platform's own media lives
	_, clog, err := env.send(t, "", []string{"image/jpeg:https:///rp/media/attachments/1/photo.jpg", "image/jpeg:/rp/media/attachments/1/other.jpg"}, nil)
	require.NoError(t, err)
	assert.Empty(t, clog.Errors)
	reqs = env.bridge.Requests()[8:]
	require.Len(t, reqs, 2)
	assert.Equal(t, "https://example.com/rp/media/attachments/1/photo.jpg", reqs[0].Body["Image"])
	assert.Equal(t, "https://example.com/rp/media/attachments/1/other.jpg", reqs[1].Body["Image"])

	// without a media domain there's nothing to complete it with, so it's recorded and the text still goes
	env.handler.Runtime().Config.MediaDomain = ""
	_, clog, err = env.send(t, "just this", []string{"image/jpeg:/rp/media/attachments/1/photo.jpg"}, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"media_unresolveable"}, logCodes(clog))
	reqs = env.bridge.Requests()[10:]
	require.Len(t, reqs, 1)
	assert.Equal(t, "/chat/send/text", reqs[0].Path)
}

func TestSendErrors(t *testing.T) {
	env := newTestEnv(t, nil)

	// the bridge rejecting a message fails it with the reason
	env.bridge.respond = func(string) (int, string) { return 400, `{"code":400,"error":"could not parse Phone","success":false}` }
	_, _, err := env.send(t, "hi", nil, nil)
	require.Error(t, err)
	serr := err.(*channels.SendError)
	assert.False(t, serr.Retryable())
	assert.Equal(t, "rejected_with_reason", serr.ClogError().Code)
	assert.Equal(t, "400", serr.ClogError().ExtCode)
	assert.Equal(t, "could not parse Phone", serr.ClogError().Message)

	// without a reason it's just the status
	env.bridge.respond = func(string) (int, string) { return 404, `not found` }
	_, _, err = env.send(t, "hi", nil, nil)
	assert.Equal(t, channels.ErrResponseStatus, err)

	// a failure inside the bridge is retried
	env.bridge.respond = func(string) (int, string) { return 500, `{"code":500,"error":"no session","success":false}` }
	_, _, err = env.send(t, "hi", nil, nil)
	assert.Equal(t, channels.ErrConnectionFailed, err)

	env.bridge.respond = func(string) (int, string) { return 429, `` }
	_, _, err = env.send(t, "hi", nil, nil)
	assert.Equal(t, channels.ErrConnectionThrottled, err)

	// a success without the message's ID is a response we don't understand
	env.bridge.respond = func(string) (int, string) { return 200, `{"code":200,"data":{"Details":"Sent"},"success":true}` }
	res, clog, err := env.send(t, "hi", nil, nil)
	assert.Equal(t, channels.ErrResponseUnexpected, err)
	assert.Empty(t, res.ExternalIDs())
	assert.Equal(t, []string{"response_value_missing"}, logCodes(clog))

	// a failure part way through a message keeps what was sent
	sends := 0
	env.bridge.respond = func(string) (int, string) {
		sends++
		if sends > 1 {
			return 500, ``
		}
		return 200, `{"code":200,"data":{"Details":"Sent","Id":"3EB0FIRST"},"success":true}`
	}
	res, _, err = env.send(t, "two", []string{"image/png:https://files.example.org/a.png", "image/png:https://files.example.org/b.png"}, nil)
	assert.Equal(t, channels.ErrConnectionFailed, err)
	assert.Equal(t, []string{"3EB0FIRST"}, res.ExternalIDs())

	// not reaching the bridge at all is retried
	env.bridge.Close()
	_, _, err = env.send(t, "hi", nil, nil)
	assert.Equal(t, channels.ErrConnectionFailed, err)

	// a channel without a bridge configured can't send anything
	unconfigured := newTestEnv(t, map[string]any{configToken: ""})
	_, _, err = unconfigured.send(t, "hi", nil, nil)
	assert.Equal(t, channels.ErrChannelConfig, err)
	assert.Empty(t, unconfigured.bridge.Requests())
}

func TestSendEvent(t *testing.T) {
	env := newTestEnv(t, nil)
	channelRef := assets.NewChannelReference(channelUUID, "Wuzapi")

	// typing marks the message being replied to read first
	clog := models.NewChannelLogForEventSend(env.channel, nil)
	err := env.handler.SendEvent(context.Background(), env.channel, events.NewTypingStarted(events.DirectionOutgoing, channelRef, "whatsapp:12065551212", "3EB0A1B2C3"), clog)
	require.NoError(t, err)
	assert.Len(t, clog.HttpLogs, 2)

	reqs := env.bridge.Requests()
	require.Len(t, reqs, 2)
	assert.Equal(t, "/chat/markread", reqs[0].Path)
	assert.Equal(t, map[string]any{"Id": []any{"3EB0A1B2C3"}, "ChatPhone": "12065551212", "SenderPhone": "12065551212"}, reqs[0].Body)
	assert.Equal(t, "/chat/presence", reqs[1].Path)
	assert.Equal(t, map[string]any{"Phone": "12065551212", "State": "composing", "Media": ""}, reqs[1].Body)

	err = env.handler.SendEvent(context.Background(), env.channel, events.NewTypingStopped(events.DirectionOutgoing, channelRef, "whatsapp:12065551212", ""), clog)
	require.NoError(t, err)
	reqs = env.bridge.Requests()[2:]
	require.Len(t, reqs, 1)
	assert.Equal(t, "/chat/presence", reqs[0].Path)
	assert.Equal(t, map[string]any{"Phone": "12065551212", "State": "paused", "Media": ""}, reqs[0].Body)

	// a failure to mark read doesn't stop the typing indicator, but a failed presence is an error
	env.bridge.respond = func(path string) (int, string) {
		if path == "/chat/markread" {
			return 500, `{"code":500,"error":"failure marking messages as read","success":false}`
		}
		return 200, `{"code":200,"data":{"Details":"Chat presence set successfuly"},"success":true}`
	}
	err = env.handler.SendEvent(context.Background(), env.channel, events.NewTypingStarted(events.DirectionOutgoing, channelRef, "whatsapp:12065551212", "3EB0A1B2C3"), clog)
	assert.NoError(t, err)

	env.bridge.respond = func(string) (int, string) { return 500, `` }
	err = env.handler.SendEvent(context.Background(), env.channel, events.NewTypingStopped(events.DirectionOutgoing, channelRef, "whatsapp:12065551212", ""), clog)
	assert.Equal(t, channels.ErrConnectionFailed, err)

	err = env.handler.SendEvent(context.Background(), env.channel, events.NewWaitExpired(), clog)
	assert.Error(t, err)
}
