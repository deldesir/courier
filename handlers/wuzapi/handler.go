package wuzapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/h2non/filetype"
	"github.com/nyaruka/courier/v26/core/channels"
	"github.com/nyaruka/courier/v26/core/models"
	"github.com/nyaruka/courier/v26/handlers"
	"github.com/nyaruka/courier/v26/runtime"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/svclogs"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/core/events"
)

// A WZ channel is a WhatsApp number linked to a WuzAPI bridge, which posts every event it receives to our webhook
// and exposes an HTTP API for sending. The bridge downloads incoming media itself and delivers it inline, and
// fetches outgoing media from the URLs we give it.

const (
	configURL     = "wuzapi_url"
	configToken   = "wuzapi_token"
	configHMACKey = "hmac_key"

	// per channel labels for list messages, so a workspace's line can use its own language: the button that opens
	// the list, the section that rows go in when their quick replies don't name one, and a footer
	configListButton  = "list_button"
	configListSection = "list_section"
	configListFooter  = "list_footer"

	signatureHeader = "x-hmac-signature"

	// the bridge delivers media inline as base64, so a webhook can be large - but not unbounded
	maxRequestBodyBytes = 64 * 1024 * 1024

	// what inline media may decode to: the request body cap less the base64 overhead
	maxAttachmentBytes = 48 * 1024 * 1024
)

// WhatsApp shows a typing indicator for about 25 seconds, so it's resent more often than that to sustain it
var sendableEvents = map[string]time.Duration{
	events.TypeTypingStarted: 20 * time.Second,
	events.TypeTypingStopped: 0,
}

func init() {
	channels.RegisterHandler(newHandler)
}

type handler struct {
	handlers.BaseHandler

	unsignedWarned sync.Map // the channels we've warned about accepting unverified webhooks from
}

func newHandler(rt *runtime.Runtime, r *channels.Routes) channels.Handler {
	h := &handler{BaseHandler: handlers.NewBaseHandler(rt, models.ChannelType("WZ"), "Wuzapi", handlers.WithRedactConfigKeys(configToken, configHMACKey))}

	// one webhook URL delivers both messages and receipts, so the route starts as "any" and each branch narrows
	// it once it knows which it's dealing with
	r.AddReceive(h, http.MethodPost, "receive", channels.ReceiveKindAny, h.receive)
	return h
}

// webhook is what the bridge posts: the type of event, the whatsmeow event it wraps, and - for a message whose
// media the bridge downloaded - that media inline
type webhook struct {
	Type  string          `json:"type"`
	State string          `json:"state"` // for a ReadReceipt: Delivered, Read or ReadSelf
	Event json.RawMessage `json:"event"`

	Base64   string `json:"base64"`
	MimeType string `json:"mimeType"`
	FileName string `json:"fileName"`
}

// messageEvent is the part of a whatsmeow message event we read
type messageEvent struct {
	Info struct {
		ID        string    `json:"ID"`
		Chat      string    `json:"Chat"`
		Sender    string    `json:"Sender"`
		SenderAlt string    `json:"SenderAlt"`
		IsFromMe  bool      `json:"IsFromMe"`
		IsGroup   bool      `json:"IsGroup"`
		PushName  string    `json:"PushName"`
		Timestamp time.Time `json:"Timestamp"`
	} `json:"Info"`
	Message *message `json:"Message"`
}

// receiptEvent is the part of a whatsmeow receipt event we read
type receiptEvent struct {
	MessageIDs []string `json:"MessageIDs"`
}

// message is the part of a WhatsApp message we read, in its protobuf JSON form
type message struct {
	Conversation        string `json:"conversation"`
	ExtendedTextMessage *struct {
		Text string `json:"text"`
	} `json:"extendedTextMessage"`

	ImageMessage    *mediaMessage `json:"imageMessage"`
	VideoMessage    *mediaMessage `json:"videoMessage"`
	AudioMessage    *mediaMessage `json:"audioMessage"`
	DocumentMessage *mediaMessage `json:"documentMessage"`
	StickerMessage  *mediaMessage `json:"stickerMessage"`

	// replies to the interactive messages we send
	ListResponseMessage *struct {
		Title string `json:"title"`
	} `json:"listResponseMessage"`
	ButtonsResponseMessage *struct {
		SelectedButtonID    string `json:"selectedButtonID"`
		SelectedDisplayText string `json:"selectedDisplayText"`
	} `json:"buttonsResponseMessage"`
	TemplateButtonReplyMessage *struct {
		SelectedID          string `json:"selectedID"`
		SelectedDisplayText string `json:"selectedDisplayText"`
	} `json:"templateButtonReplyMessage"`
	InteractiveResponseMessage *struct {
		NativeFlowResponseMessage *struct {
			ParamsJSON string `json:"paramsJSON"`
		} `json:"nativeFlowResponseMessage"`
	} `json:"interactiveResponseMessage"`

	// wrappers around another message
	EphemeralMessage           *wrappedMessage `json:"ephemeralMessage"`
	ViewOnceMessage            *wrappedMessage `json:"viewOnceMessage"`
	ViewOnceMessageV2          *wrappedMessage `json:"viewOnceMessageV2"`
	ViewOnceMessageV2Extension *wrappedMessage `json:"viewOnceMessageV2Extension"`
	DocumentWithCaptionMessage *wrappedMessage `json:"documentWithCaptionMessage"`
}

type wrappedMessage struct {
	Message *message `json:"message"`
}

// mediaMessage is a media message's caption, plus what the bridge needs to download it for us
type mediaMessage struct {
	Caption       string `json:"caption"`
	URL           string `json:"URL"`
	DirectPath    string `json:"directPath"`
	MediaKey      string `json:"mediaKey"`
	Mimetype      string `json:"mimetype"`
	FileEncSHA256 string `json:"fileEncSHA256"`
	FileSHA256    string `json:"fileSHA256"`
	FileLength    uint64 `json:"fileLength"`
}

// what a reply to a native flow button carries in its params
type nativeFlowParams struct {
	ID          string `json:"id"`
	DisplayText string `json:"display_text"`
}

// receive is our receive function for the bridge's webhooks
func (h *handler) receive(ctx context.Context, channel *models.Channel, r *http.Request, in *channels.Received, clog *models.ChannelLog) error {
	body, err := readBody(r)
	if err != nil {
		clog.Error(models.ErrorRequestUnparseable(err))
		return channels.Ignore("%s", err)
	}

	// a channel with a key only accepts webhooks the bridge signed with it. One without is a channel claimed before
	// keys were exchanged, and accepts anything - which is worth knowing about, once.
	if key := channel.StringConfigForKey(configHMACKey, ""); key != "" {
		if err := verifySignature(r.Header.Get(signatureHeader), body, key); err != nil {
			clog.Error(errorUnauthenticated(err))
			return channels.Unauthenticated(err)
		}
	} else if _, warned := h.unsignedWarned.LoadOrStore(channel.UUID(), true); !warned {
		slog.Warn("channel has no hmac_key so its webhooks can't be verified", "channel_uuid", channel.UUID())
	}

	payload, err := parseWebhook(r, body)
	if err != nil {
		// a webhook we can't parse is one we'll never be able to parse, so it's answered as ignored rather than
		// as an error the bridge would retry
		clog.Error(models.ErrorRequestUnparseable(err))
		return channels.Ignore("unparseable webhook")
	}

	switch payload.Type {
	case "Message":
		in.As(channels.ReceiveKindMsg)
		msg, err := h.receiveMessage(ctx, channel, payload, clog)
		if err != nil {
			return err
		}
		in.Msg(msg)
	case "ReadReceipt":
		in.As(channels.ReceiveKindStatus)
		statuses, err := receiveReceipt(channel, payload, clog)
		if err != nil {
			return err
		}
		for _, s := range statuses {
			in.Status(s)
		}
	default:
		return channels.Ignore("unhandled event type: %s", payload.Type)
	}
	return nil
}

// readBody reads the request body, refusing one over our cap rather than silently truncating it - a truncated body
// would only fail signature verification with a misleading error
func readBody(r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("error reading request body: %w", err)
	}
	if len(body) > maxRequestBodyBytes {
		return nil, fmt.Errorf("request body exceeds %d bytes", maxRequestBodyBytes)
	}
	return body, nil
}

// verifySignature checks the bridge's signature of a webhook: HMAC-SHA256 with the channel's key, hex encoded, over
// the bytes it posted - the form encoding or the JSON, whichever format it's configured to post in
func verifySignature(signature string, body []byte, key string) error {
	if signature == "" {
		return errors.New("missing request signature")
	}
	given, err := hex.DecodeString(signature)
	if err != nil {
		return errors.New("invalid request signature")
	}

	mac := hmac.New(sha256.New, []byte(key))
	mac.Write(body)

	if !hmac.Equal(given, mac.Sum(nil)) {
		return errors.New("invalid request signature")
	}
	return nil
}

// parseWebhook reads the event out of a webhook. The bridge posts a form whose jsonData field is the event, or -
// when it's configured to post JSON - the event itself as the body.
func parseWebhook(r *http.Request, body []byte) (*webhook, error) {
	data := body
	if contentType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); contentType == "application/x-www-form-urlencoded" {
		form, err := url.ParseQuery(string(body))
		if err != nil {
			return nil, fmt.Errorf("invalid form body: %w", err)
		}
		if !form.Has("jsonData") {
			return nil, errors.New("form body has no jsonData field")
		}
		data = []byte(form.Get("jsonData"))
	}

	payload := &webhook{}
	if err := json.Unmarshal(data, payload); err != nil {
		return nil, fmt.Errorf("invalid event JSON: %w", err)
	}
	return payload, nil
}

// receiveMessage turns a message event into a message, or an error saying why it isn't one
func (h *handler) receiveMessage(ctx context.Context, channel *models.Channel, payload *webhook, clog *models.ChannelLog) (*models.MsgIn, error) {
	event := &messageEvent{}
	if err := json.Unmarshal(payload.Event, event); err != nil {
		clog.Error(models.ErrorRequestUnparseable(err))
		return nil, channels.Ignore("unparseable message event")
	}

	// the bridge reports our own sends as messages too, and everything from groups, broadcast lists and status posts
	if event.Info.IsFromMe {
		return nil, channels.Ignore("ignoring own message")
	}
	if event.Info.IsGroup || !isUserJID(event.Info.Chat) {
		return nil, channels.Ignore("ignoring group or broadcast message")
	}

	urn, err := senderURN(event.Info.Sender, event.Info.SenderAlt)
	if err != nil {
		clog.Error(models.ErrorRequestUnparseable(err))
		return nil, channels.Ignore("ignoring message without a phone number sender")
	}

	msg := unwrap(event.Message)
	if msg == nil {
		return nil, channels.Ignore("ignoring event with no message")
	}

	text := messageText(msg)

	var attachment string
	if media, endpoint := mediaOf(msg); media != nil {
		attachment = h.saveMedia(ctx, channel, payload, media, endpoint, clog)
	}

	if text == "" && attachment == "" {
		return nil, channels.Ignore("ignoring message with no text or media")
	}

	// mark it read at WhatsApp now that we've accepted it, so the contact sees their ticks turn blue while the reply
	// is being produced. Best effort: a failure is in the log and doesn't stop the message.
	if event.Info.ID != "" {
		h.markRead(ctx, channel, urn, []string{event.Info.ID}, clog)
	}

	m := models.NewIncomingMsg(channel, urn, text, event.Info.ID, clog).WithContactName(event.Info.PushName)
	if attachment != "" {
		m.WithAttachment(attachment)
	}
	if !event.Info.Timestamp.IsZero() {
		m.WithReceivedOn(event.Info.Timestamp.UTC())
	}

	return m, nil
}

// receiveReceipt turns a receipt event into status updates, one per message it covers
func receiveReceipt(channel *models.Channel, payload *webhook, clog *models.ChannelLog) ([]*models.StatusUpdate, error) {
	var status models.MsgStatus
	switch payload.State {
	case "Delivered":
		status = models.MsgStatusDelivered
	case "Read":
		status = models.MsgStatusRead
	case "ReadSelf":
		return nil, channels.Ignore("ignoring receipt for our own read")
	default:
		return nil, channels.Ignore("ignoring receipt state: %s", payload.State)
	}

	event := &receiptEvent{}
	if err := json.Unmarshal(payload.Event, event); err != nil {
		clog.Error(models.ErrorRequestUnparseable(err))
		return nil, channels.Ignore("unparseable receipt event")
	}

	statuses := make([]*models.StatusUpdate, 0, len(event.MessageIDs))
	for _, id := range event.MessageIDs {
		if id != "" {
			statuses = append(statuses, models.NewStatusUpdateByExternalID(channel, id, status, clog))
		}
	}
	return statuses, nil
}

// senderURN resolves a message's sender to a WhatsApp URN. With LID addressing the sender is a @lid identity and
// the phone number is in the alternative address, so whichever of the two is a phone number JID is used.
func senderURN(sender, senderAlt string) (urns.URN, error) {
	for _, jid := range []string{sender, senderAlt} {
		if phone, ok := phoneFromJID(jid); ok {
			return urns.New(urns.WhatsApp, phone)
		}
	}
	return urns.NilURN, errors.New("sender has no phone number")
}

// phoneFromJID returns the phone number of a phone number JID, e.g. 12065551212 from 12065551212:3@s.whatsapp.net
func phoneFromJID(jid string) (string, bool) {
	user, server, found := strings.Cut(jid, "@")
	if !found || server != "s.whatsapp.net" {
		return "", false
	}
	user, _, _ = strings.Cut(user, ":") // the device part
	return user, user != ""
}

// isUserJID returns whether a JID is an individual user rather than a group, broadcast list or status
func isUserJID(jid string) bool {
	_, server, _ := strings.Cut(jid, "@")
	return server == "s.whatsapp.net" || server == "lid"
}

// unwrap returns the message inside any wrappers - ephemeral, view once, document with caption
func unwrap(m *message) *message {
	for m != nil {
		var wrapper *wrappedMessage
		switch {
		case m.EphemeralMessage != nil:
			wrapper = m.EphemeralMessage
		case m.ViewOnceMessage != nil:
			wrapper = m.ViewOnceMessage
		case m.ViewOnceMessageV2 != nil:
			wrapper = m.ViewOnceMessageV2
		case m.ViewOnceMessageV2Extension != nil:
			wrapper = m.ViewOnceMessageV2Extension
		case m.DocumentWithCaptionMessage != nil:
			wrapper = m.DocumentWithCaptionMessage
		default:
			return m
		}
		if wrapper.Message == nil {
			return m
		}
		m = wrapper.Message
	}
	return nil
}

// messageText returns the text of a message: what was typed, what was tapped, or a media caption
func messageText(m *message) string {
	switch {
	case m.Conversation != "":
		return m.Conversation
	case m.ExtendedTextMessage != nil && m.ExtendedTextMessage.Text != "":
		return m.ExtendedTextMessage.Text
	case m.ListResponseMessage != nil:
		return m.ListResponseMessage.Title
	case m.ButtonsResponseMessage != nil:
		return firstOf(m.ButtonsResponseMessage.SelectedDisplayText, m.ButtonsResponseMessage.SelectedButtonID)
	case m.TemplateButtonReplyMessage != nil:
		return firstOf(m.TemplateButtonReplyMessage.SelectedDisplayText, m.TemplateButtonReplyMessage.SelectedID)
	case m.InteractiveResponseMessage != nil && m.InteractiveResponseMessage.NativeFlowResponseMessage != nil:
		params := &nativeFlowParams{}
		json.Unmarshal([]byte(m.InteractiveResponseMessage.NativeFlowResponseMessage.ParamsJSON), params)
		return firstOf(params.DisplayText, params.ID)
	}
	if media, _ := mediaOf(m); media != nil {
		return media.Caption
	}
	return ""
}

func firstOf(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// mediaOf returns the media a message carries and the bridge endpoint that downloads it, if any. Stickers are
// images as far as downloading goes.
func mediaOf(m *message) (*mediaMessage, string) {
	switch {
	case m.ImageMessage != nil:
		return m.ImageMessage, "downloadimage"
	case m.VideoMessage != nil:
		return m.VideoMessage, "downloadvideo"
	case m.AudioMessage != nil:
		return m.AudioMessage, "downloadaudio"
	case m.DocumentMessage != nil:
		return m.DocumentMessage, "downloaddocument"
	case m.StickerMessage != nil:
		return m.StickerMessage, "downloadimage"
	}
	return nil, ""
}

// saveMedia saves a message's media to our storage and returns it as an attachment, or empty if that wasn't
// possible - which is recorded on the channel log rather than failing the message, so that its text still arrives.
// The bridge normally delivers the media inline; if it didn't, we ask it to download it for us - the CDN serves it
// encrypted and only the bridge holds the session keys.
func (h *handler) saveMedia(ctx context.Context, channel *models.Channel, payload *webhook, media *mediaMessage, endpoint string, clog *models.ChannelLog) string {
	var data []byte
	var err error
	if payload.Base64 != "" {
		data, err = decodeBase64(payload.Base64)
	} else {
		data, err = h.downloadMedia(ctx, channel, media, endpoint, clog)
	}
	if err != nil {
		slog.Debug("unable to get message media", "error", err, "channel_uuid", channel.UUID())
		clog.Error(models.ErrorAttachmentNotDecodable())
		return ""
	}

	contentType, extension := sniffType(data, firstOf(payload.MimeType, media.Mimetype))

	storageURL, err := models.SaveAttachment(ctx, h.Runtime(), channel, contentType, data, extension)
	if err != nil {
		slog.Error("error saving attachment", "error", err, "channel_uuid", channel.UUID())
		clog.Error(errorAttachmentNotSaved())
		return ""
	}

	return contentType + ":" + storageURL
}

// decodeBase64 decodes inline media, tolerating a data URI, and refusing anything over our cap before decoding it
func decodeBase64(s string) ([]byte, error) {
	if strings.HasPrefix(s, "data:") {
		if _, after, found := strings.Cut(s, ","); found {
			s = after
		}
	}
	if base64.StdEncoding.DecodedLen(len(s)) > maxAttachmentBytes {
		return nil, fmt.Errorf("media exceeds %d bytes", maxAttachmentBytes)
	}
	return base64.StdEncoding.DecodeString(s)
}

// sniffType works out the content type of media from its bytes, the way the upload endpoints do, rather than
// trusting what it was declared as. A type the sniffer doesn't know - a plain text document, say - falls back to the
// declared type, with an extension looked up for it.
func sniffType(data []byte, declared string) (string, string) {
	if t, _ := filetype.Match(data); t != filetype.Unknown {
		return t.MIME.Value, t.Extension
	}

	contentType, _, err := mime.ParseMediaType(declared)
	if err != nil {
		return "application/octet-stream", ""
	}
	extension := ""
	if exts, _ := mime.ExtensionsByType(contentType); len(exts) > 0 {
		extension = strings.TrimPrefix(exts[0], ".")
	}
	return contentType, extension
}

// downloadMedia has the bridge download and decrypt a message's media for us
func (h *handler) downloadMedia(ctx context.Context, channel *models.Channel, media *mediaMessage, endpoint string, clog *models.ChannelLog) ([]byte, error) {
	b, err := bridgeFor(channel)
	if err != nil {
		return nil, err
	}

	payload := &downloadPayload{
		URL:           media.URL,
		DirectPath:    media.DirectPath,
		MediaKey:      media.MediaKey,
		Mimetype:      media.Mimetype,
		FileEncSHA256: media.FileEncSHA256,
		FileSHA256:    media.FileSHA256,
		FileLength:    media.FileLength,
	}

	resp, body, err := h.post(ctx, b, "/chat/"+endpoint, payload, clog)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		clog.Error(models.ErrorResponseStatusCode())
		return nil, fmt.Errorf("download responded with status %d", resp.StatusCode)
	}

	// the bridge answers with a data URI
	data, err := jsonparser.GetString(body, "data", "Data")
	if err != nil {
		clog.Error(models.ErrorResponseValueMissing("Data"))
		return nil, err
	}
	return decodeBase64(data)
}

// markRead marks messages read at WhatsApp, best effort - a failure only ends up in the log
func (h *handler) markRead(ctx context.Context, channel *models.Channel, urn urns.URN, ids []string, clog *models.ChannelLog) {
	b, err := bridgeFor(channel)
	if err != nil {
		return
	}

	phone := strings.TrimPrefix(urn.Path(), "+")
	payload := &markReadPayload{IDs: ids, ChatPhone: phone, SenderPhone: phone}

	resp, _, err := h.post(ctx, b, "/chat/markread", payload, clog)
	if err != nil {
		slog.Debug("error marking messages read", "error", err, "channel_uuid", channel.UUID())
	} else if resp.StatusCode/100 != 2 {
		slog.Debug("error marking messages read", "status", resp.StatusCode, "channel_uuid", channel.UUID())
	}
}

// Send implements the channels.Handler interface. Each attachment goes as its own media message, the text as the
// caption of the last one that can carry one - or as its own message, interactive if it has quick replies.
func (h *handler) Send(ctx context.Context, msg *models.MsgOut, res *channels.SendResult, clog *models.ChannelLog) error {
	b, err := bridgeFor(msg.Channel())
	if err != nil {
		return err
	}

	phone := strings.TrimPrefix(msg.URN().Path(), "+")

	attachments, err := h.resolveAttachments(ctx, msg, clog)
	if err != nil {
		return err
	}

	text := msg.Text()

	// url quick replies can't be tappable buttons on this channel, so they go in the text as labelled links
	for _, qr := range handlers.FilterQuickRepliesByType(msg.QuickReplies(), "url") {
		if qr.Extra != "" {
			text += "\n\n" + qr.Text + ": " + qr.Extra
		}
	}

	qrs := handlers.FilterQuickRepliesByType(msg.QuickReplies(), "text")
	asList := len(qrs) > 3
	for _, qr := range qrs {
		if qr.Extra != "" {
			asList = true // a description makes it a list row
		}
	}

	// a button message can carry an image as its header, so the first attachment goes there when it's one
	headerImage := ""
	if len(qrs) > 0 && !asList && len(attachments) > 0 && attachments[0].Type == handlers.MediaTypeImage {
		headerImage = attachments[0].URL
		attachments = attachments[1:]
	}

	// the text goes as the caption of the last attachment that can carry one, unless it has to be interactive
	captionIdx := -1
	if len(qrs) == 0 && text != "" {
		for i := len(attachments) - 1; i >= 0; i-- {
			if attachments[i].Type != handlers.MediaTypeAudio {
				captionIdx = i
				break
			}
		}
	}

	for i, att := range attachments {
		caption := ""
		if i == captionIdx {
			caption = text
		}
		if err := h.sendMedia(ctx, b, phone, att, caption, res, clog); err != nil {
			return err
		}
	}

	if captionIdx >= 0 {
		return nil // the text went as a caption
	}
	if text == "" && len(qrs) == 0 {
		if len(attachments) == 0 {
			return channels.ErrMessageInvalid // nothing to send
		}
		return nil
	}

	var payload any
	endpoint := "text"

	switch {
	case len(qrs) == 0:
		payload = &textPayload{Phone: phone, Body: text}

	case asList:
		endpoint = "list"
		payload = &listPayload{
			Phone:      phone,
			Desc:       text,
			ButtonText: msg.Channel().StringConfigForKey(configListButton, "Select"),
			Sections:   listSections(qrs, msg.Channel().StringConfigForKey(configListSection, "Menu")),
			FooterText: msg.Channel().StringConfigForKey(configListFooter, ""),
		}

	default:
		endpoint = "buttons"
		buttons := make([]button, len(qrs))
		for i, qr := range qrs {
			buttons[i] = button{Type: "reply", Title: qr.Text, ID: strconv.Itoa(i)}
		}
		payload = &buttonsPayload{Phone: phone, Body: text, Buttons: buttons, Image: headerImage}
	}

	return h.send(ctx, b, endpoint, payload, res, clog)
}

// listSections groups quick replies into the sections of a list message. A quick reply written "Section|Title"
// opens, or continues, a section of that name; one without goes in the default section.
func listSections(qrs []models.QuickReply, defaultSection string) []listSection {
	var sections []listSection

	for i, qr := range qrs {
		title, sectionTitle := qr.Text, defaultSection
		if k := strings.Index(title, "|"); k > 0 && k < len(title)-1 {
			sectionTitle, title = strings.TrimSpace(title[:k]), strings.TrimSpace(title[k+1:])
		}
		if len(sections) == 0 || sections[len(sections)-1].Title != sectionTitle {
			sections = append(sections, listSection{Title: sectionTitle})
		}
		last := &sections[len(sections)-1]
		last.Rows = append(last.Rows, listRow{Title: title, Description: qr.Extra, RowID: strconv.Itoa(i)})
	}

	return sections
}

// resolveAttachments resolves the message's attachments, first completing any URL without a host against our media
// domain - which is how media stored under the platform's own media path arrives when mailroom has no attachment
// domain configured. The bridge fetches media from the URLs we give it, so they have to be ones it can reach.
func (h *handler) resolveAttachments(ctx context.Context, msg *models.MsgOut, clog *models.ChannelLog) ([]*handlers.Attachment, error) {
	mediaDomain := h.Runtime().Config.MediaDomain
	attachments := make([]string, 0, len(msg.Attachments()))

	for _, a := range msg.Attachments() {
		contentType, mediaURL := handlers.SplitAttachment(a)

		parsed, err := url.Parse(mediaURL)
		if err != nil || (parsed.Host == "" && mediaDomain == "") {
			clog.Error(models.ErrorMediaUnresolveable(contentType))
			continue
		}
		if parsed.Host == "" {
			parsed.Scheme, parsed.Host = "https", mediaDomain
			mediaURL = parsed.String()
		}

		attachments = append(attachments, contentType+":"+mediaURL)
	}

	return handlers.ResolveAttachments(ctx, h.Runtime(), attachments, nil, true, clog)
}

// sendMedia sends an attachment as a media message of the matching kind
func (h *handler) sendMedia(ctx context.Context, b *bridge, phone string, att *handlers.Attachment, caption string, res *channels.SendResult, clog *models.ChannelLog) error {
	var endpoint string
	var payload any

	switch att.Type {
	case handlers.MediaTypeImage:
		endpoint, payload = "image", &imagePayload{Phone: phone, Image: att.URL, Caption: caption}
	case handlers.MediaTypeVideo:
		endpoint, payload = "video", &videoPayload{Phone: phone, Video: att.URL, Caption: caption}
	case handlers.MediaTypeAudio:
		endpoint, payload = "audio", &audioPayload{Phone: phone, Audio: att.URL}
	default:
		endpoint, payload = "document", &documentPayload{Phone: phone, Document: att.URL, FileName: firstOf(att.Name, "document"), Caption: caption}
	}

	return h.send(ctx, b, endpoint, payload, res, clog)
}

// send posts a message to the bridge and records the ID WhatsApp gave it
func (h *handler) send(ctx context.Context, b *bridge, endpoint string, payload any, res *channels.SendResult, clog *models.ChannelLog) error {
	resp, body, err := h.post(ctx, b, "/chat/send/"+endpoint, payload, clog)
	if err := sendError(resp, body, err); err != nil {
		return err
	}

	id, err := jsonparser.GetString(body, "data", "Id")
	if err != nil || id == "" {
		clog.Error(models.ErrorResponseValueMissing("Id"))
		return channels.ErrResponseUnexpected
	}

	res.AddExternalID(id)
	return nil
}

// sendError converts the outcome of a send request into the error to fail it with, if any. Not reaching the bridge
// or an error inside it is retried; anything it rejected is failed with the reason it gave.
func sendError(resp *http.Response, body []byte, err error) error {
	if err := handlers.ErrorFromResponse(resp, err); err != nil {
		if err == channels.ErrResponseStatus {
			if reason, _ := jsonparser.GetString(body, "error"); reason != "" {
				return channels.ErrFailedWithReason(strconv.Itoa(resp.StatusCode), reason)
			}
		}
		return err
	}
	return nil
}

// SendableEvents declares support for typing indicators
func (h *handler) SendableEvents(*models.Channel) map[string]time.Duration {
	return sendableEvents
}

// SendEvent sends typing indicators as chat presence, first marking the message being replied to read when we know
// it - read, then typing, is what a person does
func (h *handler) SendEvent(ctx context.Context, channel *models.Channel, event events.Event, clog *models.ChannelLog) error {
	b, err := bridgeFor(channel)
	if err != nil {
		return err
	}

	var urn urns.URN
	var state, msgID string

	switch typed := event.(type) {
	case *events.TypingStarted:
		urn, state, msgID = typed.URN, "composing", typed.MsgExternalID
	case *events.TypingStopped:
		urn, state = typed.URN, "paused"
	default:
		return fmt.Errorf("unsupported event type: %s", event.Type())
	}

	if msgID != "" {
		h.markRead(ctx, channel, urn, []string{msgID}, clog)
	}

	phone := strings.TrimPrefix(urn.Path(), "+")

	resp, body, err := h.post(ctx, b, "/chat/presence", &presencePayload{Phone: phone, State: state}, clog)
	return sendError(resp, body, err)
}

// bridge is a channel's bridge: where it is and what it accepts as authentication
type bridge struct {
	url   string
	token string
}

func bridgeFor(channel *models.Channel) (*bridge, error) {
	b := &bridge{
		url:   strings.TrimSuffix(channel.StringConfigForKey(configURL, ""), "/"),
		token: channel.StringConfigForKey(configToken, ""),
	}
	if b.url == "" || b.token == "" {
		return nil, channels.ErrChannelConfig
	}
	return b, nil
}

// post sends a JSON request to the bridge, logging the exchange
func (h *handler) post(ctx context.Context, b *bridge, path string, payload any, clog *models.ChannelLog) (*http.Response, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.url+path, bytes.NewReader(jsonx.MustMarshal(payload)))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", b.token)

	return h.RequestHTTP(req, clog)
}

// the requests we make of the bridge

type textPayload struct {
	Phone string `json:"Phone"`
	Body  string `json:"Body"`
}

type imagePayload struct {
	Phone   string `json:"Phone"`
	Image   string `json:"Image"`
	Caption string `json:"Caption,omitempty"`
}

type videoPayload struct {
	Phone   string `json:"Phone"`
	Video   string `json:"Video"`
	Caption string `json:"Caption,omitempty"`
}

type audioPayload struct {
	Phone string `json:"Phone"`
	Audio string `json:"Audio"`
}

type documentPayload struct {
	Phone    string `json:"Phone"`
	Document string `json:"Document"`
	FileName string `json:"FileName"`
	Caption  string `json:"Caption,omitempty"`
}

type buttonsPayload struct {
	Phone   string   `json:"Phone"`
	Body    string   `json:"Body"`
	Buttons []button `json:"Buttons"`
	Image   string   `json:"Image,omitempty"`
}

type button struct {
	Type  string `json:"type"`
	Title string `json:"title"`
	ID    string `json:"id"`
}

type listPayload struct {
	Phone      string        `json:"Phone"`
	Desc       string        `json:"Desc"`
	ButtonText string        `json:"ButtonText"`
	Sections   []listSection `json:"Sections"`
	FooterText string        `json:"FooterText,omitempty"`
}

type listSection struct {
	Title string    `json:"title"`
	Rows  []listRow `json:"rows"`
}

type listRow struct {
	Title       string `json:"title"`
	Description string `json:"desc"`
	RowID       string `json:"rowId"`
}

type presencePayload struct {
	Phone string `json:"Phone"`
	State string `json:"State"`
	Media string `json:"Media"`
}

type markReadPayload struct {
	IDs         []string `json:"Id"`
	ChatPhone   string   `json:"ChatPhone"`
	SenderPhone string   `json:"SenderPhone"`
}

type downloadPayload struct {
	URL           string `json:"Url"`
	DirectPath    string `json:"DirectPath"`
	MediaKey      string `json:"MediaKey"`
	Mimetype      string `json:"Mimetype"`
	FileEncSHA256 string `json:"FileEncSHA256"`
	FileSHA256    string `json:"FileSHA256"`
	FileLength    uint64 `json:"FileLength"`
}

// the errors we record on channel logs that no shared constructor covers

func errorUnauthenticated(err error) *svclogs.Error {
	return &svclogs.Error{Code: "request_unauthenticated", Message: fmt.Sprintf("Request could not be verified: %s.", err)}
}

func errorAttachmentNotSaved() *svclogs.Error {
	return &svclogs.Error{Code: "attachment_not_saved", Message: "Unable to save attachment to storage."}
}
