package openclaw

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"

	"math/rand"
	"time"

	"github.com/buger/jsonparser"
	"github.com/nyaruka/courier"
	"github.com/nyaruka/courier/core/models"
	"github.com/nyaruka/courier/handlers"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/urns"
)

func init() {
	courier.RegisterHandler(NewHandler())
}

// OpenClawHandler is the handler for OpenClaw
type OpenClawHandler struct {
	handlers.BaseHandler
}

// OpenClawPayload represents the top-level incoming webhook structure
type OpenClawPayload struct {
	Type        string          `json:"type"`        // "Message" or "ReadReceipt"
	Payload     json.RawMessage `json:"payload"`     // Polymorphic, depends on Type
	Event       json.RawMessage `json:"event"`       // Alternate key sometimes used
	ReceiptType string          `json:"receiptType"` // For ReadReceipt
	Status      string          `json:"status"`      // For ReadReceipt
	ID          string          `json:"id"`          // Message ID
}

// OpenClawEvent corresponds to the "event" key in the payload
type OpenClawEvent struct {
	Info    OpenClawInfo    `json:"Info"`
	Message OpenClawMessage `json:"Message"`
	Status  string          `json:"status"` // Possible location for status updates?
}

type OpenClawInfo struct {
	Sender    interface{} `json:"Sender"` // Can be string or struct
	SenderAlt interface{} `json:"SenderAlt"`
	Chat      interface{} `json:"Chat"` // Valid in some versions for context
	ID        string      `json:"ID"`
	PushName  string      `json:"PushName"`
	Timestamp interface{} `json:"Timestamp"` // Sometimes string, sometimes int
	Status    string      `json:"Status"`    // Maybe here?
	IsFromMe  bool        `json:"IsFromMe"`
	IsGroup   bool        `json:"IsGroup"`
}

type OpenClawMessage struct {
	Conversation        string `json:"conversation"`
	Body                string `json:"body"`
	Text                string `json:"text"` // Some versions
	ExtendedTextMessage struct {
		Text string `json:"text"`
	} `json:"extendedTextMessage"`
	ImageMessage    OpenClawMediaMessage `json:"imageMessage"`
	VideoMessage    OpenClawMediaMessage `json:"videoMessage"`
	AudioMessage    OpenClawMediaMessage `json:"audioMessage"`
	DocumentMessage OpenClawMediaMessage `json:"documentMessage"`
	StickerMessage  OpenClawMediaMessage `json:"stickerMessage"`
	VoiceMessage    OpenClawMediaMessage `json:"voiceMessage"` // PTT

	// Contextual Wrappers
	EphemeralMessage *struct {
		Message *OpenClawMessage `json:"message"`
	} `json:"ephemeralMessage"`
	ViewOnceMessage *struct {
		Message *OpenClawMessage `json:"message"`
	} `json:"viewOnceMessage"`
}

type OpenClawMediaMessage struct {
	Caption  string `json:"caption"` // Image/Video/Doc
	Mimetype string `json:"mimetype"`
	URL      string `json:"url"`
	// Additional fields for download fallback if needed
}

func (h *OpenClawHandler) handleMessageInternal(ctx context.Context, channel courier.Channel, payload *OpenClawPayload, clog *courier.ChannelLog, w http.ResponseWriter, r *http.Request) ([]courier.Event, error) {
	// Try parsing "Event" first (Standard OpenClaw Shim)
	var event OpenClawEvent
	if len(payload.Event) > 0 {
		if err := json.Unmarshal(payload.Event, &event); err != nil {
			fmt.Printf("OpenClaw ERROR: invalid event payload: %s\n", err) // Debug log
			return nil, nil                                                // Return 200 to clear queue
		}
	} else if len(payload.Payload) > 0 {
		// Fallback: Maybe payload has the event data directly?
		if err := json.Unmarshal(payload.Payload, &event); err != nil {
			fmt.Printf("OpenClaw ERROR: invalid payload (fallback): %s\n", err) // Debug log
			return nil, nil                                                     // Return 200 to clear queue
		}
	} else {
		fmt.Printf("OpenClaw ERROR: empty event and payload\n")
		return nil, nil // Return 200 to clear queue
	}

	senderStr, _ := event.Info.Sender.(string)
	senderAltStr, _ := event.Info.SenderAlt.(string)
	chatStr, _ := event.Info.Chat.(string)

	log.Printf("OpenClaw DEBUG: Sender='%s' SenderAlt='%s' Chat='%s' EventType=%s IsFromMe=%v IsGroup=%v", senderStr, senderAltStr, chatStr, event.Info.Status, event.Info.IsFromMe, event.Info.IsGroup)

	if event.Info.IsFromMe {
		log.Printf("OpenClaw DEBUG: Ignoring IsFromMe message (Self)")
		return nil, nil
	}

	if event.Info.IsGroup {
		log.Printf("OpenClaw DEBUG: Ignoring Group Message (IsGroup=true)")
		return nil, nil
	}

	if strings.HasSuffix(senderStr, "@g.us") || strings.Contains(senderStr, "-") {
		log.Printf("OpenClaw DEBUG: Ignoring Group Message (Suffix Check)")
		return nil, nil // Group message
	}

	if strings.Contains(senderStr, "@broadcast") || strings.Contains(chatStr, "@broadcast") {
		log.Printf("OpenClaw DEBUG: Ignoring Broadcast/Status: Sender=%s Chat=%s", senderStr, chatStr)
		return nil, nil
	}

	if senderStr == "" {
		return nil, handlers.WriteAndLogRequestError(ctx, h, channel, w, r, fmt.Errorf("missing sender"))
	}

	phoneSource := senderStr
	if strings.Contains(senderAltStr, "@s.whatsapp.net") {
		log.Printf("OpenClaw DEBUG: Using SenderAlt for phone resolution: %s", senderAltStr)
		phoneSource = senderAltStr
	}

	phone := strings.Split(phoneSource, "@")[0]
	phone = strings.Split(phone, ":")[0]
	phone = strings.TrimSpace(phone)
	phone = strings.TrimPrefix(phone, "+")

	log.Printf("OpenClaw DEBUG: NormalizedPhone='%s'", phone)

	if len(phone) > 15 {
		log.Printf("OpenClaw DEBUG: Ignoring suspiciously long number (Legacy/Broadcast Group ID?): %s", phone)
		return nil, nil
	}

	urn, err := urns.New(urns.WhatsApp, phone)
	if err != nil {
		return nil, handlers.WriteAndLogRequestError(ctx, h, channel, w, r, fmt.Errorf("invalid whatsapp urn: %w", err))
	}

	if event.Message.EphemeralMessage != nil && event.Message.EphemeralMessage.Message != nil {
		event.Message = *event.Message.EphemeralMessage.Message
	}
	if event.Message.ViewOnceMessage != nil && event.Message.ViewOnceMessage.Message != nil {
		event.Message = *event.Message.ViewOnceMessage.Message
	}

	text := event.Message.ExtendedTextMessage.Text
	if text == "" {
		text = event.Message.Body
	}
	if text == "" {
		text = event.Message.Text
	}
	if text == "" {
		text = event.Message.Conversation
	}

	var mediaURL string
	var mediaMsg OpenClawMediaMessage
	var isMedia bool

	if event.Message.ImageMessage.URL != "" {
		mediaMsg = event.Message.ImageMessage
		isMedia = true
		if text == "" {
			text = mediaMsg.Caption
		}
		if text == "" {
			text = "Image"
		}
	} else if event.Message.VideoMessage.URL != "" {
		mediaMsg = event.Message.VideoMessage
		isMedia = true
		if text == "" {
			text = mediaMsg.Caption
		}
		if text == "" {
			text = "Video"
		}
	} else if event.Message.AudioMessage.URL != "" {
		mediaMsg = event.Message.AudioMessage
		isMedia = true
		if text == "" {
			text = "Audio"
		}
	} else if event.Message.VoiceMessage.URL != "" {
		mediaMsg = event.Message.VoiceMessage
		isMedia = true
		if text == "" {
			text = "Voice Message"
		}
	} else if event.Message.DocumentMessage.URL != "" {
		mediaMsg = event.Message.DocumentMessage
		isMedia = true
		if text == "" {
			text = mediaMsg.Caption
		}
		if text == "" {
			text = "Document"
		}
	} else if event.Message.StickerMessage.URL != "" {
		mediaMsg = event.Message.StickerMessage
		isMedia = true
		if text == "" {
			text = "Sticker"
		}
	}

	if isMedia {
		mediaData := &OpenClawMediaData{
			Url:      mediaMsg.URL,
			Mimetype: mediaMsg.Mimetype,
		}

		raw, mimeType, err := h.downloadMedia(ctx, channel, mediaData)
		if err == nil {
			savedURL, err := h.Backend().SaveAttachment(ctx, channel, event.Info.ID, raw, mimeType)
			if err == nil {
				mediaURL = savedURL
			}
		} else {
			clog.Error(courier.ErrorExternal("download_failed", fmt.Sprintf("failed to download media: %s", err)))
		}
	}

	if text == "" && mediaURL == "" {
		log.Printf("OpenClaw DEBUG: Empty text and no media. Ignoring event.")
		return nil, nil
	}

	msg := h.Backend().NewIncomingMsg(ctx, channel, urn, text, event.Info.ID, clog).
		WithContactName(event.Info.PushName)

	if mediaURL != "" {
		msg.WithAttachment(mediaURL)
	}

	return handlers.WriteMsgsAndResponse(ctx, h, []courier.MsgIn{msg}, w, r, clog)
}

// NewHandler creates a new handler
func NewHandler() courier.ChannelHandler {
	return &OpenClawHandler{
		handlers.NewBaseHandler("OC", "OpenClaw", handlers.WithRedactConfigKeys("openclaw_token", "hmac_key")),
	}
}

// Initialize is called by the engine once everything is loaded
func (h *OpenClawHandler) Initialize(s courier.Server) error {
	h.SetServer(s)
	s.AddHandlerRoute(h, http.MethodPost, "receive", courier.ChannelLogTypeMsgReceive, h.handleWebhook)
	return nil
}

// handleWebhook is our HTTP handler function for incoming messages
func (h *OpenClawHandler) handleWebhook(ctx context.Context, channel courier.Channel, w http.ResponseWriter, r *http.Request, clog *courier.ChannelLog) ([]courier.Event, error) {
	// Debug Logging: Log the raw payload to debug mismatches
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, handlers.WriteAndLogRequestError(ctx, h, channel, w, r, fmt.Errorf("failed to read body: %w", err))
	}
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	fmt.Printf("OpenClaw Raw Webhook: %s\n", string(bodyBytes))

	var payload OpenClawPayload
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		query, parseErr := url.ParseQuery(string(bodyBytes))
		if parseErr == nil && query.Has("jsonData") {
			jsonData := query.Get("jsonData")
			if jsonData == "" {
				fmt.Printf("OpenClaw ERROR: empty jsonData fallback\n")
				return nil, nil // Return 200 to clear queue
			}
			fmt.Printf("OpenClaw DEBUG: jsonData Fallback: %s\n", jsonData)
			if jsonErr := json.Unmarshal([]byte(jsonData), &payload); jsonErr != nil {
				log.Printf("OpenClaw ERROR: invalid json in jsonData: %s | DATA: %s", jsonErr, jsonData)
				return nil, nil // Return 200 to clear queue
			}
		} else {
			log.Printf("OpenClaw ERROR: invalid json: %s | BODY: %s", err, string(bodyBytes))
			return nil, nil // Return 200 to clear queue
		}
	}

	if strings.EqualFold(payload.Type, "Message") {
		return h.handleMessageInternal(ctx, channel, &payload, clog, w, r)
	} else if strings.EqualFold(payload.Type, "ReadReceipt") {
		return h.handleReceiptInternal(ctx, channel, &payload, clog, w, r)
	}

	return nil, nil // Ignored event
}

// Temporary struct for media download
type OpenClawMediaData struct {
	Url           string
	DirectPath    string
	MediaKey      string
	Mimetype      string
	FileEncSHA256 string
	FileSHA256    string
	FileLength    uint64
}

func (h *OpenClawHandler) downloadMedia(ctx context.Context, channel courier.Channel, data *OpenClawMediaData) ([]byte, string, error) {

	openclawURL := channel.StringConfigForKey("openclaw_url", "")
	token := channel.StringConfigForKey("openclaw_token", "")

	endpoint := "downloaddocument" // default
	if strings.HasPrefix(data.Mimetype, "image") {
		endpoint = "downloadimage"
	} else if strings.HasPrefix(data.Mimetype, "video") {
		endpoint = "downloadvideo"
	} else if strings.HasPrefix(data.Mimetype, "audio") {
		endpoint = "downloadaudio"
	}

	if strings.HasPrefix(data.Url, "http") {
		resp, err := http.Get(data.Url)
		if err == nil && resp.StatusCode == 200 {
			d, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return d, resp.Header.Get("Content-Type"), nil
		}
	}

	reqData := map[string]interface{}{
		"Url":           data.Url,
		"DirectPath":    data.DirectPath,
		"MediaKey":      data.MediaKey,
		"Mimetype":      data.Mimetype,
		"FileEncSHA256": data.FileEncSHA256,
		"FileSHA256":    data.FileSHA256,
		"FileLength":    data.FileLength,
	}

	jsonBody, _ := json.Marshal(reqData)
	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/chat/%s", openclawURL, endpoint), bytes.NewBuffer(jsonBody))
	req.Header.Set("Authorization", token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.Backend().HttpClient(false).Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, "", fmt.Errorf("status %d", resp.StatusCode)
	}

	respDec := struct {
		Data     string `json:"Data"` // Base64
		Mimetype string `json:"Mimetype"`
	}{}

	if err := json.NewDecoder(resp.Body).Decode(&respDec); err != nil {
		return nil, "", err
	}

	parts := strings.Split(respDec.Data, ",")
	if len(parts) < 2 {
		return nil, "", fmt.Errorf("invalid base64 response")
	}

	raw, err := base64.StdEncoding.DecodeString(parts[1])
	return raw, respDec.Mimetype, err
}

func (h *OpenClawHandler) handleReceiptInternal(ctx context.Context, channel courier.Channel, payload *OpenClawPayload, clog *courier.ChannelLog, w http.ResponseWriter, r *http.Request) ([]courier.Event, error) {
	var status models.MsgStatus

	s := payload.Status
	if s == "" {
		s = payload.ReceiptType
	}

	switch s {
	case "read":
		status = models.MsgStatusRead
	case "delivered":
		status = models.MsgStatusDelivered
	case "sent":
		status = models.MsgStatusSent
	default:
		return nil, nil
	}

	id := payload.ID

	if id == "" {
		return nil, nil // No ID
	}

	event := h.Backend().NewStatusUpdateByExternalID(channel, id, status, clog)
	return handlers.WriteMsgStatusAndResponse(ctx, h, channel, event, w, r)
}

func (h *OpenClawHandler) Send(ctx context.Context, msg courier.MsgOut, res *courier.SendResult, log *courier.ChannelLog) error {
	ms := 1500 + rand.Intn(2000)
	time.Sleep(time.Duration(ms) * time.Millisecond)

	openclawURL := msg.Channel().StringConfigForKey("openclaw_url", "")
	token := msg.Channel().StringConfigForKey("openclaw_token", "")

	if openclawURL == "" || token == "" {
		return fmt.Errorf("missing openclaw_url or openclaw_token in config")
	}

	phone := strings.TrimPrefix(msg.URN().Path(), "+")

	attachments := msg.Attachments()

	if len(attachments) > 0 {
		attType, attURL := handlers.SplitAttachment(attachments[0])

		endpointType := "document"
		if strings.HasPrefix(attType, "image") {
			endpointType = "image"
		} else if strings.HasPrefix(attType, "video") {
			endpointType = "video"
		} else if strings.HasPrefix(attType, "audio") {
			endpointType = "audio"
		}

		if strings.HasPrefix(attURL, "https:///rp/") {
			attURL = strings.Replace(attURL, "https:///rp/", "http://localhost/rp/", 1)
		} else if strings.HasPrefix(attURL, "http:///rp/") {
			attURL = strings.Replace(attURL, "http:///rp/", "http://localhost/rp/", 1)
		}

		payload := map[string]string{
			"Phone":   phone,
			"Body":    attURL,
			"Caption": msg.Text(),
		}
		jsonBody, _ := json.Marshal(payload)

		url := fmt.Sprintf("%s/chat/send/%s", openclawURL, endpointType)
		return h.doRequest(url, token, jsonBody, res, log)
	}

	payload := map[string]string{
		"Phone": phone,
		"Body":  msg.Text(),
	}
	jsonBody, _ := json.Marshal(payload)
	url := fmt.Sprintf("%s/chat/send/text", openclawURL)

	return h.doRequest(url, token, jsonBody, res, log)
}

func (h *OpenClawHandler) doRequest(url string, token string, body []byte, res *courier.SendResult, log *courier.ChannelLog) error {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	fmt.Printf("OpenClaw DEBUG: Sending %s Payload: %s\n", url, string(body))
	req.Header.Set("Authorization", token)
	req.Header.Set("token", token) // Compatibility
	req.Header.Set("Content-Type", "application/json")

	resp, respBody, err := h.requestHTTPUnsafe(req, log)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("openclaw error: %s", string(respBody))
	}

	id, _ := jsonparser.GetString(respBody, "data", "id")
	if id == "" {
		id, _ = jsonparser.GetString(respBody, "data", "Id")
	}
	if id == "" {
		id, _ = jsonparser.GetString(respBody, "newId") 
	}

	fmt.Printf("OpenClaw DEBUG: Response: %s\n", string(respBody))

	if id != "" {
		res.AddExternalID(id)
	}
	return nil
}

func (h *OpenClawHandler) requestHTTPUnsafe(req *http.Request, clog *courier.ChannelLog) (*http.Response, []byte, error) {
	client := h.Backend().HttpClient(false)

	req.Header.Set("User-Agent", fmt.Sprintf("Courier/%s", h.Server().Config().Version))

	trace, err := httpx.DoTrace(client, req, nil, nil, 0)
	if trace != nil {
		clog.HTTP(trace)
		return trace.Response, trace.ResponseBody, nil
	}
	return nil, nil, err
}
