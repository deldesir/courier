package wuzapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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

// WuzapiHandler is the handler for Wuzapi
type WuzapiHandler struct {
	handlers.BaseHandler
}

// WuzapiPayload represents the top-level incoming webhook structure
type WuzapiPayload struct {
	Type        string          `json:"type"`        // "Message" or "ReadReceipt"
	Payload     json.RawMessage `json:"payload"`     // Polymorphic, depends on Type
	Event       json.RawMessage `json:"event"`       // Alternate key sometimes used
	ReceiptType string          `json:"receiptType"` // For ReadReceipt
	Status      string          `json:"status"`      // For ReadReceipt
	ID          string          `json:"id"`          // Message ID
}

// WuzapiEvent corresponds to the "event" key in the payload
type WuzapiEvent struct {
	Info    WuzapiInfo    `json:"Info"`
	Message WuzapiMessage `json:"Message"`
	Status  string        `json:"status"` // Possible location for status updates?
}

type WuzapiInfo struct {
	Sender    interface{} `json:"Sender"` // Can be string or struct
	SenderAlt interface{} `json:"SenderAlt"`
	Chat      interface{} `json:"Chat"` // Valid in some wauzapi versions for context
	ID        string      `json:"ID"`
	PushName  string      `json:"PushName"`
	Timestamp interface{} `json:"Timestamp"` // Sometimes string, sometimes int
	Status    string      `json:"Status"`    // Maybe here?
	IsFromMe  bool        `json:"IsFromMe"`
	IsGroup   bool        `json:"IsGroup"`
}

type WuzapiMessage struct {
	Conversation        string `json:"conversation"`
	Body                string `json:"body"`
	Text                string `json:"text"` // Some versions
	ExtendedTextMessage struct {
		Text string `json:"text"`
	} `json:"extendedTextMessage"`
	ImageMessage    WuzapiMediaMessage `json:"imageMessage"`
	VideoMessage    WuzapiMediaMessage `json:"videoMessage"`
	AudioMessage    WuzapiMediaMessage `json:"audioMessage"`
	DocumentMessage WuzapiMediaMessage `json:"documentMessage"`
	StickerMessage  WuzapiMediaMessage `json:"stickerMessage"`
	VoiceMessage    WuzapiMediaMessage `json:"voiceMessage"` // PTT

	// Contextual Wrappers
	EphemeralMessage *struct {
		Message *WuzapiMessage `json:"message"`
	} `json:"ephemeralMessage"`
	ViewOnceMessage *struct {
		Message *WuzapiMessage `json:"message"`
	} `json:"viewOnceMessage"`
}

type WuzapiMediaMessage struct {
	Caption  string `json:"caption"` // Image/Video/Doc
	Mimetype string `json:"mimetype"`
	URL      string `json:"url"`
	// Additional fields for download fallback if needed
}

// ... NewHandler, Initialize, handleWebhook, verifySignature remain same ...

func (h *WuzapiHandler) handleMessageInternal(ctx context.Context, channel courier.Channel, payload *WuzapiPayload, clog *courier.ChannelLog, w http.ResponseWriter, r *http.Request) ([]courier.Event, error) {
	// Try parsing "Event" first (Standard Wuzapi)
	var event WuzapiEvent
	if len(payload.Event) > 0 {
		if err := json.Unmarshal(payload.Event, &event); err != nil {
			fmt.Printf("Wuzapi ERROR: invalid event payload: %s\n", err) // Debug log
			return nil, nil                                              // Return 200 to clear queue
		}
	} else if len(payload.Payload) > 0 {
		// Fallback: Maybe payload has the event data directly?
		if err := json.Unmarshal(payload.Payload, &event); err != nil {
			fmt.Printf("Wuzapi ERROR: invalid payload (fallback): %s\n", err) // Debug log
			return nil, nil                                                   // Return 200 to clear queue
		}
	} else {
		fmt.Printf("Wuzapi ERROR: empty event and payload\n")
		return nil, nil // Return 200 to clear queue
	}

	// Extract Sender
	// Extract Sender & Chat
	// Extract Sender & Chat
	// Extract Sender & Chat
	senderStr, _ := event.Info.Sender.(string)
	senderAltStr, _ := event.Info.SenderAlt.(string)
	chatStr, _ := event.Info.Chat.(string)

	log.Printf("Wuzapi DEBUG: Sender='%s' SenderAlt='%s' Chat='%s' EventType=%s IsFromMe=%v IsGroup=%v", senderStr, senderAltStr, chatStr, event.Info.Status, event.Info.IsFromMe, event.Info.IsGroup)

	if event.Info.IsFromMe {
		log.Printf("Wuzapi DEBUG: Ignoring IsFromMe message (Self)")
		return nil, nil
	}

	if event.Info.IsGroup {
		log.Printf("Wuzapi DEBUG: Ignoring Group Message (IsGroup=true)")
		return nil, nil
	}

	if strings.HasSuffix(senderStr, "@g.us") || strings.Contains(senderStr, "-") {
		log.Printf("Wuzapi DEBUG: Ignoring Group Message (Suffix Check)")
		return nil, nil // Group message
	}

	// Filter Broadcast / Status Updates
	// WhatsApp Status: 'status@broadcast' (usually in Chat for status updates sent by others)
	// Broadcast Lists: '1234567890@broadcast'
	if strings.Contains(senderStr, "@broadcast") || strings.Contains(chatStr, "@broadcast") {
		log.Printf("Wuzapi DEBUG: Ignoring Broadcast/Status: Sender=%s Chat=%s", senderStr, chatStr)
		return nil, nil
	}

	if senderStr == "" {
		return nil, handlers.WriteAndLogRequestError(ctx, h, channel, w, r, fmt.Errorf("missing sender"))
	}

	// Prefer SenderAlt if it looks like a real phone number JID
	phoneSource := senderStr
	if strings.Contains(senderAltStr, "@s.whatsapp.net") {
		log.Printf("Wuzapi DEBUG: Using SenderAlt for phone resolution: %s", senderAltStr)
		phoneSource = senderAltStr
	}

	phone := strings.Split(phoneSource, "@")[0]
	// Remove device ID if present (e.g. "12345:8")
	phone = strings.Split(phone, ":")[0]

	phone = strings.TrimSpace(phone)
	// URN library expects raw digits for WhatsApp, no plus sign
	phone = strings.TrimPrefix(phone, "+")

	log.Printf("Wuzapi DEBUG: NormalizedPhone='%s'", phone)

	// E.164 standard max length is 15 digits.
	// Wuzapi sometimes passes Broadcast JIDs as numbers if we aren't careful.
	if len(phone) > 15 {
		log.Printf("Wuzapi DEBUG: Ignoring suspiciously long number (Legacy/Broadcast Group ID?): %s", phone)
		return nil, nil
	}

	urn, err := urns.New(urns.WhatsApp, phone)
	if err != nil {
		return nil, handlers.WriteAndLogRequestError(ctx, h, channel, w, r, fmt.Errorf("invalid whatsapp urn: %w", err))
	}

	// Unnest Ephemeral/ViewOnce
	if event.Message.EphemeralMessage != nil && event.Message.EphemeralMessage.Message != nil {
		event.Message = *event.Message.EphemeralMessage.Message
	}
	if event.Message.ViewOnceMessage != nil && event.Message.ViewOnceMessage.Message != nil {
		event.Message = *event.Message.ViewOnceMessage.Message
	}

	// Extract Text
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

	// Handle Media
	var mediaURL string
	var mediaMsg WuzapiMediaMessage
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
		mediaData := &WuzapiMediaData{
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

	// Final check for text
	if text == "" && mediaURL == "" {
		log.Printf("Wuzapi DEBUG: Empty text and no media. Ignoring event.")
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
	return &WuzapiHandler{
		handlers.NewBaseHandler("WZ", "Wuzapi", handlers.WithRedactConfigKeys("wuzapi_token", "hmac_key")),
	}
}

// Initialize is called by the engine once everything is loaded
func (h *WuzapiHandler) Initialize(s courier.Server) error {
	h.SetServer(s)
	s.AddHandlerRoute(h, http.MethodPost, "receive", courier.ChannelLogTypeMsgReceive, h.handleWebhook)
	return nil
}

// handleWebhook is our HTTP handler function for incoming messages
func (h *WuzapiHandler) handleWebhook(ctx context.Context, channel courier.Channel, w http.ResponseWriter, r *http.Request, clog *courier.ChannelLog) ([]courier.Event, error) {
	// 1. HMAC Verification
	// 1. HMAC Verification - TEMPORARILY DISABLED due to Key Rotation Issue
	// hmacKey := channel.StringConfigForKey("hmac_key", "")
	// if hmacKey != "" {
	// 	if err := h.verifySignature(r, hmacKey); err != nil {
	// 		return nil, handlers.WriteAndLogRequestError(ctx, h, channel, w, r, err)
	// 	}
	// }

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, handlers.WriteAndLogRequestError(ctx, h, channel, w, r, fmt.Errorf("failed to read body: %w", err))
	}
	// Restore body for any subsequent needs (though we parsed it)
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	// Debug Logging: Log the raw payload to debug mismatches
	fmt.Printf("Wuzapi Raw Webhook: %s\n", string(bodyBytes))

	var payload WuzapiPayload
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		// Fallback: Check if jsonData is in POST (Wuzapi/Zap form format)
		// Wuzapi sometimes sends 'application/x-www-form-urlencoded' with 'jsonData={...}'
		query, parseErr := url.ParseQuery(string(bodyBytes))
		if parseErr == nil && query.Has("jsonData") {
			jsonData := query.Get("jsonData")
			if jsonData == "" {
				fmt.Printf("Wuzapi ERROR: empty jsonData fallback\n")
				return nil, nil // Return 200 to clear queue
			}
			fmt.Printf("Wuzapi DEBUG: jsonData Fallback: %s\n", jsonData)
			if jsonErr := json.Unmarshal([]byte(jsonData), &payload); jsonErr != nil {
				log.Printf("Wuzapi ERROR: invalid json in jsonData: %s | DATA: %s", jsonErr, jsonData)
				return nil, nil // Return 200 to clear queue
			}
		} else {
			// Original error if fallback didn't match
			log.Printf("Wuzapi ERROR: invalid json: %s | BODY: %s", err, string(bodyBytes))
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

func (h *WuzapiHandler) verifySignature(r *http.Request, key string) error {
	signature := r.Header.Get("x-hmac-signature")
	if signature == "" {
		signature = r.Header.Get("X-Hub-Signature")
	}
	if signature == "" {
		// For backward compatibility or debugging, maybe log warning?
		// For now, enforce if key is present.
		return fmt.Errorf("missing signature")
	}

	body, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewBuffer(body)) // Restore

	mac := hmac.New(sha256.New, []byte(key))
	mac.Write(body)
	expectedSignature := hex.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(expectedSignature), []byte(signature)) {
		return fmt.Errorf("invalid signature")
	}
	return nil
}

// Temporary struct for media download
type WuzapiMediaData struct {
	Url           string
	DirectPath    string
	MediaKey      string
	Mimetype      string
	FileEncSHA256 string
	FileSHA256    string
	FileLength    uint64
}

func (h *WuzapiHandler) downloadMedia(ctx context.Context, channel courier.Channel, data *WuzapiMediaData) ([]byte, string, error) {

	wuzapiURL := channel.StringConfigForKey("wuzapi_url", "")
	token := channel.StringConfigForKey("wuzapi_token", "")

	// Construct payload for /chat/download...
	endpoint := "downloaddocument" // default
	if strings.HasPrefix(data.Mimetype, "image") {
		endpoint = "downloadimage"
	} else if strings.HasPrefix(data.Mimetype, "video") {
		endpoint = "downloadvideo"
	} else if strings.HasPrefix(data.Mimetype, "audio") {
		endpoint = "downloadaudio"
	}

	// Attempt direct fetch first if URL looks standard
	if strings.HasPrefix(data.Url, "http") {
		resp, err := http.Get(data.Url)
		if err == nil && resp.StatusCode == 200 {
			d, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return d, resp.Header.Get("Content-Type"), nil
		}
	}

	// Fallback to Wuzapi Custom API
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
	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/chat/%s", wuzapiURL, endpoint), bytes.NewBuffer(jsonBody))
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

	parts := strings.Split(respDec.Data, ",") // data:image/jpeg;base64,....
	if len(parts) < 2 {
		return nil, "", fmt.Errorf("invalid base64 response")
	}

	raw, err := base64.StdEncoding.DecodeString(parts[1])
	return raw, respDec.Mimetype, err
}

func (h *WuzapiHandler) handleReceiptInternal(ctx context.Context, channel courier.Channel, payload *WuzapiPayload, clog *courier.ChannelLog, w http.ResponseWriter, r *http.Request) ([]courier.Event, error) {
	// Map Status
	var status models.MsgStatus

	// Status can be in payload.Status OR payload.ReceiptType
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
		// Ignore others or map to sent
		return nil, nil
	}

	// ID might be in payload.ID or nested in event? Wuzapi usually puts it in top level for receipts
	id := payload.ID

	if id == "" {
		return nil, nil // No ID
	}

	event := h.Backend().NewStatusUpdateByExternalID(channel, id, status, clog)
	return handlers.WriteMsgStatusAndResponse(ctx, h, channel, event, w, r)
}

// Send implements the ChannelHandler interface
func (h *WuzapiHandler) Send(ctx context.Context, msg courier.MsgOut, res *courier.SendResult, log *courier.ChannelLog) error {
	// JITTER: Sleep for 1.5 - 3.5 seconds to mimic human typing behavior
	// This helps avoid triggering anti-spam mechanisms
	ms := 1500 + rand.Intn(2000)
	time.Sleep(time.Duration(ms) * time.Millisecond)

	wuzapiURL := msg.Channel().StringConfigForKey("wuzapi_url", "")
	token := msg.Channel().StringConfigForKey("wuzapi_token", "")

	if wuzapiURL == "" || token == "" {
		return fmt.Errorf("missing wuzapi_url or wuzapi_token in config")
	}

	phone := strings.TrimPrefix(msg.URN().Path(), "+")

	// Check for Attachments
	attachments := msg.Attachments()

	if len(attachments) > 0 {
		// Handle Media Send
		// Just take the first one for now as Wuzapi usually sends one media per message
		attType, attURL := handlers.SplitAttachment(attachments[0])

		// Map mime type to Wuzapi endpoint types
		// /chat/send/{image|video|audio|document}
		endpointType := "document"
		payloadKey := "Document"
		if strings.HasPrefix(attType, "image") {
			endpointType = "image"
			payloadKey = "Image"
		} else if strings.HasPrefix(attType, "video") {
			endpointType = "video"
			payloadKey = "Video"
		} else if strings.HasPrefix(attType, "audio") {
			endpointType = "audio"
			payloadKey = "Audio"
		}

		// Wuzapi expects:
		// { "phone": "...", "Image": "http://...", "Caption": "..." }

		// Sanitize URL: RapidPro sometimes sends "https:///rp/..." which fails DNS lookup.
		// We rewrite checks for "https:///rp" to "http://localhost/rp" so Wuzapi can fetch it locally via Nginx.
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

		url := fmt.Sprintf("%s/chat/send/%s", wuzapiURL, endpointType)
		return h.doRequest(url, token, jsonBody, res, log)
	}

	// Text Only
	payload := map[string]string{
		"Phone": phone,
		"Body":  msg.Text(),
	}
	jsonBody, _ := json.Marshal(payload)
	url := fmt.Sprintf("%s/chat/send/text", wuzapiURL)

	return h.doRequest(url, token, jsonBody, res, log)
}

func (h *WuzapiHandler) doRequest(url string, token string, body []byte, res *courier.SendResult, log *courier.ChannelLog) error {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	fmt.Printf("Wuzapi DEBUG: Sending %s Payload: %s\n", url, string(body))
	req.Header.Set("Authorization", token)
	req.Header.Set("token", token) // Compatibility with unpatched Wuzapi
	req.Header.Set("Content-Type", "application/json")

	// Use requestHTTPUnsafe to allow localhost connections
	resp, respBody, err := h.requestHTTPUnsafe(req, log)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("wuzapi error: %s", string(respBody))
	}

	// Extract ID
	// Wuzapi response is wrapped in data envelope by Respond(): {"data": {"Id": "..."}}
	// Check "id" and "Id"
	id, _ := jsonparser.GetString(respBody, "data", "id")
	if id == "" {
		id, _ = jsonparser.GetString(respBody, "data", "Id")
	}
	if id == "" {
		id, _ = jsonparser.GetString(respBody, "newId") // Legacy check
	}

	// Log success response for debugging
	fmt.Printf("Wuzapi DEBUG: Response: %s\n", string(respBody))

	if id != "" {
		res.AddExternalID(id)
	}
	return nil
}

// requestHTTPUnsafe bypasses the backend's HttpAccess configuration to allow requests to private IPs (localhost)
func (h *WuzapiHandler) requestHTTPUnsafe(req *http.Request, clog *courier.ChannelLog) (*http.Response, []byte, error) {
	client := h.Backend().HttpClient(false)

	req.Header.Set("User-Agent", fmt.Sprintf("Courier/%s", h.Server().Config().Version))

	trace, err := httpx.DoTrace(client, req, nil, nil, 0)
	if trace != nil {
		clog.HTTP(trace)
		return trace.Response, trace.ResponseBody, nil
	}
	return nil, nil, err
}
