// Package gm wraps go.mau.fi/mautrix-gmessages/pkg/libgm with the conventions
// gmcli needs: filesystem-backed AuthData persistence, an event subscriber
// model on top of libgm's single SetEventHandler, and helpers for the QR
// pairing flow.
//
// Two entry points cover the lifecycle:
//
//	Pair(ctx, layout, render)         // first run: produces session.json
//	Open(layout, logger) -> *Client   // subsequent runs: ready to Connect()
//
// The wrapper does not own a goroutine of its own; libgm runs the long-poll.
// Subscribers must not block in their handlers.
package gm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"go.mau.fi/mautrix-gmessages/pkg/libgm"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"

	"github.com/fdsouvenir/gmcli/internal/paths"
)

// PairTimeout is the upper bound on how long we wait for a phone to scan the
// QR code. Google's relay drops unfinished pairings after a few minutes.
const PairTimeout = 5 * time.Minute

// sendMetadataTimeout bounds how long a write waits for the phone settings
// event that carries SIM metadata. mautrix/gmessages includes this metadata
// in send requests; sending without it can be accepted but not actually sent
// on some devices.
const sendMetadataTimeout = 10 * time.Second

// EventHandler is invoked for each event delivered by libgm. The argument
// type is one of the concrete types in pkg/libgm/events or pkg/libgm/gmproto.
type EventHandler func(evt any)

// Client is a thin wrapper around *libgm.Client adding fan-out event
// subscription and persistence on AuthTokenRefreshed.
type Client struct {
	libgm  *libgm.Client
	auth   *libgm.AuthData
	layout paths.Layout
	logger zerolog.Logger

	mu          sync.RWMutex
	subscribers []EventHandler
	settings    *gmproto.Settings
}

// Open loads session.json and returns a connected-but-not-yet-Connect()'d
// Client. Returns an error if no session exists — the caller must run Pair
// first.
func Open(layout paths.Layout, logger zerolog.Logger) (*Client, error) {
	auth, err := loadAuth(layout.Session)
	if err != nil {
		return nil, err
	}
	if auth.Browser == nil {
		return nil, fmt.Errorf("session %s has no paired device; run `gmcli auth` first", layout.Session)
	}
	c := &Client{
		auth:   auth,
		layout: layout,
		logger: logger,
	}
	c.libgm = libgm.NewClient(auth, nil, logger)
	c.libgm.SetEventHandler(c.dispatch)
	return c, nil
}

// Subscribe registers a handler. Multiple subscribers receive each event in
// the order they were registered. Handlers must not block.
func (c *Client) Subscribe(h EventHandler) {
	c.mu.Lock()
	c.subscribers = append(c.subscribers, h)
	c.mu.Unlock()
}

// Connect opens the long-poll connection. Events flow to subscribers
// immediately. Returns when the initial sync completes; the connection
// continues running in a background goroutine inside libgm.
func (c *Client) Connect() error {
	return c.libgm.Connect()
}

// Disconnect closes the long-poll. Safe to call multiple times.
func (c *Client) Disconnect() {
	c.libgm.Disconnect()
}

// IsConnected reports whether the long-poll is currently active.
func (c *Client) IsConnected() bool {
	return c.libgm.IsConnected()
}

// WaitForReady blocks until the libgm client emits *events.ClientReady or
// the context is cancelled. SendMessage and SendReaction need an established
// session before they can round-trip a response; ClientReady is the earliest
// signal that the session is up. The handler is removed before returning.
//
// Subscribe(c.WaitForReady...) is not the right idiom — this method
// installs and removes a single-fire subscriber for you.
func (c *Client) WaitForReady(ctx context.Context) error {
	if c.libgm.IsConnected() {
		// Already ready — but ClientReady has likely already fired and
		// won't repeat. Best effort: return immediately.
		return nil
	}
	ready := make(chan struct{}, 1)
	var fired sync.Once

	c.mu.Lock()
	idx := len(c.subscribers)
	c.subscribers = append(c.subscribers, func(evt any) {
		if _, ok := evt.(*events.ClientReady); ok {
			fired.Do(func() { close(ready) })
		}
	})
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		// Remove only our subscriber; preserve any added concurrently.
		if idx < len(c.subscribers) {
			c.subscribers = append(c.subscribers[:idx], c.subscribers[idx+1:]...)
		}
		c.mu.Unlock()
	}()

	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Underlying returns the wrapped *libgm.Client for callers that need access
// to libgm methods we haven't surfaced yet (ListContacts, FetchMessages,
// etc.). Higher-level operations should prefer the typed wrappers below.
func (c *Client) Underlying() *libgm.Client { return c.libgm }

// SendTextResult describes a successful send.
type SendTextResult struct {
	MessageID      string
	ConversationID string
	TmpID          string
}

// SendText sends a text message into the given conversation. ReplyToID is
// optional; when set, the new message is rendered as a quoted reply by the
// recipient's client. The libgm long-poll must be Connected; call
// WaitForReady first for fresh sessions.
func (c *Client) SendText(ctx context.Context, conversationID, body, replyToID string) (*SendTextResult, error) {
	if conversationID == "" {
		return nil, fmt.Errorf("conversation id is required")
	}
	if body == "" {
		return nil, fmt.Errorf("message body is required")
	}
	settingsCtx, cancel := context.WithTimeout(ctx, sendMetadataTimeout)
	defer cancel()
	if err := c.WaitForSettings(settingsCtx); err != nil {
		return nil, fmt.Errorf("wait for phone send settings: %w", err)
	}
	tmpID := uuid.NewString()
	req, err := c.buildSendTextRequest(conversationID, body, replyToID, tmpID)
	if err != nil {
		return nil, err
	}
	waitEcho, unsubscribe := c.watchMessageEcho(tmpID)
	defer unsubscribe()

	resp, err := c.libgm.SendMessage(req)
	if err != nil {
		return nil, fmt.Errorf("libgm send: %w", err)
	}
	if resp.GetStatus() != gmproto.SendMessageResponse_SUCCESS {
		return nil, fmt.Errorf("send rejected by phone: %s", sendStatusMessage(resp))
	}

	echo, err := waitEcho(ctx)
	if err != nil {
		return nil, fmt.Errorf("send accepted by phone, but no sent-message echo arrived for tmp_id %s: %w", tmpID, err)
	}
	return &SendTextResult{
		MessageID:      echo.Message.GetMessageID(),
		ConversationID: echo.Message.GetConversationID(),
		TmpID:          tmpID,
	}, nil
}

func (c *Client) buildSendTextRequest(conversationID, body, replyToID, tmpID string) (*gmproto.SendMessageRequest, error) {
	conv, err := c.libgm.GetConversation(conversationID)
	if err != nil {
		return nil, fmt.Errorf("get conversation %s before send: %w", conversationID, err)
	}
	outgoingID := conv.GetDefaultOutgoingID()
	if outgoingID == "" {
		return nil, fmt.Errorf("conversation %s has no default outgoing participant; cannot choose sender/SIM", conversationID)
	}
	req := &gmproto.SendMessageRequest{
		ConversationID: conversationID,
		TmpID:          tmpID,
		MessagePayload: &gmproto.MessagePayload{
			ConversationID: conversationID,
			ParticipantID:  outgoingID,
			TmpID:          tmpID,
			TmpID2:         tmpID,
			MessageInfo: []*gmproto.MessageInfo{{
				Data: &gmproto.MessageInfo_MessageContent{
					MessageContent: &gmproto.MessageContent{Content: body},
				},
			}},
		},
	}
	if sim := c.simForParticipant(outgoingID); sim != nil {
		req.SIMPayload = sim.GetSIMData().GetSIMPayload()
	} else {
		return nil, fmt.Errorf("conversation %s uses outgoing participant %s, but no matching SIM metadata was received", conversationID, outgoingID)
	}
	if replyToID != "" {
		req.Reply = &gmproto.ReplyPayload{MessageID: replyToID}
	}
	return req, nil
}

// WaitForSettings blocks until libgm emits the phone settings event. Send
// requests need its SIM metadata to match the browser client shape.
func (c *Client) WaitForSettings(ctx context.Context) error {
	c.mu.RLock()
	hasSettings := c.settings != nil
	c.mu.RUnlock()
	if hasSettings {
		return nil
	}

	ready := make(chan struct{}, 1)
	var fired sync.Once
	c.mu.Lock()
	idx := len(c.subscribers)
	c.subscribers = append(c.subscribers, func(evt any) {
		if _, ok := evt.(*gmproto.Settings); ok {
			fired.Do(func() { close(ready) })
		}
	})
	if c.settings != nil {
		fired.Do(func() { close(ready) })
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if idx < len(c.subscribers) {
			c.subscribers = append(c.subscribers[:idx], c.subscribers[idx+1:]...)
		}
		c.mu.Unlock()
	}()

	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) simForParticipant(participantID string) *gmproto.SIMCard {
	c.mu.RLock()
	settings := c.settings
	c.mu.RUnlock()
	if settings == nil {
		return nil
	}
	for _, sim := range settings.GetSIMCards() {
		if sim.GetSIMParticipant().GetID() == participantID {
			return sim
		}
	}
	return nil
}

func (c *Client) watchMessageEcho(tmpID string) (func(context.Context) (*libgm.WrappedMessage, error), func()) {
	echo := make(chan *libgm.WrappedMessage, 1)
	c.mu.Lock()
	idx := len(c.subscribers)
	c.subscribers = append(c.subscribers, func(evt any) {
		w, ok := evt.(*libgm.WrappedMessage)
		if !ok || w.Message.GetTmpID() != tmpID {
			return
		}
		select {
		case echo <- w:
		default:
		}
	})
	c.mu.Unlock()

	unsubscribe := func() {
		c.mu.Lock()
		if idx < len(c.subscribers) {
			c.subscribers = append(c.subscribers[:idx], c.subscribers[idx+1:]...)
		}
		c.mu.Unlock()
	}
	wait := func(ctx context.Context) (*libgm.WrappedMessage, error) {
		select {
		case w := <-echo:
			return w, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return wait, unsubscribe
}

func sendStatusMessage(resp *gmproto.SendMessageResponse) string {
	switch resp.GetStatus() {
	case gmproto.SendMessageResponse_UNKNOWN:
		if resp.GetGoogleAccountSwitch() != nil {
			return "switch back to QR pairing or log in with Google account to send messages"
		}
		return "unknown status"
	case gmproto.SendMessageResponse_FAILURE_2:
		return "unknown permanent error"
	case gmproto.SendMessageResponse_FAILURE_3:
		return "unknown temporary error"
	case gmproto.SendMessageResponse_FAILURE_4:
		return "Google Messages is not your default SMS app"
	default:
		return resp.GetStatus().String()
	}
}

// ReactionAction selects ADD / REMOVE / SWITCH semantics on SendReaction.
type ReactionAction int

const (
	ReactionAdd ReactionAction = iota
	ReactionRemove
	ReactionSwitch
)

// SendReaction adds, removes, or switches a unicode reaction on a message.
func (c *Client) SendReaction(messageID, emoji string, action ReactionAction) error {
	if messageID == "" {
		return fmt.Errorf("message id is required")
	}
	if emoji == "" {
		return fmt.Errorf("emoji is required")
	}
	var act gmproto.SendReactionRequest_Action
	switch action {
	case ReactionAdd:
		act = gmproto.SendReactionRequest_ADD
	case ReactionRemove:
		act = gmproto.SendReactionRequest_REMOVE
	case ReactionSwitch:
		act = gmproto.SendReactionRequest_SWITCH
	default:
		return fmt.Errorf("unknown reaction action %v", action)
	}
	_, err := c.libgm.SendReaction(&gmproto.SendReactionRequest{
		MessageID:    messageID,
		Action:       act,
		ReactionData: &gmproto.ReactionData{Unicode: emoji},
	})
	if err != nil {
		return fmt.Errorf("libgm reaction: %w", err)
	}
	return nil
}

// DownloadMedia retrieves and decrypts the bytes for an attachment.
// The connection does not need to be in long-poll mode — DownloadMedia
// uses authenticated HTTP — but the AuthData's TachyonAuthToken must be
// fresh. Call Connect once before this if the session has been idle.
func (c *Client) DownloadMedia(mediaID string, key []byte) ([]byte, error) {
	if mediaID == "" {
		return nil, fmt.Errorf("media id is required")
	}
	return c.libgm.DownloadMedia(mediaID, key)
}

// ConversationStatus mirrors gmproto.ConversationStatus so the CLI surface
// doesn't have to import gmproto directly. The values match the wire enum
// exactly; UpdateConversationStatus enforces that at the boundary.
type ConversationStatus int

const (
	// ConversationActive restores a conversation to the inbox folder.
	ConversationActive ConversationStatus = 1
	// ConversationArchived moves a conversation to the archive folder. The
	// underlying messages and contacts are preserved; the conversation
	// simply hides from the default inbox view. Reversible via
	// ConversationActive.
	ConversationArchived ConversationStatus = 2
	// ConversationDeleted removes the conversation from the phone (and
	// therefore from gmcli's local archive on next sync). Irreversible
	// from gmcli's side.
	ConversationDeleted ConversationStatus = 3
	// ConversationSpamFolder moves the conversation to the spam folder.
	ConversationSpamFolder ConversationStatus = 5
	// ConversationBlockedFolder moves the conversation to the blocked
	// folder and implicitly blocks the sender on the phone.
	ConversationBlockedFolder ConversationStatus = 6
)

// String returns the human-readable name for a ConversationStatus. Useful
// for CLI/JSON output where we want stable strings rather than the
// numeric enum values.
func (s ConversationStatus) String() string {
	switch s {
	case ConversationActive:
		return "active"
	case ConversationArchived:
		return "archived"
	case ConversationDeleted:
		return "deleted"
	case ConversationSpamFolder:
		return "spam"
	case ConversationBlockedFolder:
		return "blocked"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// UpdateConversationStatus archives, restores, deletes, or moves a
// conversation to spam/blocked. Delete uses a separate protocol path
// because the relay requires the participant phone number in a
// dedicated DeleteConversationData payload — the status-update path
// that archive/unarchive/spam use is silently rejected for delete.
//
// The change is round-tripped to the phone via libgm; the local SQLite
// store is updated by the sync pump as the resulting Conversation event
// flows back through dispatch.
//
// The libgm long-poll must be Connected; the runWithConnectedClient
// helper in cmd/ ensures this. Requires the writer/--read-only=false
// gate on the CLI side.
//
// IMPORTANT: libgm's UpdateConversationResponse carries a Success bool
// that this method checks. Both the status-update path and the delete
// path inspect resp.GetSuccess() and return a descriptive error when
// the phone rejects the request.
func (c *Client) UpdateConversationStatus(conversationID string, status ConversationStatus) error {
	if conversationID == "" {
		return fmt.Errorf("conversation id is required")
	}

	if status == ConversationDeleted {
		phone, err := c.lookupParticipantPhone(conversationID)
		if err != nil {
			return fmt.Errorf("delete %s: %w", conversationID, err)
		}
		req := &gmproto.UpdateConversationRequest{
			Action:         gmproto.ConversationActionStatus_DELETE,
			ConversationID: conversationID,
			Data: &gmproto.UpdateConversationRequest_DeleteData{
			DeleteData: &gmproto.DeleteConversationData{
				ConversationID: conversationID,
				Phone:          phone,
			},
			},
		}
		resp, err := c.libgm.UpdateConversation(req)
		if err != nil {
			return fmt.Errorf("libgm delete conversation %s (phone=%q): %w", conversationID, phone, err)
		}
		if !resp.GetSuccess() {
			return fmt.Errorf("phone rejected delete of conversation %s (phone=%q); response.success=false", conversationID, phone)
		}
		return nil
	}

	// Status-update path for archive / unarchive / spam / blocked.
	// The current proto wraps UpdateConversationData inside an
	// UpdateConversationRequest_UpdateData oneof variant.
	req := &gmproto.UpdateConversationRequest{
		ConversationID: conversationID,
		Data: &gmproto.UpdateConversationRequest_UpdateData{
			UpdateData: &gmproto.UpdateConversationData{
				ConversationID: conversationID,
				Data: &gmproto.UpdateConversationData_Status{
					Status: gmproto.ConversationStatus(status),
				},
			},
		},
	}
	resp, err := c.libgm.UpdateConversation(req)
	if err != nil {
		return fmt.Errorf("libgm update conversation %s -> %s: %w", conversationID, status, err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("phone rejected update of conversation %s to status %s (response.success=false)", conversationID, status)
	}
	return nil
}

// lookupParticipantPhone fetches the conversation from the relay and
// returns the first visible non-me participant's phone number (E.164 or
// shortcode). Used for delete operations that require a phone to scope
// the relay-side cleanup. Returns an empty string with no error if no
// participant has a phone — the relay treats empty phone as best-effort.
func (c *Client) lookupParticipantPhone(conversationID string) (string, error) {
	conv, err := c.libgm.GetConversation(conversationID)
	if err != nil {
		return "", fmt.Errorf("get conversation for phone lookup: %w", err)
	}
	for _, p := range conv.GetParticipants() {
		if p.GetIsMe() {
			continue
		}
		if !p.GetIsVisible() {
			continue
		}
		if num := p.GetID().GetNumber(); num != "" {
			return num, nil
		}
	}
	// No phone-bearing participant found. The proto says phone is
	// optional; return empty and let the relay decide.
	return "", nil
}

// ConversationFolder mirrors gmproto.ListConversationsRequest_Folder so
// CLI callers don't need to import gmproto.
type ConversationFolder int

const (
	// FolderInbox is the default visible inbox (status=ACTIVE).
	FolderInbox ConversationFolder = 1
	// FolderArchive lists conversations the user has archived (status=ARCHIVED).
	FolderArchive ConversationFolder = 2
	// FolderSpamBlocked lists conversations in the spam or blocked folders.
	FolderSpamBlocked ConversationFolder = 5
)

// String returns the human-readable name for a ConversationFolder.
func (f ConversationFolder) String() string {
	switch f {
	case FolderInbox:
		return "inbox"
	case FolderArchive:
		return "archive"
	case FolderSpamBlocked:
		return "spam_blocked"
	default:
		return fmt.Sprintf("unknown(%d)", int(f))
	}
}

// ListConversationsFromFolder pulls a page of conversations from one of
// the phone-side folders (inbox / archive / spam-blocked). Unlike the
// store-backed `chats list` command, this reads live from the phone via
// libgm — useful for inspecting folders that are not synced into the
// local archive (archive and spam are not pulled by default).
func (c *Client) ListConversationsFromFolder(folder ConversationFolder, count int) (*gmproto.ListConversationsResponse, error) {
	if count <= 0 {
		count = 50
	}
	resp, err := c.libgm.ListConversations(count, gmproto.ListConversationsRequest_Folder(folder))
	if err != nil {
		return nil, fmt.Errorf("libgm list conversations folder=%s: %w", folder, err)
	}
	return resp, nil
}

// AuthSnapshot returns a deep copy of the current AuthData by JSON
// round-trip. Useful for diagnostics; do not modify.
func (c *Client) AuthSnapshot() (*libgm.AuthData, error) {
	b, err := json.Marshal(c.auth)
	if err != nil {
		return nil, err
	}
	var out libgm.AuthData
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// dispatch is the single libgm callback. It persists on token refresh and
// then fans out to subscribers.
func (c *Client) dispatch(evt any) {
	switch e := evt.(type) {
	case *events.AuthTokenRefreshed, *events.PairSuccessful:
		if err := saveAuth(c.layout.Session, c.auth); err != nil {
			c.logger.Error().Err(err).Msg("Failed to persist refreshed auth data")
		}
	case *gmproto.Settings:
		c.mu.Lock()
		c.settings = e
		c.mu.Unlock()
	}
	c.mu.RLock()
	subs := append([]EventHandler(nil), c.subscribers...)
	c.mu.RUnlock()
	for _, h := range subs {
		h(evt)
	}
}

// PairResult is returned by Pair on success. PhoneID identifies the paired
// device; SessionPath is where the persisted AuthData lives.
type PairResult struct {
	PhoneID     string
	SessionPath string
}

// QRRenderer is invoked once Pair has the QR URL ready. The implementation
// is responsible for displaying it (terminal QR, plain URL, etc.).
type QRRenderer func(qrURL string)

// Pair runs the QR pairing flow. It writes session.json on success and
// returns the paired phone ID. Cancellable via ctx; otherwise bounded by
// PairTimeout. Existing session.json (if any) is overwritten on success.
func Pair(ctx context.Context, layout paths.Layout, logger zerolog.Logger, render QRRenderer) (*PairResult, error) {
	if err := layout.EnsureDirs(); err != nil {
		return nil, err
	}
	auth := libgm.NewAuthData()
	cli := libgm.NewClient(auth, nil, logger)

	done := make(chan *events.PairSuccessful, 1)
	fatal := make(chan error, 1)
	cli.SetEventHandler(func(evt any) {
		switch e := evt.(type) {
		case *events.PairSuccessful:
			select {
			case done <- e:
			default:
			}
		case *events.ListenFatalError:
			select {
			case fatal <- fmt.Errorf("pairing transport failed: %w", e.Error):
			default:
			}
		}
	})

	qr, err := cli.StartLogin()
	if err != nil {
		return nil, fmt.Errorf("start login: %w", err)
	}
	render(qr)

	timeout := time.NewTimer(PairTimeout)
	defer timeout.Stop()

	select {
	case <-ctx.Done():
		cli.Disconnect()
		return nil, ctx.Err()
	case err := <-fatal:
		cli.Disconnect()
		return nil, err
	case <-timeout.C:
		cli.Disconnect()
		return nil, errors.New("pairing timed out — phone never scanned the QR code")
	case res := <-done:
		// libgm reconnects internally 2s after PairSuccessful; we're not
		// going to keep this client around, so close down cleanly.
		cli.Disconnect()
		if err := saveAuth(layout.Session, auth); err != nil {
			return nil, fmt.Errorf("persist session: %w", err)
		}
		return &PairResult{PhoneID: res.PhoneID, SessionPath: layout.Session}, nil
	}
}

func loadAuth(path string) (*libgm.AuthData, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("no session at %s; run `gmcli auth` first", path)
		}
		return nil, fmt.Errorf("open session %s: %w", path, err)
	}
	defer f.Close()
	var auth libgm.AuthData
	if err := json.NewDecoder(f).Decode(&auth); err != nil {
		return nil, fmt.Errorf("decode session %s: %w", path, err)
	}
	return &auth, nil
}

func saveAuth(path string, auth *libgm.AuthData) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(auth); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("encode session: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmp, path, err)
	}
	return nil
}
