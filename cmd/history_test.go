package cmd

import (
	"encoding/json"
	"strings"
	"testing"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func TestHistoryBackfillResultJSONIsUnambiguous(t *testing.T) {
	res := historyBackfillResult{
		ConversationID:       "198",
		Requests:             2,
		Count:                100,
		FetchedMessages:      150,
		SyncRecordsProcessed: 150,
		MessagesBefore:       301,
		MessagesAfter:        325,
		MessagesAddedForChat: 24,
	}

	raw, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := string(raw)
	for _, want := range []string{
		`"conversation_id":"198"`,
		`"fetched_messages":150`,
		`"sync_records_processed":150`,
		`"messages_before":301`,
		`"messages_after":325`,
		`"messages_added_for_chat":24`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("json missing %s: %s", want, out)
		}
	}
	if strings.Contains(out, `"imported"`) {
		t.Fatalf("json should not expose ambiguous imported field: %s", out)
	}
}

func TestPageReachesSince(t *testing.T) {
	since := int64(1_789_567_795_000) // 2026-09-16T10:09:55-04:00
	newer := &gmproto.Message{Timestamp: (since + 60_000) * 1000}
	older := &gmproto.Message{Timestamp: (since - 60_000) * 1000}
	if pageReachesSince([]*gmproto.Message{newer}, since) {
		t.Fatal("page of newer messages should not reach since")
	}
	if !pageReachesSince([]*gmproto.Message{newer, older}, since) {
		t.Fatal("page containing an older message should reach since")
	}
	if pageReachesSince([]*gmproto.Message{older}, 0) {
		t.Fatal("zero since should never stop paging")
	}
}
