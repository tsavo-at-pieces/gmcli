package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"

	"github.com/fdsouvenir/gmcli/internal/gm"
	"github.com/fdsouvenir/gmcli/internal/output"
	"github.com/fdsouvenir/gmcli/internal/store"
	gmsync "github.com/fdsouvenir/gmcli/internal/sync"
)

type historyBackfillResult struct {
	ConversationID       string `json:"conversation_id"`
	Requests             int    `json:"requests"`
	Count                int64  `json:"count"`
	FetchedMessages      int    `json:"fetched_messages"`
	SyncRecordsProcessed int    `json:"sync_records_processed"`
	MessagesBefore       int    `json:"messages_before"`
	MessagesAfter        int    `json:"messages_after"`
	MessagesAddedForChat int    `json:"messages_added_for_chat"`
}

func historyCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "history",
		Short: "Best-effort message history backfill",
		Long: "Fetch older messages for a conversation through the paired phone. " +
			"Like wacli, this is best-effort: Google may return partial history, " +
			"and the phone must be online.",
	}
	c.AddCommand(historyBackfillCmd())
	return c
}

func historyBackfillCmd() *cobra.Command {
	var chat, since string
	var requests int
	var count int64
	var fromLatest bool
	c := &cobra.Command{
		Use:   "backfill",
		Short: "Fetch older messages for one conversation",
		Long: "Fetch older messages for one conversation. --requests limits how many " +
			"FetchMessages calls gmcli makes, and --count limits how many message " +
			"records each call asks the phone for. By default paging starts at the " +
			"oldest stored message and walks further back; --from-latest starts at " +
			"the newest message instead, which fills gaps left by a sync outage, and " +
			"--since stops once a page reaches that time. JSON output separates " +
			"protocol records processed from messages added to the target conversation.",
		Args: noArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if chat == "" {
				return usageErrorf("--chat is required")
			}
			if requests <= 0 {
				requests = 10
			}
			if count <= 0 {
				count = 50
			}
			if since != "" && !fromLatest {
				return usageErrorf("--since requires --from-latest")
			}
			sinceTime, err := parseFlagTime(since)
			if err != nil {
				return usageErrorf("--since: %v", err)
			}
			var sinceMS int64
			if !sinceTime.IsZero() {
				sinceMS = sinceTime.UnixMilli()
			}
			res, err := runHistoryBackfill(chat, requests, count, fromLatest, sinceMS)
			if err != nil {
				return err
			}
			if flags.jsonOut {
				return output.JSON(os.Stdout, res)
			}
			fmt.Fprintf(os.Stderr, "Backfill for %s: fetched %d message record(s), chat messages %d -> %d (+%d), using %d request(s)\n",
				res.ConversationID, res.FetchedMessages, res.MessagesBefore, res.MessagesAfter, res.MessagesAddedForChat, res.Requests)
			return nil
		},
	}
	c.Flags().StringVar(&chat, "chat", "", "conversation_id to backfill")
	c.Flags().IntVar(&requests, "requests", 10, "max FetchMessages calls to make for the target conversation")
	c.Flags().Int64Var(&count, "count", 50, "max message records to request per FetchMessages call")
	c.Flags().BoolVar(&fromLatest, "from-latest", false, "page back from the newest message instead of the oldest stored one")
	c.Flags().StringVar(&since, "since", "", "with --from-latest, stop once a page reaches this time (YYYY-MM-DD or RFC3339)")
	return c
}

func runHistoryBackfill(chat string, requests int, count int64, fromLatest bool, sinceMS int64) (historyBackfillResult, error) {
	layout, err := resolveLayout()
	if err != nil {
		return historyBackfillResult{}, err
	}
	logger := newLogger()
	ctx, cancel := signalContext(context.Background())
	defer cancel()

	st, err := store.Open(ctx, layout.Database)
	if err != nil {
		return historyBackfillResult{}, fmt.Errorf("open store: %w", err)
	}
	defer st.Close()

	client, err := gm.Open(layout, logger)
	if err != nil {
		return historyBackfillResult{}, err
	}

	pump := gmsync.New(st, logger)
	client.Subscribe(pump.Handle)

	if err := client.Connect(); err != nil {
		return historyBackfillResult{}, fmt.Errorf("connect: %w", err)
	}
	defer client.Disconnect()

	if conv, err := client.Underlying().GetConversation(ctx, chat); err == nil && conv != nil {
		pump.Handle(conv)
	} else if _, localErr := st.GetConversation(ctx, chat); localErr != nil {
		if err != nil {
			return historyBackfillResult{}, fmt.Errorf("get conversation %s: %w", chat, err)
		}
		return historyBackfillResult{}, fmt.Errorf("conversation %s is not in the local store; run `gmcli sync` first", chat)
	}

	var cursor *gmproto.Cursor
	if !fromLatest {
		cursor, err = oldestCursor(ctx, st, chat)
		if err != nil {
			return historyBackfillResult{}, err
		}
	}

	before, err := st.CountMessagesForConversation(ctx, chat)
	if err != nil {
		return historyBackfillResult{}, fmt.Errorf("count messages before backfill: %w", err)
	}

	res := historyBackfillResult{ConversationID: chat, Count: count, MessagesBefore: before}
	for i := 0; i < requests; i++ {
		resp, err := client.Underlying().FetchMessages(ctx, chat, count, cursor)
		if err != nil {
			return res, fmt.Errorf("fetch messages: %w", err)
		}
		res.Requests++
		msgs := resp.GetMessages()
		res.FetchedMessages += len(msgs)
		imported := pump.ImportMessages(ctx, msgs)
		res.SyncRecordsProcessed += imported
		next := resp.GetCursor()
		if len(msgs) == 0 || sameCursor(cursor, next) || pageReachesSince(msgs, sinceMS) {
			break
		}
		cursor = next
	}
	after, err := st.CountMessagesForConversation(ctx, chat)
	if err != nil {
		return res, fmt.Errorf("count messages after backfill: %w", err)
	}
	res.MessagesAfter = after
	res.MessagesAddedForChat = after - before
	return res, nil
}

// pageReachesSince reports whether a FetchMessages page already contains a
// message at or before sinceMS, so paging further back is unnecessary.
func pageReachesSince(msgs []*gmproto.Message, sinceMS int64) bool {
	if sinceMS <= 0 {
		return false
	}
	for _, m := range msgs {
		if ts := gmsync.TimestampMS(m.GetTimestamp()); ts > 0 && ts <= sinceMS {
			return true
		}
	}
	return false
}

func oldestCursor(ctx context.Context, st *store.Store, chat string) (*gmproto.Cursor, error) {
	msgs, err := st.ListMessages(ctx, store.ListMessageOpts{
		ConversationID: chat,
		Limit:          1,
		Order:          "asc",
	})
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	return &gmproto.Cursor{
		LastItemID:        msgs[0].ID,
		LastItemTimestamp: msgs[0].TimestampMS,
	}, nil
}

func sameCursor(a, b *gmproto.Cursor) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.GetLastItemID() == b.GetLastItemID() &&
		a.GetLastItemTimestamp() == b.GetLastItemTimestamp()
}
