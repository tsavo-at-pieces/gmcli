package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/fdsouvenir/gmcli/internal/gm"
	"github.com/fdsouvenir/gmcli/internal/output"
	"github.com/fdsouvenir/gmcli/internal/store"
)

func chatsCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "chats",
		Short: "List, inspect, and manage conversations (archive / unarchive / delete)",
	}
	c.AddCommand(chatsListCmd())
	c.AddCommand(chatsShowCmd())
	c.AddCommand(chatsArchiveCmd())
	c.AddCommand(chatsUnarchiveCmd())
	c.AddCommand(chatsDeleteCmd())
	c.AddCommand(chatsSpamCmd())
	c.AddCommand(chatsRemoteListCmd())
	return c
}

func chatsListCmd() *cobra.Command {
	var (
		limit      int
		unreadOnly bool
		pinned     bool
	)
	c := &cobra.Command{
		Use:   "list",
		Short: "List conversations, most recently active first",
		Example: "  gmcli chats list --limit 20\n" +
			"  gmcli --json chats list --unread-only",
		Args: noArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			st, err := openStore()
			if err != nil {
				return err
			}
			defer st.Close()
			convs, err := st.ListConversations(context.Background(), store.ListConversationOpts{
				Limit:      limit,
				UnreadOnly: unreadOnly,
				Pinned:     pinned,
			})
			if err != nil {
				return err
			}
			if flags.jsonOut {
				return output.JSON(os.Stdout, convs)
			}
			if len(convs) == 0 {
				fmt.Fprintln(os.Stdout, "conversations: 0 found")
				return nil
			}
			rows := make([][]string, 0, len(convs))
			for _, c := range convs {
				kind := "1:1"
				if c.IsGroup {
					kind = "grp"
				}
				rows = append(rows, []string{
					output.FormatTime(c.LastMessageTimeMS),
					kind,
					boolMark(c.Unread, "*"),
					boolMark(c.Pinned, "P"),
					participantSummary(c.ParticipantsJSON),
					truncate(c.Name, 40),
					c.ID,
				})
			}
			return output.Table(os.Stdout,
				[]string{"last_msg", "kind", "unread", "pin", "participants", "name", "conv_id"},
				rows)
		},
	}
	c.Flags().IntVar(&limit, "limit", 50, "max rows")
	c.Flags().BoolVar(&unreadOnly, "unread-only", false, "only conversations with unread messages")
	c.Flags().BoolVar(&pinned, "pinned", false, "only pinned conversations")
	return c
}

func chatsShowCmd() *cobra.Command {
	var limit int
	c := &cobra.Command{
		Use:   "show <conversation-id>",
		Short: "Show a conversation header and its most recent messages",
		Example: "  gmcli chats show <conversation-id>\n" +
			"  gmcli --full chats show <conversation-id> --limit 100",
		Args: exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			st, err := openStore()
			if err != nil {
				return err
			}
			defer st.Close()
			ctx := context.Background()
			conv, err := st.GetConversation(ctx, args[0])
			if err != nil {
				if errors.Is(err, store.ErrNotFound) || err.Error() == "sql: no rows in result set" {
					return fmt.Errorf("no conversation with id %s", args[0])
				}
				return err
			}
			msgs, err := st.ListMessages(ctx, store.ListMessageOpts{
				ConversationID: args[0],
				Limit:          limit,
				Order:          "desc",
			})
			if err != nil {
				return err
			}
			reverseMessages(msgs)
			if flags.jsonOut {
				return output.JSON(os.Stdout, struct {
					Conversation store.Conversation `json:"conversation"`
					Messages     []store.Message    `json:"messages"`
				}{conv, msgs})
			}
			renderChatDetail(conv, msgs)
			return nil
		},
	}
	c.Flags().IntVar(&limit, "limit", 50, "max recent messages to display")
	return c
}

func reverseMessages(msgs []store.Message) {
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}
}

func renderChatDetail(c store.Conversation, msgs []store.Message) {
	fmt.Printf("conversation_id: %s\n", c.ID)
	fmt.Printf("name:            %s\n", c.Name)
	fmt.Printf("group:           %v\n", c.IsGroup)
	fmt.Printf("unread:          %v\n", c.Unread)
	fmt.Printf("pinned:          %v\n", c.Pinned)
	fmt.Printf("last_message:    %s\n", output.FormatTime(c.LastMessageTimeMS))
	fmt.Printf("participants:    %s\n", participantSummary(c.ParticipantsJSON))
	fmt.Println()
	if len(msgs) == 0 {
		fmt.Fprintf(os.Stdout, "messages: 0 found in conversation %s\n", c.ID)
		return
	}
	_ = renderMessages(msgs)
}

// boolMark returns mark when b is true, else empty.
func boolMark(b bool, mark string) string {
	if b {
		return mark
	}
	return ""
}

// chatsArchiveCmd moves one or more conversations to the archive folder
// on the phone. Reversible via `gmcli chats unarchive`. Requires
// --read-only=false. The local SQLite store reflects the new status on
// the next sync event for each affected conversation.
func chatsArchiveCmd() *cobra.Command {
	return chatsStatusCmd(chatsStatusOpts{
		use:       "archive <conversation-id>...",
		short:     "Move conversations to the archive folder on the phone (reversible)",
		status:    gm.ConversationArchived,
		verb:      "archived",
		batchVerb: "archive",
	})
}

// chatsUnarchiveCmd restores one or more conversations from the archive
// folder back to the inbox. Requires --read-only=false.
func chatsUnarchiveCmd() *cobra.Command {
	return chatsStatusCmd(chatsStatusOpts{
		use:       "unarchive <conversation-id>...",
		short:     "Restore conversations from the archive folder back to the inbox",
		status:    gm.ConversationActive,
		verb:      "unarchived",
		batchVerb: "unarchive",
	})
}

// chatsDeleteCmd deletes one or more conversations on the phone. This is
// irreversible from gmcli's side — once the phone confirms the deletion,
// the conversation is also removed from the local archive on next sync.
// Requires --read-only=false plus an additional --yes confirmation to
// reduce the blast radius of typos.
func chatsDeleteCmd() *cobra.Command {
	c := chatsStatusCmd(chatsStatusOpts{
		use:       "delete <conversation-id>...",
		short:     "Delete conversations on the phone (irreversible; requires --yes)",
		status:    gm.ConversationDeleted,
		verb:      "deleted",
		batchVerb: "delete",
		requireYes: true,
	})
	return c
}

// chatsSpamCmd moves conversations into the spam folder on the phone.
// Useful as a milder alternative to delete: the conversation is hidden
// from the inbox but kept around in case it was mis-classified.
func chatsSpamCmd() *cobra.Command {
	return chatsStatusCmd(chatsStatusOpts{
		use:       "spam <conversation-id>...",
		short:     "Move conversations to the spam folder on the phone",
		status:    gm.ConversationSpamFolder,
		verb:      "marked as spam",
		batchVerb: "mark-as-spam",
	})
}

// chatsStatusOpts parameterises chatsStatusCmd so archive/unarchive/
// delete/spam can share a single implementation. The differences are
// strictly UX (use-line, short help, verb in success output) plus the
// underlying gm.ConversationStatus we send to libgm.
type chatsStatusOpts struct {
	use        string
	short      string
	status     gm.ConversationStatus
	verb       string
	batchVerb  string
	requireYes bool
}

// chatsStatusCmd is the shared implementation for archive/unarchive/
// delete/spam. Each accepts one or more conversation IDs as positional
// args, requires --read-only=false, and (for delete) requires --yes.
// Failures on individual conversations do not abort the batch; the
// command returns a non-zero exit only if at least one update failed.
func chatsStatusCmd(opts chatsStatusOpts) *cobra.Command {
	var yes bool
	c := &cobra.Command{
		Use:   opts.use,
		Short: opts.short,
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireWritable(); err != nil {
				return err
			}
			if opts.requireYes && !yes {
				return fmt.Errorf("%s is irreversible; re-run with --yes to confirm", opts.batchVerb)
			}
			return runWithConnectedClient(func(ctx context.Context, client *gm.Client, _ *store.Store) error {
				type result struct {
					ConversationID string `json:"conversation_id"`
					Status         string `json:"status"`
					Updated        bool   `json:"updated"`
					Error          string `json:"error,omitempty"`
				}
				results := make([]result, 0, len(args))
				var firstErr error
				for _, convID := range args {
					convID = strings.TrimSpace(convID)
					if convID == "" {
						continue
					}
					err := client.UpdateConversationStatus(convID, opts.status)
					r := result{
						ConversationID: convID,
						Status:         opts.status.String(),
						Updated:        err == nil,
					}
					if err != nil {
						r.Error = err.Error()
						if firstErr == nil {
							firstErr = err
						}
					}
					results = append(results, r)
				}
				if flags.jsonOut {
					if err := output.JSON(os.Stdout, results); err != nil {
						return err
					}
				} else {
					for _, r := range results {
						if r.Updated {
							fmt.Fprintf(os.Stderr, "%s conversation %s (status=%s)\n", opts.verb, r.ConversationID, r.Status)
						} else {
							fmt.Fprintf(os.Stderr, "FAILED to %s conversation %s: %s\n", opts.batchVerb, r.ConversationID, r.Error)
						}
					}
				}
				return firstErr
			})
		},
	}
	if opts.requireYes {
		c.Flags().BoolVar(&yes, "yes", false, "confirm the irreversible operation (required for delete)")
	}
	return c
}

// chatsRemoteListCmd lists conversations LIVE from the phone (rather than
// from the local SQLite store), filtered by folder. Use this to inspect
// archived or spam-folder conversations, which are not synced into the
// local archive by default and therefore don't show up in
// `gmcli chats list`. Read-only — does not require --read-only=false.
func chatsRemoteListCmd() *cobra.Command {
	var folderName string
	var count int
	c := &cobra.Command{
		Use:   "remote-list",
		Short: "List conversations LIVE from the phone, filtered by folder (inbox / archive / spam)",
		Long: "Pulls conversation metadata directly from the phone via libgm, " +
			"bypassing the local SQLite archive. Useful for inspecting the " +
			"archive or spam folders, which are not synced into the local " +
			"store by default. Use `--folder archive` to find conv_ids you " +
			"can then `unarchive`.",
		RunE: func(cmd *cobra.Command, args []string) error {
			folder, err := parseFolder(folderName)
			if err != nil {
				return err
			}
			return runWithConnectedClient(func(ctx context.Context, client *gm.Client, _ *store.Store) error {
				resp, err := client.ListConversationsFromFolder(folder, count)
				if err != nil {
					return err
				}
				convs := resp.GetConversations()
				if flags.jsonOut {
					return output.JSON(os.Stdout, convs)
				}
				if len(convs) == 0 {
					fmt.Fprintf(os.Stderr, "(no conversations in folder %s)\n", folder)
					return nil
				}
				rows := make([][]string, 0, len(convs))
				for _, conv := range convs {
					kind := "1:1"
					if conv.GetType() == 2 { // GROUP per gmproto.ConversationType
						kind = "grp"
					}
					rows = append(rows, []string{
						conv.GetConversationID(),
						kind,
						truncate(conv.GetName(), 40),
						fmt.Sprintf("%d", conv.GetLastMessageTimestamp()),
					})
				}
				return output.Table(os.Stdout,
					[]string{"conv_id", "kind", "name", "last_msg_ts_ms"},
					rows)
			})
		},
	}
	c.Flags().StringVar(&folderName, "folder", "archive", "folder to list: inbox, archive, or spam")
	c.Flags().IntVar(&count, "limit", 50, "max conversations to return")
	return c
}

// parseFolder maps the user-facing --folder string to the libgm folder
// enum. Accepts case-insensitive aliases for the obvious cases.
func parseFolder(name string) (gm.ConversationFolder, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "inbox", "in":
		return gm.FolderInbox, nil
	case "archive", "archived":
		return gm.FolderArchive, nil
	case "spam", "blocked", "spam_blocked", "spam-blocked":
		return gm.FolderSpamBlocked, nil
	default:
		return 0, fmt.Errorf("unknown folder %q (expected: inbox, archive, spam)", name)
	}
}

// participantSummary parses the participants_json blob and returns a comma
// separated list of names (preferring formatted_number when name is empty).
// Best-effort — malformed JSON returns "?".
func participantSummary(js string) string {
	if js == "" || js == "[]" {
		return ""
	}
	var parts []map[string]any
	if err := json.Unmarshal([]byte(js), &parts); err != nil {
		return "?"
	}
	names := make([]string, 0, len(parts))
	for _, p := range parts {
		if v, _ := p["is_me"].(bool); v {
			continue
		}
		if name, _ := p["name"].(string); name != "" {
			names = append(names, name)
			continue
		}
		if n, _ := p["formatted_number"].(string); n != "" {
			names = append(names, n)
			continue
		}
		if n, _ := p["e164"].(string); n != "" {
			names = append(names, n)
		}
	}
	if len(names) == 0 {
		return ""
	}
	out := names[0]
	for _, n := range names[1:] {
		out += ", " + n
	}
	return truncate(out, 40)
}
