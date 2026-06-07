package engine

import (
	"testing"

	"github.com/PurgeBot-net/common/job"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
)

func TestEnsureChannelProgressBackfillsChannels(t *testing.T) {
	progress := &job.PurgeProgress{
		ChannelIDs:   []uint64{100, 200, 300},
		CurrentIndex: 9,
		Channels: []job.PurgeChannelProgress{
			{ChannelID: 100, Deleted: 2, Done: true},
		},
	}

	ensureChannelProgress(progress)

	if progress.CurrentIndex != len(progress.ChannelIDs) {
		t.Fatalf("current index was not clamped: %d", progress.CurrentIndex)
	}
	if len(progress.Channels) != len(progress.ChannelIDs) {
		t.Fatalf("channels were not backfilled: %#v", progress.Channels)
	}
	for i, id := range progress.ChannelIDs {
		if progress.Channels[i].ChannelID != id {
			t.Fatalf("channel %d mismatch: got %d want %d", i, progress.Channels[i].ChannelID, id)
		}
	}
}

func TestChannelResultsFromProgressIncludesCompletedAndErroredChannels(t *testing.T) {
	progress := &job.PurgeProgress{
		Channels: []job.PurgeChannelProgress{
			{ChannelID: 100, Deleted: 0, Done: true},
			{ChannelID: 200, Deleted: 3, Done: true},
			{ChannelID: 300, Error: "missing access", Done: true},
			{ChannelID: 400},
		},
	}

	results := channelResultsFromProgress(progress)

	if len(results) != 3 {
		t.Fatalf("unexpected result count: got %d want 3", len(results))
	}
	if results[0].name != "<#100>" || results[0].deleted != 0 || results[0].err != nil {
		t.Fatalf("unexpected first result: %#v", results[0])
	}
	if results[1].name != "<#200>" || results[1].deleted != 3 || results[1].err != nil {
		t.Fatalf("unexpected second result: %#v", results[1])
	}
	if results[2].name != "<#300>" || results[2].err == nil || results[2].err.Error() != "missing access" {
		t.Fatalf("unexpected errored result: %#v", results[2])
	}
}

func TestPendingDeletesHelpers(t *testing.T) {
	progress := &job.PurgeProgress{}

	setPendingDeletes(progress, 100, []snowflake.ID{200, 300})
	if progress.PendingChannelID != 100 {
		t.Fatalf("pending channel mismatch: %d", progress.PendingChannelID)
	}
	if len(progress.PendingDeleteIDs) != 2 || progress.PendingDeleteIDs[0] != 200 || progress.PendingDeleteIDs[1] != 300 {
		t.Fatalf("pending delete IDs mismatch: %#v", progress.PendingDeleteIDs)
	}

	clearPendingDeletes(progress)
	if progress.PendingChannelID != 0 || progress.PendingDeleteIDs != nil {
		t.Fatalf("pending deletes were not cleared: %#v", progress)
	}
}

func TestStatusTargetFromInteractionResponse(t *testing.T) {
	channelID, messageID, ok := statusTargetFromInteractionResponse(&discord.Message{
		ID:        200,
		ChannelID: 100,
		Flags:     discord.MessageFlagsNone,
	})
	if !ok || channelID != 100 || messageID != 200 {
		t.Fatalf("public response was not durable: channel=%d message=%d ok=%t", channelID, messageID, ok)
	}

	_, _, ok = statusTargetFromInteractionResponse(&discord.Message{
		ID:        201,
		ChannelID: 100,
		Flags:     discord.MessageFlagEphemeral,
	})
	if ok {
		t.Fatal("ephemeral response should not be treated as a durable status target")
	}
}

func TestInteractionResponseStillLoading(t *testing.T) {
	if !interactionResponseStillLoading(&discord.Message{Flags: discord.MessageFlagLoading}) {
		t.Fatal("deferred interaction response should still be loading")
	}
	if interactionResponseStillLoading(&discord.Message{Flags: discord.MessageFlagsNone}) {
		t.Fatal("regular message should not be treated as loading")
	}
}

func TestStatusFallbackErrorPredicates(t *testing.T) {
	if !shouldCreateStatusMessageAfterInteractionError(&rest.Error{Code: rest.JSONErrorCodeInvalidWebhookToken}) {
		t.Fatal("invalid webhook token should create a channel status message")
	}
	if !shouldCreateStatusMessageAfterInteractionError(&rest.Error{Code: rest.JSONErrorCodeUnknownWebhook}) {
		t.Fatal("unknown webhook should create a channel status message")
	}
	if shouldCreateStatusMessageAfterInteractionError(&rest.Error{Code: rest.JSONErrorCodeUnknownMessage}) {
		t.Fatal("unknown message is not an interaction-token failure")
	}
	if !shouldReplaceStatusMessageAfterUpdateError(&rest.Error{Code: rest.JSONErrorCodeUnknownMessage}) {
		t.Fatal("unknown status message should be replaced")
	}
	if shouldReplaceStatusMessageAfterUpdateError(&rest.Error{Code: rest.JSONErrorCodeInvalidWebhookToken}) {
		t.Fatal("invalid webhook token is not a channel-message update failure")
	}
}
