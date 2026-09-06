package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/PurgeBot-net/common/job"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
)

func TestEnsureChannelProgressBackfillsChannels(t *testing.T) {
	progress := &job.PurgeProgress{
		ChannelIDs: []uint64{100, 200, 300},
		Channels: []job.PurgeChannelProgress{
			{ChannelID: 100, Deleted: 2, Done: true},
		},
	}

	ensureChannelProgress(progress)

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
	ch := &job.PurgeChannelProgress{ChannelID: 100}

	setPendingDeletes(ch, []snowflake.ID{200, 300})
	if len(ch.PendingDeleteIDs) != 2 || ch.PendingDeleteIDs[0] != 200 || ch.PendingDeleteIDs[1] != 300 {
		t.Fatalf("pending delete IDs mismatch: %#v", ch.PendingDeleteIDs)
	}

	clearPendingDeletes(ch)
	if ch.PendingDeleteIDs != nil {
		t.Fatalf("pending deletes were not cleared: %#v", ch)
	}
}

func TestEnsureChannelProgressMatchesByChannelID(t *testing.T) {
	// Entries stored out of order must follow their channel, not their position,
	// or deletions get credited to the wrong channel.
	progress := &job.PurgeProgress{
		ChannelIDs: []uint64{100, 200, 300},
		Channels: []job.PurgeChannelProgress{
			{ChannelID: 300, Deleted: 7, Done: true},
			{ChannelID: 100, Deleted: 2, BeforeID: 55},
		},
	}

	ensureChannelProgress(progress)

	if len(progress.Channels) != 3 {
		t.Fatalf("channels were not resized: %#v", progress.Channels)
	}
	if progress.Channels[0].ChannelID != 100 || progress.Channels[0].Deleted != 2 || progress.Channels[0].BeforeID != 55 {
		t.Fatalf("channel 100 lost its progress: %#v", progress.Channels[0])
	}
	if progress.Channels[1].ChannelID != 200 || progress.Channels[1].Deleted != 0 {
		t.Fatalf("channel 200 was not backfilled empty: %#v", progress.Channels[1])
	}
	if progress.Channels[2].ChannelID != 300 || progress.Channels[2].Deleted != 7 || !progress.Channels[2].Done {
		t.Fatalf("channel 300 lost its progress: %#v", progress.Channels[2])
	}
}

func TestAbortReasonDistinguishesCancelFromShutdown(t *testing.T) {
	if reason := abortReason(context.Background()); reason != nil {
		t.Fatalf("a live context is not an abort: %v", reason)
	}

	cancelled, abort := context.WithCancelCause(context.Background())
	abort(ErrCancelled)
	if reason := abortReason(cancelled); !errors.Is(reason, ErrCancelled) {
		t.Fatalf("user cancel should stay distinguishable: %v", reason)
	}

	shutdown, stop := context.WithCancel(context.Background())
	stop()
	if reason := abortReason(shutdown); !errors.Is(reason, ErrInterrupted) {
		t.Fatalf("shutdown should read as interrupted: %v", reason)
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

func ids(n int) []snowflake.ID {
	out := make([]snowflake.ID, n)
	for i := range out {
		out[i] = snowflake.ID(i + 1)
	}
	return out
}

func TestNextBatchCapsAtBulkDeleteLimit(t *testing.T) {
	batch, rest := nextBatch(nil)
	if len(batch) != 0 || len(rest) != 0 {
		t.Fatalf("empty buffer: got %d and %d", len(batch), len(rest))
	}

	batch, rest = nextBatch(ids(1))
	if len(batch) != 1 || len(rest) != 0 {
		t.Fatalf("single id: got %d and %d", len(batch), len(rest))
	}

	batch, rest = nextBatch(ids(bulkDeleteMaxBatch))
	if len(batch) != bulkDeleteMaxBatch || len(rest) != 0 {
		t.Fatalf("exactly one batch: got %d and %d", len(batch), len(rest))
	}

	// Only reachable now that a page can top up an almost-full buffer.
	batch, rest = nextBatch(ids(199))
	if len(batch) != bulkDeleteMaxBatch || len(rest) != 99 {
		t.Fatalf("overfull buffer: got %d and %d", len(batch), len(rest))
	}
	if batch[0] != snowflake.ID(1) || rest[0] != snowflake.ID(bulkDeleteMaxBatch+1) {
		t.Fatalf("split lost ordering: %v then %v", batch[0], rest[0])
	}
}

func TestShouldFlush(t *testing.T) {
	if shouldFlush(bulkDeleteMaxBatch-1, 1) {
		t.Fatal("a partial batch early in a channel should keep accumulating")
	}
	if !shouldFlush(bulkDeleteMaxBatch, 1) {
		t.Fatal("a full batch should flush")
	}
	if !shouldFlush(0, maxPagesBeforeFlush) {
		t.Fatal("a sparse channel should still checkpoint")
	}
}

func TestSplitAgedOut(t *testing.T) {
	fresh := snowflake.New(time.Now().Add(-time.Hour))
	aged := snowflake.New(time.Now().Add(-bulkDeleteMaxAge - time.Hour))

	gotFresh, gotAged := splitAgedOut([]snowflake.ID{fresh, aged, fresh})
	if len(gotFresh) != 2 {
		t.Fatalf("recent messages should stay bulk-deletable: %v", gotFresh)
	}
	if len(gotAged) != 1 || gotAged[0] != aged {
		t.Fatalf("a message that aged past 14 days must leave the batch: %v", gotAged)
	}
}
