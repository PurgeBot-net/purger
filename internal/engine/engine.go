package engine

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/PurgeBot-net/common/job"
	"github.com/PurgeBot-net/database"
	"github.com/PurgeBot-net/locale"
	"github.com/PurgeBot-net/purger/config"
	"github.com/PurgeBot-net/purger/internal/ratelimit"
)

const (
	bulkDeleteMaxAge      = 14 * 24 * time.Hour
	fetchBatchSize        = 100
	bulkDeleteMaxBatch    = 100
	maxPagesBeforeFlush   = 20
	cancelPollInterval    = time.Second
	statusRefreshInterval = 5 * time.Second
)

var (
	ErrInterrupted = errors.New("purge interrupted")
	ErrCancelled   = errors.New("purge cancelled")
)

type Engine struct {
	cfg    config.Config
	logger *zap.Logger
	db     *database.Database
	redis  *redis.Client
	client *bot.Client
}

func New(cfg config.Config, logger *zap.Logger, db *database.Database, redis *redis.Client, client *bot.Client) *Engine {
	return &Engine{cfg: cfg, logger: logger, db: db, redis: redis, client: client}
}

// A failed lookup must stay distinguishable from a confirmed absence, because
// the inactive purge deletes on absence.
type memberStatus int

const (
	memberStatusUnknown memberStatus = iota
	memberStatusPresent
	memberStatusAbsent
)

type memberLookup struct {
	roles  []snowflake.ID
	status memberStatus
}

type execState struct {
	// job.SaveProgress marshals the whole blob and writes UpdatedAt back into it,
	// so the save runs under mu alongside the mutations.
	mu       sync.Mutex
	progress *job.PurgeProgress

	// members caches guild member lookups. Key absent = not yet fetched.
	membersMu sync.RWMutex
	members   map[snowflake.ID]memberLookup

	// lang is j.Locale, for the delete paths that do not carry the job.
	lang string

	// See permissions.go. Both caches expire after botPermsTTL.
	botPermsMu sync.Mutex
	botPermsAt time.Time
	botPerms   botPermissions

	parentMu         sync.RWMutex
	parentOverwrites map[snowflake.ID]parentLookup

	// excluded holds the bot's own status messages. Append-only: the UI can
	// replace its message mid-run, and clearing the set would leave a window in
	// which a channel goroutine deletes the replacement.
	excludedMu sync.RWMutex
	excluded   map[snowflake.ID]bool

	// filterRegex is pre-compiled when FilterMode is regex; nil otherwise.
	filterRegex *regexp.Regexp

	// The status-message fields below are written only by the parent goroutine.
	// commandMessageID is the ID of the purge command's interaction response.
	commandMessageID snowflake.ID
	// fallbackChannelID and fallbackMessageID are used when the interaction
	// token has expired (e.g. after a worker crash/restart). Status updates
	// are sent by editing a regular channel message instead.
	fallbackChannelID snowflake.ID
	fallbackMessageID snowflake.ID
}

func newExecState(j *job.PurgeJob) (*execState, error) {
	s := &execState{
		lang:             j.Locale,
		members:          make(map[snowflake.ID]memberLookup),
		excluded:         make(map[snowflake.ID]bool),
		parentOverwrites: make(map[snowflake.ID]parentLookup),
	}
	if j.FilterMode == job.FilterModeRegex && j.Filter != "" {
		pattern := j.Filter
		if !j.CaseSensitive {
			pattern = "(?i)" + pattern
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("%s", locale.MsgPurgeInvalidRegex.In(j.Locale, err.Error()))
		}
		s.filterRegex = re
	}
	return s, nil
}

func memberLookupSaysAbsent(err error) bool {
	return rest.IsJSONErrorCode(err, rest.JSONErrorCodeUnknownMember, rest.JSONErrorCodeUnknownUser)
}

// Failures are deliberately not cached, so a transient error cannot poison the
// rest of the run. The REST call runs unlocked so a slow lookup does not block
// cache hits in the other channels; a duplicated lookup is harmless, and disgo
// serialises them anyway because the bucket is keyed on the guild.
func (s *execState) lookupMember(ctx context.Context, e *Engine, guildID, userID snowflake.ID) ([]snowflake.ID, memberStatus) {
	s.membersMu.RLock()
	cached, ok := s.members[userID]
	s.membersMu.RUnlock()
	if ok {
		return cached.roles, cached.status
	}
	member, err := e.client.Rest.GetMember(guildID, userID)
	if err != nil {
		if !memberLookupSaysAbsent(err) {
			e.logger.Warn("look up guild member", zap.Uint64("user", uint64(userID)), zap.Error(err))
			return nil, memberStatusUnknown
		}
		s.storeMember(userID, memberLookup{status: memberStatusAbsent})
		return nil, memberStatusAbsent
	}
	s.storeMember(userID, memberLookup{roles: member.RoleIDs, status: memberStatusPresent})
	return member.RoleIDs, memberStatusPresent
}

func (s *execState) storeMember(userID snowflake.ID, lookup memberLookup) {
	s.membersMu.Lock()
	defer s.membersMu.Unlock()
	s.members[userID] = lookup
}

func (s *execState) exclude(ids ...snowflake.ID) {
	s.excludedMu.Lock()
	defer s.excludedMu.Unlock()
	for _, id := range ids {
		if id != 0 {
			s.excluded[id] = true
		}
	}
}

func (s *execState) isExcluded(id snowflake.ID) bool {
	s.excludedMu.RLock()
	defer s.excludedMu.RUnlock()
	return s.excluded[id]
}

func (s *execState) totalDeleted() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.progress.TotalDeleted
}

func (s *execState) results() []channelResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return channelResultsFromProgress(s.progress)
}

type channelResult struct {
	name    string
	deleted int
	err     error
}

// Matched by channel ID, not position, so entries that drifted out of order cannot
// credit deletions to the wrong channel. Call once, before any goroutine takes a
// pointer into Channels.
func ensureChannelProgress(p *job.PurgeProgress) {
	stored := make(map[uint64]job.PurgeChannelProgress, len(p.Channels))
	for _, ch := range p.Channels {
		if ch.ChannelID != 0 {
			stored[ch.ChannelID] = ch
		}
	}
	channels := make([]job.PurgeChannelProgress, len(p.ChannelIDs))
	for i, id := range p.ChannelIDs {
		if existing, ok := stored[id]; ok {
			channels[i] = existing
			continue
		}
		channels[i] = job.PurgeChannelProgress{ChannelID: id}
	}
	p.Channels = channels
}

func channelResultsFromProgress(p *job.PurgeProgress) []channelResult {
	results := make([]channelResult, 0, len(p.Channels))
	for _, ch := range p.Channels {
		if !ch.Done && ch.Deleted == 0 && ch.Error == "" {
			continue
		}
		result := channelResult{name: fmt.Sprintf("<#%d>", ch.ChannelID), deleted: ch.Deleted}
		if ch.Error != "" {
			result.err = errors.New(ch.Error)
		}
		results = append(results, result)
	}
	return results
}

func (e *Engine) saveProgress(ctx context.Context, p *job.PurgeProgress) {
	saveCtx := ctx
	var cancel context.CancelFunc
	if ctx.Err() != nil {
		saveCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}
	if err := job.SaveProgress(saveCtx, e.redis, p); err != nil {
		e.logger.Warn("save purge progress", zap.String("id", p.JobID), zap.Error(err))
	}
}

func statusTargetFromInteractionResponse(msg *discord.Message) (snowflake.ID, snowflake.ID, bool) {
	if msg == nil || msg.ID == 0 || msg.ChannelID == 0 || msg.Flags.Has(discord.MessageFlagEphemeral) {
		return 0, 0, false
	}
	return msg.ChannelID, msg.ID, true
}

func interactionResponseStillLoading(msg *discord.Message) bool {
	return msg != nil && msg.Flags.Has(discord.MessageFlagLoading)
}

func shouldCreateStatusMessageAfterInteractionError(err error) bool {
	return rest.IsJSONErrorCode(err, rest.JSONErrorCodeInvalidWebhookToken, rest.JSONErrorCodeUnknownWebhook)
}

func shouldReplaceStatusMessageAfterUpdateError(err error) bool {
	return rest.IsJSONErrorCode(err, rest.JSONErrorCodeUnknownMessage)
}

func (e *Engine) createStatusMessage(ctx context.Context, j *job.PurgeJob, components ...discord.LayoutComponent) (snowflake.ID, snowflake.ID, bool) {
	if j.InteractionChannelID == 0 {
		return 0, 0, false
	}
	msg, err := e.client.Rest.CreateMessage(
		snowflake.ID(j.InteractionChannelID),
		discord.NewMessageCreateV2(components...),
	)
	if err != nil {
		e.logger.Warn("create channel status message", zap.Error(err))
		return 0, 0, false
	}
	channelID := msg.ChannelID
	if channelID == 0 {
		channelID = snowflake.ID(j.InteractionChannelID)
	}
	return channelID, msg.ID, true
}

func (e *Engine) adoptInteractionResponseTarget(ctx context.Context, j *job.PurgeJob, state *execState) {
	msg, err := e.client.Rest.GetInteractionResponse(snowflake.ID(j.ApplicationID), j.InteractionToken)
	if err != nil {
		e.logger.Warn("fetch interaction response for status target", zap.Error(err))
		return
	}
	if msg.ID != 0 {
		state.commandMessageID = msg.ID
		state.exclude(msg.ID)
	}
	if channelID, messageID, ok := statusTargetFromInteractionResponse(msg); ok {
		e.setStatusTarget(ctx, state, channelID, messageID)
	}
}

func (e *Engine) dismissStaleInteractionMessage(j *job.PurgeJob, state *execState, newMessageID snowflake.ID) {
	if state.commandMessageID == 0 || state.commandMessageID == newMessageID {
		return
	}
	channelID := state.fallbackChannelID
	if channelID == 0 && j.InteractionChannelID != 0 {
		channelID = snowflake.ID(j.InteractionChannelID)
	}
	if channelID == 0 {
		return
	}
	if err := e.client.Rest.DeleteMessage(channelID, state.commandMessageID); err != nil {
		e.logger.Warn("delete stale interaction status message", zap.Error(err))
	}
}

func (e *Engine) setStatusTarget(ctx context.Context, state *execState, channelID, messageID snowflake.ID) {
	state.fallbackChannelID = channelID
	state.fallbackMessageID = messageID
	state.exclude(messageID)
	e.persistMessageTargets(ctx, state)
}

func (e *Engine) clearStatusTarget(ctx context.Context, state *execState) {
	state.fallbackChannelID = 0
	state.fallbackMessageID = 0
	e.persistMessageTargets(ctx, state)
}

func (e *Engine) persistMessageTargets(ctx context.Context, state *execState) {
	if state.progress == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	changed := false
	if state.commandMessageID != 0 && state.progress.CommandMessageID != uint64(state.commandMessageID) {
		state.progress.CommandMessageID = uint64(state.commandMessageID)
		changed = true
	}
	if state.progress.FallbackChannelID != uint64(state.fallbackChannelID) {
		state.progress.FallbackChannelID = uint64(state.fallbackChannelID)
		changed = true
	}
	if state.progress.FallbackMessageID != uint64(state.fallbackMessageID) {
		state.progress.FallbackMessageID = uint64(state.fallbackMessageID)
		changed = true
	}
	if changed {
		e.saveProgress(ctx, state.progress)
	}
}

func (e *Engine) isCancelled(jobID string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cancelled, err := job.IsCancelled(ctx, e.redis, jobID)
	if err != nil {
		e.logger.Warn("check purge cancellation", zap.String("id", jobID), zap.Error(err))
	}
	return cancelled
}

// Every write to progress goes through here, so a mutation cannot interleave with
// another goroutine marshalling the blob.
func (e *Engine) commit(ctx context.Context, state *execState, mutate func()) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if mutate != nil {
		mutate()
	}
	e.saveProgress(ctx, state.progress)
}

func addDeleted(p *job.PurgeProgress, ch *job.PurgeChannelProgress, n int) {
	if n <= 0 || ch == nil {
		return
	}
	ch.Deleted += n
	p.TotalDeleted += n
}

func setPendingDeletes(ch *job.PurgeChannelProgress, ids []snowflake.ID) {
	ch.PendingDeleteIDs = make([]uint64, len(ids))
	for i, id := range ids {
		ch.PendingDeleteIDs[i] = uint64(id)
	}
}

func clearPendingDeletes(ch *job.PurgeChannelProgress) {
	ch.PendingDeleteIDs = nil
}

// worker.go branches on the difference: an interrupted job stays active for
// recovery, a cancelled one is cleaned up.
func abortReason(ctx context.Context) error {
	if errors.Is(context.Cause(ctx), ErrCancelled) {
		return ErrCancelled
	}
	if ctx.Err() != nil {
		return ErrInterrupted
	}
	return nil
}

// An aborted context surfaces as a transport error, not a Discord error code, so a REST
// failure has to be re-read against the context before it counts as a real failure.
func abortedDuring(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	return abortReason(ctx)
}

// These reject every other message in the channel too, so per-message retries
// would be pointless.
func deleteDeniedForChannel(err error) bool {
	return rest.IsJSONErrorCode(err,
		rest.JSONErrorCodeMissingAccess,
		rest.JSONErrorCodeLackPermissionsToPerformAction,
	)
}

func shouldFlush(buffered, pagesSinceFlush int) bool {
	return buffered >= bulkDeleteMaxBatch || pagesSinceFlush >= maxPagesBeforeFlush
}

func nextBatch(buffered []snowflake.ID) ([]snowflake.ID, []snowflake.ID) {
	if len(buffered) > bulkDeleteMaxBatch {
		return buffered[:bulkDeleteMaxBatch], buffered[bulkDeleteMaxBatch:]
	}
	return buffered, nil
}

// A message can cross the 14-day line while it waits in the buffer, and a single
// one that has rejects the whole bulk delete.
func splitAgedOut(buffered []snowflake.ID) (fresh, aged []snowflake.ID) {
	cutoff := time.Now().Add(-bulkDeleteMaxAge)
	for _, id := range buffered {
		if id.Time().After(cutoff) {
			fresh = append(fresh, id)
		} else {
			aged = append(aged, id)
		}
	}
	return fresh, aged
}

func (e *Engine) deleteEach(ctx context.Context, state *execState, ch *job.PurgeChannelProgress, ids []snowflake.ID) (int, error) {
	unresolved := 0
	for _, id := range ids {
		if reason := abortReason(ctx); reason != nil {
			e.commit(ctx, state, nil)
			return unresolved, reason
		}
		failed, err := e.deleteBatch(ctx, state, ch, []snowflake.ID{id})
		if err != nil {
			return unresolved, err
		}
		unresolved += failed
	}
	return unresolved, nil
}

// Returns how many messages it still could not remove.
func (e *Engine) settlePendingDeletes(ctx context.Context, state *execState, ch *job.PurgeChannelProgress) (int, error) {
	if len(ch.PendingDeleteIDs) == 0 {
		return 0, nil
	}

	channelID := ch.ChannelID
	cid := snowflake.ID(channelID)

	var retry []snowflake.ID
	var reconciled int
	for _, rawID := range ch.PendingDeleteIDs {
		if reason := abortReason(ctx); reason != nil {
			e.commit(ctx, state, nil)
			return 0, reason
		}

		id := snowflake.ID(rawID)
		if _, err := e.client.Rest.GetMessage(cid, id, rest.WithCtx(ctx)); err == nil {
			retry = append(retry, id)
		} else if rest.IsJSONErrorCode(err, rest.JSONErrorCodeUnknownMessage) {
			reconciled++
		} else if reason := abortedDuring(ctx, err); reason != nil {
			// Leaves PendingDeleteIDs whole so recovery redoes the batch.
			e.commit(ctx, state, nil)
			return 0, reason
		} else {
			e.logger.Warn("check pending deleted message", zap.Uint64("channel", channelID), zap.Uint64("message", rawID), zap.Error(err))
			retry = append(retry, id)
		}
	}

	e.commit(ctx, state, func() {
		addDeleted(state.progress, ch, reconciled)
		setPendingDeletes(ch, retry)
	})

	unresolved := 0
	for _, id := range retry {
		if reason := abortReason(ctx); reason != nil {
			e.commit(ctx, state, nil)
			return unresolved, reason
		}

		err := e.client.Rest.DeleteMessage(cid, id, rest.WithCtx(ctx))
		switch {
		case err == nil || rest.IsJSONErrorCode(err, rest.JSONErrorCodeUnknownMessage):
			e.commit(ctx, state, func() { addDeleted(state.progress, ch, 1) })
		case deleteDeniedForChannel(err):
			e.commit(ctx, state, func() { clearPendingDeletes(ch) })
			e.logger.Warn("delete denied", zap.Uint64("channel", channelID), zap.Error(err))
			return unresolved, fmt.Errorf("%s", locale.MsgPurgeMissingPerms.In(state.lang, permissionNames(deniedPermission(err))))
		default:
			if reason := abortedDuring(ctx, err); reason != nil {
				e.commit(ctx, state, nil)
				return unresolved, reason
			}
			unresolved++
			e.logger.Warn("delete pending message", zap.Uint64("channel", channelID), zap.Uint64("message", uint64(id)), zap.Error(err))
		}
	}

	e.commit(ctx, state, func() { clearPendingDeletes(ch) })
	return unresolved, nil
}

// Discord rejects a bulk delete as a whole, so a rejected batch is retried
// message by message rather than lost. Returns how many still failed.
func (e *Engine) deleteBatch(ctx context.Context, state *execState, ch *job.PurgeChannelProgress, batch []snowflake.ID) (int, error) {
	channelID := ch.ChannelID
	cid := snowflake.ID(channelID)
	e.commit(ctx, state, func() { setPendingDeletes(ch, batch) })

	// ctx must never carry a deadline: disgo fails a request whose deadline falls
	// before the bucket reset it is waiting for.
	var err error
	if len(batch) == 1 {
		err = e.client.Rest.DeleteMessage(cid, batch[0], rest.WithCtx(ctx))
		if rest.IsJSONErrorCode(err, rest.JSONErrorCodeUnknownMessage) {
			err = nil
		}
	} else {
		err = e.client.Rest.BulkDeleteMessages(cid, batch, rest.WithCtx(ctx))
	}

	if err == nil {
		e.commit(ctx, state, func() {
			addDeleted(state.progress, ch, len(batch))
			clearPendingDeletes(ch)
		})
		return 0, nil
	}
	if deleteDeniedForChannel(err) {
		e.commit(ctx, state, func() { clearPendingDeletes(ch) })
		e.logger.Warn("delete denied", zap.Uint64("channel", channelID), zap.Error(err))
		return 0, fmt.Errorf("%s", locale.MsgPurgeMissingPerms.In(state.lang, permissionNames(deniedPermission(err))))
	}

	// Neither is one bad message in the batch, so splitting it up would only multiply
	// the load Discord just refused.
	if reason := abortedDuring(ctx, err); reason != nil {
		e.commit(ctx, state, nil)
		return 0, reason
	}
	if ratelimit.IsTooManyRequests(err) {
		e.commit(ctx, state, func() { clearPendingDeletes(ch) })
		e.logger.Warn("bulk delete still rate limited after retries",
			zap.Uint64("channel", channelID), zap.Int("count", len(batch)))
		return len(batch), nil
	}

	e.logger.Warn("delete messages, retrying individually",
		zap.Uint64("channel", channelID), zap.Int("count", len(batch)), zap.Error(err))
	return e.settlePendingDeletes(ctx, state, ch)
}

// Execute runs a purge job end-to-end.
func (e *Engine) Execute(ctx context.Context, j *job.PurgeJob) error {
	start := time.Now()
	target := e.targetDisplay(j)

	if ctx.Err() != nil {
		return ErrInterrupted
	}

	progressCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	progress, err := job.GetProgress(progressCtx, e.redis, j.ID)
	cancel()
	if err != nil {
		return fmt.Errorf("get purge progress: %w", err)
	}
	if ctx.Err() != nil {
		return ErrInterrupted
	}

	showBranding := true
	if c, err := e.db.GetCustomization(ctx, int64(j.GuildID)); err == nil && c != nil {
		showBranding = !c.RemoveBranding
	}

	// Set up the response channel before anything else so errors can always be delivered.
	var commandMsgID, fallbackChanID, fallbackMsgID snowflake.ID
	statusJustCreated := false
	if progress != nil {
		commandMsgID = snowflake.ID(progress.CommandMessageID)
		fallbackChanID = snowflake.ID(progress.FallbackChannelID)
		fallbackMsgID = snowflake.ID(progress.FallbackMessageID)
	}
	if fallbackMsgID == 0 {
		if msg, err := e.client.Rest.GetInteractionResponse(snowflake.ID(j.ApplicationID), j.InteractionToken); err == nil {
			commandMsgID = msg.ID
			if _, _, ok := statusTargetFromInteractionResponse(msg); !ok {
				startContainer := discord.NewContainer(
					discord.NewTextDisplay(locale.MsgPurgeInProgress.In(j.Locale, target)),
					discord.NewTextDisplay(locale.MsgPurgeStatusLabel.In(j.Locale, locale.MsgPurgeStatusStarting.In(j.Locale))),
				)
				if channelID, messageID, ok := e.createStatusMessage(ctx, j, startContainer, cancelButton(j)); ok {
					e.dismissStaleInteractionMessage(j, &execState{commandMessageID: commandMsgID}, messageID)
					fallbackChanID = channelID
					fallbackMsgID = messageID
					statusJustCreated = true
				}
			}
		} else if j.InteractionChannelID != 0 {
			e.logger.Info("interaction response unavailable, creating channel status message", zap.Error(err))
			startContainer := discord.NewContainer(
				discord.NewTextDisplay(locale.MsgPurgeInProgress.In(j.Locale, target)),
				discord.NewTextDisplay(locale.MsgPurgeStatusLabel.In(j.Locale, locale.MsgPurgeStatusStarting.In(j.Locale))),
			)
			if channelID, messageID, ok := e.createStatusMessage(ctx, j, startContainer, cancelButton(j)); ok {
				fallbackChanID = channelID
				fallbackMsgID = messageID
				statusJustCreated = true
			}
		}
	} else if fallbackChanID != 0 && fallbackMsgID != 0 {
		if msg, err := e.client.Rest.GetMessage(fallbackChanID, fallbackMsgID); err == nil && interactionResponseStillLoading(msg) {
			e.logger.Info("restored status message still loading, re-acknowledging via interaction webhook")
			commandMsgID = snowflake.ID(progress.CommandMessageID)
			fallbackChanID = 0
			fallbackMsgID = 0
			progress.FallbackChannelID = 0
			progress.FallbackMessageID = 0
		}
	}

	state, err := newExecState(j)
	if err != nil {
		errState := &execState{fallbackChannelID: fallbackChanID, fallbackMessageID: fallbackMsgID}
		e.updateText(ctx, j, errState, fmt.Sprintf("❌ %s", err.Error()))
		return err
	}
	state.commandMessageID = commandMsgID
	state.fallbackChannelID = fallbackChanID
	state.fallbackMessageID = fallbackMsgID

	resumed := progress != nil

	if progress == nil {
		channels, err := e.resolveChannels(ctx, j)
		if err != nil {
			e.updateText(ctx, j, state, locale.MsgPurgeResolveError.In(j.Locale, err.Error()))
			return err
		}
		cutoff := time.Time{}
		if j.Days > 0 {
			cutoff = start.Add(-time.Duration(j.Days) * 24 * time.Hour)
		}
		progress = &job.PurgeProgress{
			JobID:             j.ID,
			GuildID:           j.GuildID,
			StartedAt:         start,
			CutoffAt:          cutoff,
			ChannelIDs:        channels,
			CommandMessageID:  uint64(commandMsgID),
			FallbackChannelID: uint64(fallbackChanID),
			FallbackMessageID: uint64(fallbackMsgID),
		}
		ensureChannelProgress(progress)
		e.saveProgress(ctx, progress)
	} else {
		if progress.StartedAt.IsZero() {
			progress.StartedAt = start
		}
		if progress.CutoffAt.IsZero() && j.Days > 0 {
			progress.CutoffAt = progress.StartedAt.Add(-time.Duration(j.Days) * 24 * time.Hour)
		}
		if commandMsgID != 0 {
			progress.CommandMessageID = uint64(commandMsgID)
		}
		if fallbackChanID != 0 {
			progress.FallbackChannelID = uint64(fallbackChanID)
		}
		if fallbackMsgID != 0 {
			progress.FallbackMessageID = uint64(fallbackMsgID)
		}
		ensureChannelProgress(progress)
		e.saveProgress(ctx, progress)
	}
	state.progress = progress
	// Both sources: on resume the stored IDs can predate the ones just resolved,
	// and either would be the bot's own message.
	state.exclude(
		state.commandMessageID, state.fallbackMessageID,
		snowflake.ID(progress.CommandMessageID), snowflake.ID(progress.FallbackMessageID),
	)

	if !statusJustCreated {
		e.sendInProgress(ctx, j, state, target, locale.MsgPurgeStatusStarting.In(j.Locale), true)
	}

	pending := make([]*job.PurgeChannelProgress, 0, len(progress.Channels))
	for i := range progress.Channels {
		if !progress.Channels[i].Done {
			pending = append(pending, &progress.Channels[i])
		}
	}

	jobCtx, abort := context.WithCancelCause(ctx)
	defer abort(nil)

	g, gctx := errgroup.WithContext(jobCtx)
	g.SetLimit(min(max(e.cfg.ChannelConcurrency, 1), max(len(pending), 1)))

	var inFlight sync.Map
	workers := new(sync.WaitGroup)
	workers.Add(2)

	go func() {
		defer workers.Done()
		ticker := time.NewTicker(cancelPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-gctx.Done():
				return
			case <-ticker.C:
				if e.isCancelled(j.ID) {
					abort(ErrCancelled)
					return
				}
			}
		}
	}()

	// The status message is edited only from here, never from a channel goroutine.
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(statusRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-gctx.Done():
				return
			case <-ticker.C:
				names := inFlightNames(progress.ChannelIDs, &inFlight)
				if names == "" {
					continue
				}
				e.sendInProgress(ctx, j, state, target, locale.MsgPurgeStatusFetching.In(j.Locale, names), true)
			}
		}
	}()

	for _, ch := range pending {
		g.Go(func() error {
			inFlight.Store(ch.ChannelID, true)
			defer inFlight.Delete(ch.ChannelID)

			err := e.purgeChannel(gctx, j, state, ch)
			// Only these abort the job. A channel that simply failed is recorded and
			// the rest carry on, exactly as when channels ran one at a time.
			if errors.Is(err, ErrCancelled) || errors.Is(err, ErrInterrupted) {
				return err
			}
			if err != nil {
				e.logger.Warn("channel purge failed", zap.Uint64("channel", ch.ChannelID), zap.Error(err))
			}
			e.commit(ctx, state, func() {
				ch.Done = true
				if err != nil {
					ch.Error = err.Error()
				}
			})
			return nil
		})
	}

	waitErr := g.Wait()
	abort(nil)
	workers.Wait()

	if errors.Is(waitErr, ErrCancelled) {
		e.sendCancelled(ctx, j, state, state.totalDeleted(), showBranding)
		return ErrCancelled
	}
	if waitErr != nil {
		e.commit(ctx, state, nil)
		return ErrInterrupted
	}

	if e.isCancelled(j.ID) {
		e.sendCancelled(ctx, j, state, state.totalDeleted(), showBranding)
		return ErrCancelled
	}
	if ctx.Err() != nil {
		e.commit(ctx, state, nil)
		return ErrInterrupted
	}

	totalDeleted := state.totalDeleted()
	elapsed := time.Since(progress.StartedAt)
	results := state.results()
	failed := 0
	for _, r := range results {
		if r.err != nil {
			failed++
		}
	}

	// deleted is cumulative across attempts, duration is only this run.
	e.logger.Info("purge job completed",
		zap.String("id", j.ID),
		zap.Uint64("guild_id", j.GuildID),
		zap.Int("deleted", totalDeleted),
		zap.Int("channels", len(results)),
		zap.Int("failed", failed),
		zap.Bool("resumed", resumed),
		zap.Duration("duration", time.Since(start)),
	)

	e.sendCompletion(ctx, j, state, target, totalDeleted, elapsed, results, showBranding)

	if err := e.db.RecordPurgeEvent(ctx, database.RecordPurgeEventParams{
		GuildID:    int64(j.GuildID),
		PurgeType:  string(j.PurgeType),
		TargetType: string(j.TargetType),
		Deleted:    totalDeleted,
		DurationMs: int(elapsed.Milliseconds()),
	}); err != nil {
		e.logger.Warn("record purge event", zap.Error(err))
	}

	return nil
}

// Resolve order, so the status message does not reorder as channels finish.
func inFlightNames(order []uint64, inFlight *sync.Map) string {
	names := make([]string, 0, len(order))
	for _, id := range order {
		if _, ok := inFlight.Load(id); ok {
			names = append(names, fmt.Sprintf("<#%d>", id))
		}
	}
	return strings.Join(names, ", ")
}

// resolveChannels returns the ordered list of channel (and thread) IDs to purge.
func (e *Engine) resolveChannels(ctx context.Context, j *job.PurgeJob) ([]uint64, error) {
	skipSet := make(map[uint64]bool, len(j.SkipChannelIDs))
	for _, id := range j.SkipChannelIDs {
		skipSet[id] = true
	}

	switch j.TargetType {
	case job.TargetTypeChannel:
		result := []uint64{j.TargetID}
		if j.IncludeThreads {
			threads := e.fetchThreadsForChannels(ctx, j.GuildID, result)
			result = append(result, threads...)
		}
		return result, nil

	case job.TargetTypeCategory:
		channels, err := e.client.Rest.GetGuildChannels(snowflake.ID(j.GuildID))
		if err != nil {
			return nil, fmt.Errorf("fetch channels: %w", err)
		}
		var result, forumIDs, textThreadParents []uint64
		for _, ch := range channels {
			if ch.ParentID() == nil || uint64(*ch.ParentID()) != j.TargetID {
				continue
			}
			if skipSet[uint64(ch.ID())] {
				continue
			}
			switch ch.Type() {
			case discord.ChannelTypeGuildText, discord.ChannelTypeGuildNews, discord.ChannelTypeGuildVoice:
				result = append(result, uint64(ch.ID()))
				if ch.Type() != discord.ChannelTypeGuildVoice {
					textThreadParents = append(textThreadParents, uint64(ch.ID()))
				}
			case discord.ChannelTypeGuildForum:
				forumIDs = append(forumIDs, uint64(ch.ID()))
			}
		}
		// Forum threads are always included (the forum channel itself has no messages).
		if len(forumIDs) > 0 {
			result = append(result, e.fetchThreadsForChannels(ctx, j.GuildID, forumIDs)...)
		}
		if j.IncludeThreads && len(textThreadParents) > 0 {
			result = append(result, e.fetchThreadsForChannels(ctx, j.GuildID, textThreadParents)...)
		}
		return result, nil

	case job.TargetTypeServer:
		channels, err := e.client.Rest.GetGuildChannels(snowflake.ID(j.GuildID))
		if err != nil {
			return nil, fmt.Errorf("fetch channels: %w", err)
		}
		var result, forumIDs, textThreadParents []uint64
		for _, ch := range channels {
			if skipSet[uint64(ch.ID())] {
				continue
			}
			switch ch.Type() {
			case discord.ChannelTypeGuildText, discord.ChannelTypeGuildNews, discord.ChannelTypeGuildVoice:
				result = append(result, uint64(ch.ID()))
				if ch.Type() != discord.ChannelTypeGuildVoice {
					textThreadParents = append(textThreadParents, uint64(ch.ID()))
				}
			case discord.ChannelTypeGuildForum:
				forumIDs = append(forumIDs, uint64(ch.ID()))
			}
		}
		if len(forumIDs) > 0 {
			result = append(result, e.fetchThreadsForChannels(ctx, j.GuildID, forumIDs)...)
		}
		if j.IncludeThreads && len(textThreadParents) > 0 {
			result = append(result, e.fetchThreadsForChannels(ctx, j.GuildID, textThreadParents)...)
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unknown target type: %s", j.TargetType)
	}
}

// fetchThreadsForChannels returns active and archived thread IDs whose parent is in parentIDs.
func (e *Engine) fetchThreadsForChannels(ctx context.Context, guildID uint64, parentIDs []uint64) []uint64 {
	parentSet := make(map[uint64]bool, len(parentIDs))
	for _, id := range parentIDs {
		parentSet[id] = true
	}

	var threadIDs []uint64

	active, err := e.client.Rest.GetActiveGuildThreads(snowflake.ID(guildID))
	if err == nil {
		for _, t := range active.Threads {
			if parentSet[uint64(*t.ParentID())] {
				threadIDs = append(threadIDs, uint64(t.ID()))
			}
		}
	} else {
		e.logger.Warn("fetch active guild threads", zap.Error(err))
	}

	for _, chanID := range parentIDs {
		cid := snowflake.ID(chanID)
		if pub, err := e.client.Rest.GetPublicArchivedThreads(cid, time.Time{}, 0); err == nil {
			for _, t := range pub.Threads {
				threadIDs = append(threadIDs, uint64(t.ID()))
			}
		}
		if priv, err := e.client.Rest.GetPrivateArchivedThreads(cid, time.Time{}, 0); err == nil {
			for _, t := range priv.Threads {
				threadIDs = append(threadIDs, uint64(t.ID()))
			}
		}
	}

	return threadIDs
}

// purgeChannel fetches and deletes matching messages from a single channel or
// thread. ch is owned by this goroutine; every write to it goes through commit.
func (e *Engine) purgeChannel(ctx context.Context, j *job.PurgeJob, state *execState, ch *job.PurgeChannelProgress) error {
	channelID := ch.ChannelID
	cid := snowflake.ID(channelID)
	unresolved := 0

	// Not at job start: a permission can be taken away between the gate and this channel's turn.
	if missing := e.missingPermissionsForChannel(ctx, state, snowflake.ID(j.GuildID), cid); missing != 0 {
		return fmt.Errorf("%s", locale.MsgPurgeMissingPerms.In(j.Locale, permissionNames(missing)))
	}

	state.mu.Lock()
	cutoff := state.progress.CutoffAt
	state.mu.Unlock()

	// Matches accumulate across fetch pages so a bulk delete carries a full batch
	// rather than whatever matched in one page of 100. ch.BeforeID stays put until
	// a flush, so an interrupted run re-scans those pages instead of skipping
	// them; anything already deleted no longer comes back from GetMessages.
	var buffered []snowflake.ID
	scannedTo := ch.BeforeID
	pagesSinceFlush := 0

	flush := func() error {
		fresh, aged := splitAgedOut(buffered)
		buffered = fresh

		failed, err := e.deleteEach(ctx, state, ch, aged)
		unresolved += failed
		if err != nil {
			return err
		}

		for len(buffered) > 0 {
			if reason := abortReason(ctx); reason != nil {
				e.commit(ctx, state, nil)
				return reason
			}

			var batch []snowflake.ID
			batch, buffered = nextBatch(buffered)
			failed, err := e.deleteBatch(ctx, state, ch, batch)
			if err != nil {
				return err
			}
			unresolved += failed
		}
		e.commit(ctx, state, func() { ch.BeforeID = scannedTo })
		pagesSinceFlush = 0
		return nil
	}

	for {
		settled, err := e.settlePendingDeletes(ctx, state, ch)
		if err != nil {
			return err
		}
		unresolved += settled
		if reason := abortReason(ctx); reason != nil {
			e.commit(ctx, state, nil)
			return reason
		}

		messages, err := e.client.Rest.GetMessages(cid, 0, snowflake.ID(scannedTo), 0, fetchBatchSize, rest.WithCtx(ctx))
		if err != nil {
			// A failed channel is recorded and skipped, so shutdown must not look like one.
			if reason := abortReason(ctx); reason != nil {
				e.commit(ctx, state, nil)
				return reason
			}
			if fetchDeniedForChannel(err) {
				e.logger.Warn("fetch denied", zap.Uint64("channel", channelID), zap.Error(err))
				return fmt.Errorf("%s", locale.MsgPurgeMissingPerms.In(j.Locale, permissionNames(deniedPermission(err))))
			}
			return fmt.Errorf("fetch messages: %w", err)
		}
		if len(messages) == 0 {
			break
		}

		var oldMessages []snowflake.ID
		var atCutoff bool

		for _, msg := range messages {
			if !cutoff.IsZero() && msg.CreatedAt.Before(cutoff) {
				atCutoff = true
				break
			}
			if state.isExcluded(msg.ID) {
				continue
			}
			if j.SkipUserID != 0 && msg.Author.ID == snowflake.ID(j.SkipUserID) {
				continue
			}
			if slices.Contains(j.SkipMessageIDs, uint64(msg.ID)) {
				continue
			}
			// One of these in a bulk delete takes the whole batch down with it.
			if !msg.Type.Deleteable() {
				continue
			}
			if !e.matchesJob(ctx, j, msg, state) {
				continue
			}
			if time.Since(msg.CreatedAt) < bulkDeleteMaxAge {
				buffered = append(buffered, msg.ID)
			} else {
				oldMessages = append(oldMessages, msg.ID)
			}
		}

		scannedTo = uint64(messages[len(messages)-1].ID)
		pagesSinceFlush++

		failed, err := e.deleteEach(ctx, state, ch, oldMessages)
		unresolved += failed
		if err != nil {
			return err
		}

		if shouldFlush(len(buffered), pagesSinceFlush) {
			if err := flush(); err != nil {
				return err
			}
		}

		if atCutoff || len(messages) < fetchBatchSize {
			break
		}
	}

	if err := flush(); err != nil {
		return err
	}

	if unresolved > 0 {
		return fmt.Errorf("%d message(s) could not be deleted", unresolved)
	}
	return nil
}

// Interaction replies arrive as webhook messages whose webhook ID is the
// application's own, so they otherwise look like webhook posts.
func isInteractionResponse(msg discord.Message) bool {
	if msg.InteractionMetadata != nil || msg.Interaction != nil {
		return true
	}
	// Replies predating interaction_metadata carry neither field.
	return msg.ApplicationID != nil && msg.WebhookID != nil && *msg.WebhookID == *msg.ApplicationID
}

// webhook_id alone is not this test: an interaction reply carries one too, but its
// author is the app's own bot user, which is a member and can hold roles.
func authorIsWebhook(msg discord.Message) bool {
	return msg.WebhookID != nil && !isInteractionResponse(msg)
}

// Migrated accounts report discriminator "0"; only pre-migration ones report "0000".
func isDeletedAccount(u discord.User) bool {
	if !strings.HasPrefix(u.Username, "Deleted User") {
		return false
	}
	return u.Discriminator == "0" || u.Discriminator == "0000"
}

// matchesJob returns true if the message should be deleted.
//
// Discord makes a webhook post's author the webhook and flags it as a bot, so
// Author.Bot cannot separate the two and a membership lookup always comes back
// absent. Every branch below states what it does with fromWebhook.
func (e *Engine) matchesJob(ctx context.Context, j *job.PurgeJob, msg discord.Message, state *execState) bool {
	guildID := snowflake.ID(j.GuildID)
	fromWebhook := authorIsWebhook(msg)

	switch j.PurgeType {
	case job.PurgeTypeUser:
		if msg.Author.ID != snowflake.ID(j.FilterUserID) {
			return false
		}

	case job.PurgeTypeRole:
		if fromWebhook {
			return false
		}
		if !j.IncludeBots && msg.Author.Bot {
			return false
		}
		roles, status := state.lookupMember(ctx, e, guildID, msg.Author.ID)
		if status != memberStatusPresent {
			return false
		}
		// @everyone's ID is the guild's, and Discord omits it from a member's roles.
		if j.FilterRoleID != j.GuildID && !slices.Contains(roles, snowflake.ID(j.FilterRoleID)) {
			return false
		}

	case job.PurgeTypeEveryone:
		// Not fromWebhook: on a crosspost, Author.Bot is the original human author.
		if !j.IncludeBots && msg.Author.Bot {
			return false
		}

	case job.PurgeTypeBot:
		if fromWebhook || !msg.Author.Bot {
			return false
		}

	case job.PurgeTypeInactive:
		if fromWebhook || msg.Author.System {
			return false
		}
		if !j.IncludeBots && msg.Author.Bot {
			return false
		}
		if _, status := state.lookupMember(ctx, e, guildID, msg.Author.ID); status != memberStatusAbsent {
			return false
		}

	case job.PurgeTypeWebhook:
		if !fromWebhook {
			return false
		}

	case job.PurgeTypeDeleted:
		if fromWebhook || msg.Author.Bot {
			return false
		}
		if !isDeletedAccount(msg.Author) {
			return false
		}
	}

	matched := e.matchesFilter(j, msg, state)
	if j.FilterKeep {
		return !matched
	}
	return matched
}

func appendAttachmentTargets(targets []string, atts []discord.Attachment) []string {
	for _, a := range atts {
		targets = append(targets, a.Filename)
		// ContentType is absent on many older uploads, so the filename is matched too.
		if a.ContentType != nil {
			targets = append(targets, *a.ContentType)
		}
	}
	return targets
}

func filterTargets(msg discord.Message) []string {
	targets := appendAttachmentTargets([]string{msg.Content}, msg.Attachments)
	// A forward carries its text and uploads in a snapshot, not on itself.
	for _, snap := range msg.MessageSnapshots {
		targets = append(targets, snap.Message.Content)
		targets = appendAttachmentTargets(targets, snap.Message.Attachments)
	}
	return targets
}

func (e *Engine) matchesFilter(j *job.PurgeJob, msg discord.Message, state *execState) bool {
	if j.Filter == "" {
		return true
	}
	targets := filterTargets(msg)

	if j.FilterMode == job.FilterModeRegex {
		if state.filterRegex == nil {
			return false
		}
		for _, target := range targets {
			if state.filterRegex.MatchString(target) {
				return true
			}
		}
		return false
	}

	filter := j.Filter
	if !j.CaseSensitive {
		filter = strings.ToLower(filter)
	}
	for _, target := range targets {
		text := target
		if !j.CaseSensitive {
			text = strings.ToLower(text)
		}
		switch j.FilterMode {
		case job.FilterModeExact:
			if text == filter {
				return true
			}
		case job.FilterModeStartsWith:
			if strings.HasPrefix(text, filter) {
				return true
			}
		case job.FilterModeEndsWith:
			if strings.HasSuffix(text, filter) {
				return true
			}
		default:
			if strings.Contains(text, filter) {
				return true
			}
		}
	}
	return false
}

// ── UI helpers ────────────────────────────────────────────────────────────────

func (e *Engine) targetDisplay(j *job.PurgeJob) string {
	if j.TargetType == job.TargetTypeServer {
		return locale.MsgTargetServer.In(j.Locale)
	}
	return fmt.Sprintf("<#%d>", j.TargetID)
}

func cancelButton(j *job.PurgeJob) discord.ActionRowComponent {
	return discord.ActionRowComponent{Components: []discord.InteractiveComponent{
		discord.ButtonComponent{
			Style:    discord.ButtonStyleDanger,
			Label:    locale.MsgCancelButton.In(j.Locale),
			CustomID: fmt.Sprintf("cancel:%s:%d", j.ID, j.RequestedByID),
			Emoji:    &discord.ComponentEmoji{Name: "🛑"},
		},
	}}
}

func (e *Engine) sendInProgress(ctx context.Context, j *job.PurgeJob, state *execState, target, status string, withCancel bool) {
	container := discord.NewContainer(
		discord.NewTextDisplay(locale.MsgPurgeInProgress.In(j.Locale, target)),
		discord.NewTextDisplay(locale.MsgPurgeStatusLabel.In(j.Locale, status)),
	)
	components := []discord.LayoutComponent{container}
	if withCancel {
		components = append(components, cancelButton(j))
	}
	e.updateComponents(ctx, j, state, components...)
}

func (e *Engine) sendCancelled(ctx context.Context, j *job.PurgeJob, state *execState, totalDeleted int, showBranding bool) {
	msg := locale.MsgPurgeCancelledHeader.In(j.Locale)
	if totalDeleted > 0 {
		msg += locale.MsgPurgeCancelledCount.In(j.Locale, totalDeleted)
	}
	texts := []discord.ContainerSubComponent{discord.NewTextDisplay(msg)}
	if showBranding {
		texts = append(texts, discord.NewTextDisplay("-# Powered by PurgeBot"))
	}
	e.updateComponents(ctx, j, state, discord.NewContainer(texts...))
}

func (e *Engine) sendCompletion(ctx context.Context, j *job.PurgeJob, state *execState, target string, totalDeleted int, elapsed time.Duration, results []channelResult, showBranding bool) {
	texts := []discord.ContainerSubComponent{
		discord.NewTextDisplay(locale.MsgPurgeCompleteHeader.In(j.Locale, target)),
		discord.NewTextDisplay(locale.MsgPurgeCompleteTotalDeleted.In(j.Locale, totalDeleted)),
		discord.NewTextDisplay(locale.MsgPurgeCompleteDuration.In(j.Locale, elapsed.Seconds())),
		discord.NewTextDisplay(locale.MsgPurgeCompleteChannelsProcessed.In(j.Locale, len(results))),
	}

	var nonEmpty []channelResult
	for _, r := range results {
		if r.deleted > 0 {
			nonEmpty = append(nonEmpty, r)
		}
	}
	if len(results) <= 10 && len(nonEmpty) > 0 {
		lines := make([]string, len(nonEmpty))
		for i, r := range nonEmpty {
			lines[i] = locale.MsgPurgeCompleteChannelLine.In(j.Locale, r.name, r.deleted)
		}
		texts = append(texts, discord.NewTextDisplay(locale.MsgPurgeCompleteChannelBreakdown.In(j.Locale, strings.Join(lines, "\n"))))
	}

	if lines := skippedChannelLines(results, j.Locale); len(lines) > 0 {
		texts = append(texts, discord.NewTextDisplay(locale.MsgPurgeCompleteSkippedChannels.In(j.Locale, strings.Join(lines, "\n"))))
	}

	if showBranding {
		texts = append(texts, discord.NewTextDisplay("-# Powered by PurgeBot"))
	}
	e.updateComponents(ctx, j, state, discord.NewContainer(texts...))
}

func (e *Engine) updateText(ctx context.Context, j *job.PurgeJob, state *execState, text string) {
	e.updateComponents(ctx, j, state, discord.NewContainer(discord.NewTextDisplay(text)))
}

func (e *Engine) updateComponents(ctx context.Context, j *job.PurgeJob, state *execState, components ...discord.LayoutComponent) {
	if state.fallbackChannelID != 0 && state.fallbackMessageID != 0 {
		_, err := e.client.Rest.UpdateMessage(
			state.fallbackChannelID,
			state.fallbackMessageID,
			discord.NewMessageUpdateV2(components...),
		)
		if err != nil {
			if shouldReplaceStatusMessageAfterUpdateError(err) {
				e.logger.Info("status message disappeared, creating replacement", zap.Error(err))
				e.clearStatusTarget(ctx, state)
				if channelID, messageID, ok := e.createStatusMessage(ctx, j, components...); ok {
					e.setStatusTarget(ctx, state, channelID, messageID)
					return
				}
			}
			e.logger.Warn("update status message", zap.Error(err))
		}
		return
	}

	// The first edit after a deferred response must go through the interaction
	// webhook so Discord clears the "Bot is thinking..." loading state.
	_, err := e.client.Rest.UpdateInteractionResponse(
		snowflake.ID(j.ApplicationID),
		j.InteractionToken,
		discord.NewMessageUpdateV2(components...),
	)
	if err != nil {
		if shouldCreateStatusMessageAfterInteractionError(err) {
			e.logger.Info("interaction token expired, switching to channel status message", zap.Error(err))
			if channelID, messageID, ok := e.createStatusMessage(ctx, j, components...); ok {
				e.dismissStaleInteractionMessage(j, state, messageID)
				e.setStatusTarget(ctx, state, channelID, messageID)
				return
			}
		}
		e.logger.Warn("update interaction", zap.Error(err))
		return
	}
	e.adoptInteractionResponseTarget(ctx, j, state)
}
