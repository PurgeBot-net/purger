package engine

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/PurgeBot-net/common/job"
	"github.com/PurgeBot-net/database"
	"github.com/PurgeBot-net/locale"
	"github.com/PurgeBot-net/purger/config"
)

const (
	bulkDeleteMaxAge = 14 * 24 * time.Hour
	fetchBatchSize   = 100
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

// execState holds per-job state shared across channel iterations.
type execState struct {
	// memberRoles caches guild member role lookups.
	// Key absent = not yet fetched. Nil value = member not in guild.
	memberRoles map[snowflake.ID][]snowflake.ID
	// filterRegex is pre-compiled when FilterMode is regex; nil otherwise.
	filterRegex *regexp.Regexp
	// commandMessageID is the ID of the purge command's interaction response.
	// It is always excluded from deletion regardless of purge settings.
	commandMessageID snowflake.ID
	// fallbackChannelID and fallbackMessageID are used when the interaction
	// token has expired (e.g. after a worker crash/restart). Status updates
	// are sent by editing a regular channel message instead.
	fallbackChannelID snowflake.ID
	fallbackMessageID snowflake.ID
	progress          *job.PurgeProgress
}

func newExecState(j *job.PurgeJob) (*execState, error) {
	s := &execState{
		memberRoles: make(map[snowflake.ID][]snowflake.ID),
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

// getMemberRoles returns the member's role IDs and whether they are in the guild.
// Results are cached to avoid repeated REST calls for the same user.
func (s *execState) getMemberRoles(ctx context.Context, e *Engine, guildID, userID snowflake.ID) ([]snowflake.ID, bool) {
	if roles, ok := s.memberRoles[userID]; ok {
		return roles, roles != nil
	}
	member, err := e.client.Rest.GetMember(guildID, userID)
	if err != nil {
		s.memberRoles[userID] = nil
		return nil, false
	}
	s.memberRoles[userID] = member.RoleIDs
	return member.RoleIDs, true
}

type channelResult struct {
	name    string
	deleted int
	err     error
}

func ensureChannelProgress(p *job.PurgeProgress) {
	if p.CurrentIndex < 0 {
		p.CurrentIndex = 0
	}
	if len(p.Channels) < len(p.ChannelIDs) {
		existing := len(p.Channels)
		for i := existing; i < len(p.ChannelIDs); i++ {
			p.Channels = append(p.Channels, job.PurgeChannelProgress{ChannelID: p.ChannelIDs[i]})
		}
	}
	if len(p.Channels) > len(p.ChannelIDs) {
		p.Channels = p.Channels[:len(p.ChannelIDs)]
	}
	for i, id := range p.ChannelIDs {
		if p.Channels[i].ChannelID == 0 {
			p.Channels[i].ChannelID = id
		}
	}
	if p.CurrentIndex > len(p.ChannelIDs) {
		p.CurrentIndex = len(p.ChannelIDs)
	}
}

func currentChannelProgress(p *job.PurgeProgress) *job.PurgeChannelProgress {
	ensureChannelProgress(p)
	if p.CurrentIndex >= len(p.Channels) {
		return nil
	}
	return &p.Channels[p.CurrentIndex]
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

func addDeleted(p *job.PurgeProgress, n int) {
	if n <= 0 {
		return
	}
	ch := currentChannelProgress(p)
	if ch == nil {
		return
	}
	ch.Deleted += n
	p.TotalDeleted += n
}

func setPendingDeletes(p *job.PurgeProgress, channelID uint64, ids []snowflake.ID) {
	p.PendingChannelID = channelID
	p.PendingDeleteIDs = make([]uint64, len(ids))
	for i, id := range ids {
		p.PendingDeleteIDs[i] = uint64(id)
	}
}

func clearPendingDeletes(p *job.PurgeProgress) {
	p.PendingChannelID = 0
	p.PendingDeleteIDs = nil
}

func (e *Engine) settlePendingDeletes(ctx context.Context, j *job.PurgeJob, progress *job.PurgeProgress) error {
	if len(progress.PendingDeleteIDs) == 0 {
		return nil
	}

	channelID := progress.PendingChannelID
	if channelID == 0 {
		if progress.CurrentIndex >= len(progress.ChannelIDs) {
			clearPendingDeletes(progress)
			e.saveProgress(ctx, progress)
			return nil
		}
		channelID = progress.ChannelIDs[progress.CurrentIndex]
	}
	cid := snowflake.ID(channelID)

	var retry []snowflake.ID
	var reconciled int
	for _, rawID := range progress.PendingDeleteIDs {
		if e.isCancelled(j.ID) {
			return ErrCancelled
		}
		if ctx.Err() != nil {
			e.saveProgress(ctx, progress)
			return ErrInterrupted
		}

		id := snowflake.ID(rawID)
		if _, err := e.client.Rest.GetMessage(cid, id); err == nil {
			retry = append(retry, id)
		} else if rest.IsJSONErrorCode(err, rest.JSONErrorCodeUnknownMessage) {
			reconciled++
		} else {
			e.logger.Warn("check pending deleted message", zap.Uint64("channel", channelID), zap.Uint64("message", rawID), zap.Error(err))
			retry = append(retry, id)
		}
	}

	addDeleted(progress, reconciled)
	setPendingDeletes(progress, channelID, retry)
	e.saveProgress(ctx, progress)

	for _, id := range retry {
		if e.isCancelled(j.ID) {
			return ErrCancelled
		}
		if ctx.Err() != nil {
			e.saveProgress(ctx, progress)
			return ErrInterrupted
		}

		if err := e.client.Rest.DeleteMessage(cid, id); err == nil || rest.IsJSONErrorCode(err, rest.JSONErrorCodeUnknownMessage) {
			addDeleted(progress, 1)
			e.saveProgress(ctx, progress)
		}
	}

	clearPendingDeletes(progress)
	e.saveProgress(ctx, progress)
	return nil
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

	if !statusJustCreated {
		e.sendInProgress(ctx, j, state, target, locale.MsgPurgeStatusStarting.In(j.Locale), true)
	}

	for progress.CurrentIndex < len(progress.ChannelIDs) {
		if e.isCancelled(j.ID) {
			e.sendCancelled(ctx, j, state, progress.TotalDeleted, showBranding)
			return ErrCancelled
		}
		if ctx.Err() != nil {
			e.saveProgress(ctx, progress)
			return ErrInterrupted
		}

		channelID := progress.ChannelIDs[progress.CurrentIndex]
		chanName := fmt.Sprintf("<#%d>", channelID)
		e.sendInProgress(ctx, j, state, target, locale.MsgPurgeStatusFetching.In(j.Locale, chanName), true)
		e.saveProgress(ctx, progress)

		err := e.purgeChannel(ctx, j, channelID, state, progress)
		if errors.Is(err, ErrCancelled) {
			e.sendCancelled(ctx, j, state, progress.TotalDeleted, showBranding)
			return ErrCancelled
		}
		if errors.Is(err, ErrInterrupted) {
			e.saveProgress(ctx, progress)
			return ErrInterrupted
		}

		ch := currentChannelProgress(progress)
		if ch != nil {
			ch.Done = true
			if err != nil {
				ch.Error = err.Error()
			}
		}
		if err != nil {
			e.logger.Warn("channel purge failed", zap.Uint64("channel", channelID), zap.Error(err))
		}
		progress.BeforeID = 0
		progress.CurrentIndex++
		e.saveProgress(ctx, progress)
	}

	if e.isCancelled(j.ID) {
		e.sendCancelled(ctx, j, state, progress.TotalDeleted, showBranding)
		return ErrCancelled
	}
	if ctx.Err() != nil {
		e.saveProgress(ctx, progress)
		return ErrInterrupted
	}

	elapsed := time.Since(progress.StartedAt)
	e.sendCompletion(ctx, j, state, target, progress.TotalDeleted, elapsed, channelResultsFromProgress(progress), showBranding)

	if err := e.db.RecordPurgeEvent(ctx, database.RecordPurgeEventParams{
		GuildID:    int64(j.GuildID),
		PurgeType:  string(j.PurgeType),
		TargetType: string(j.TargetType),
		Deleted:    progress.TotalDeleted,
		DurationMs: int(elapsed.Milliseconds()),
	}); err != nil {
		e.logger.Warn("record purge event", zap.Error(err))
	}

	return nil
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

// purgeChannel fetches and deletes matching messages from a single channel or thread.
func (e *Engine) purgeChannel(ctx context.Context, j *job.PurgeJob, channelID uint64, state *execState, progress *job.PurgeProgress) error {
	cid := snowflake.ID(channelID)

	for {
		if err := e.settlePendingDeletes(ctx, j, progress); err != nil {
			return err
		}
		if e.isCancelled(j.ID) {
			return ErrCancelled
		}
		if ctx.Err() != nil {
			e.saveProgress(ctx, progress)
			return ErrInterrupted
		}

		beforeID := snowflake.ID(progress.BeforeID)
		messages, err := e.client.Rest.GetMessages(cid, 0, beforeID, 0, fetchBatchSize)
		if err != nil {
			return fmt.Errorf("fetch messages: %w", err)
		}
		if len(messages) == 0 {
			break
		}

		var toDelete []snowflake.ID
		var oldMessages []snowflake.ID
		var atCutoff bool

		for _, msg := range messages {
			if !progress.CutoffAt.IsZero() && msg.CreatedAt.Before(progress.CutoffAt) {
				atCutoff = true
				break
			}
			if state.commandMessageID != 0 && msg.ID == state.commandMessageID {
				continue
			}
			if state.fallbackMessageID != 0 && msg.ID == state.fallbackMessageID {
				continue
			}
			if j.SkipUserID != 0 && msg.Author.ID == snowflake.ID(j.SkipUserID) {
				continue
			}
			if slices.Contains(j.SkipMessageIDs, uint64(msg.ID)) {
				continue
			}
			if !e.matchesJob(ctx, j, msg, state) {
				continue
			}
			if time.Since(msg.CreatedAt) < bulkDeleteMaxAge {
				toDelete = append(toDelete, msg.ID)
			} else {
				oldMessages = append(oldMessages, msg.ID)
			}
		}

		nextBeforeID := progress.BeforeID
		if len(messages) > 0 {
			nextBeforeID = uint64(messages[len(messages)-1].ID)
		}

		for len(toDelete) > 0 {
			if e.isCancelled(j.ID) {
				return ErrCancelled
			}
			if ctx.Err() != nil {
				e.saveProgress(ctx, progress)
				return ErrInterrupted
			}

			batch := toDelete
			if len(batch) > 100 {
				batch = toDelete[:100]
			}
			toDelete = toDelete[len(batch):]
			setPendingDeletes(progress, channelID, batch)
			e.saveProgress(ctx, progress)
			deleted := 0
			if len(batch) == 1 {
				if e.client.Rest.DeleteMessage(cid, batch[0]) == nil {
					deleted = 1
				}
			} else {
				if err := e.client.Rest.BulkDeleteMessages(cid, batch); err == nil {
					deleted = len(batch)
				}
			}
			addDeleted(progress, deleted)
			clearPendingDeletes(progress)
			e.saveProgress(ctx, progress)
		}
		for _, id := range oldMessages {
			if e.isCancelled(j.ID) {
				return ErrCancelled
			}
			if ctx.Err() != nil {
				e.saveProgress(ctx, progress)
				return ErrInterrupted
			}
			setPendingDeletes(progress, channelID, []snowflake.ID{id})
			e.saveProgress(ctx, progress)
			if e.client.Rest.DeleteMessage(cid, id) == nil {
				addDeleted(progress, 1)
			}
			clearPendingDeletes(progress)
			e.saveProgress(ctx, progress)
		}

		progress.BeforeID = nextBeforeID
		e.saveProgress(ctx, progress)

		if atCutoff || len(messages) < fetchBatchSize {
			break
		}
	}

	return nil
}

// matchesJob returns true if the message should be deleted.
func (e *Engine) matchesJob(ctx context.Context, j *job.PurgeJob, msg discord.Message, state *execState) bool {
	guildID := snowflake.ID(j.GuildID)

	switch j.PurgeType {
	case job.PurgeTypeUser:
		if msg.Author.ID != snowflake.ID(j.FilterUserID) {
			return false
		}

	case job.PurgeTypeRole:
		roles, isMember := state.getMemberRoles(ctx, e, guildID, msg.Author.ID)
		if !isMember {
			return false
		}
		hasRole := false
		for _, r := range roles {
			if uint64(r) == j.FilterRoleID {
				hasRole = true
				break
			}
		}
		if !hasRole {
			return false
		}

	case job.PurgeTypeEveryone:
		if !j.IncludeBots && msg.Author.Bot {
			return false
		}

	case job.PurgeTypeBot:
		if !msg.Author.Bot {
			return false
		}

	case job.PurgeTypeInactive:
		if !j.IncludeBots && msg.Author.Bot {
			return false
		}
		_, isMember := state.getMemberRoles(ctx, e, guildID, msg.Author.ID)
		if isMember {
			return false
		}

	case job.PurgeTypeWebhook:
		if msg.WebhookID == nil {
			return false
		}

	case job.PurgeTypeDeleted:
		if msg.Author.Bot {
			return false
		}
		if !strings.HasPrefix(msg.Author.Username, "Deleted User") || msg.Author.Discriminator != "0000" {
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

	var skipped []channelResult
	for _, r := range results {
		if r.err != nil {
			skipped = append(skipped, r)
		}
	}
	if len(skipped) > 0 {
		lines := make([]string, len(skipped))
		for i, r := range skipped {
			lines[i] = locale.MsgPurgeCompleteSkippedLine.In(j.Locale, r.name, r.err.Error())
		}
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
