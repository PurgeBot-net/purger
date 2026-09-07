package engine

import (
	"context"
	"strings"
	"time"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
	"go.uber.org/zap"

	"github.com/PurgeBot-net/locale"
)

const (
	// The set the invite asks for (permissions=74752).
	requiredPurgePerms = discord.PermissionViewChannel |
		discord.PermissionReadMessageHistory |
		discord.PermissionManageMessages

	// Re-read rather than snapshotted once: a purge runs for minutes, and a frozen snapshot would
	// miss the mid-run change this check exists to catch.
	botPermsTTL = 60 * time.Second
)

type botPermissions struct {
	rolePerms map[snowflake.ID]discord.Permissions
	roleIDs   []snowflake.ID
	ok        bool
}

type parentLookup struct {
	overwrites discord.PermissionOverwrites
	at         time.Time
	ok         bool
}

// Unlike lookupMember, the REST pair runs under the lock and failures are cached: there is one
// entry, so holding it collapses every channel's first touch into a single fetch, and a failure
// here only disables a guard rather than changing what gets deleted.
func (s *execState) botPermissions(ctx context.Context, e *Engine, guildID snowflake.ID) botPermissions {
	s.botPermsMu.Lock()
	defer s.botPermsMu.Unlock()

	if !s.botPermsAt.IsZero() && time.Since(s.botPermsAt) < botPermsTTL {
		return s.botPerms
	}

	botID := snowflake.ID(e.cfg.ApplicationID)
	roles, rolesErr := e.client.Rest.GetRoles(guildID, rest.WithCtx(ctx))
	member, memberErr := e.client.Rest.GetMember(guildID, botID, rest.WithCtx(ctx))

	s.botPermsAt = time.Now()
	if rolesErr != nil || memberErr != nil || member == nil {
		e.logger.Warn("look up bot permissions",
			zap.Uint64("guild", uint64(guildID)),
			zap.NamedError("roles", rolesErr),
			zap.NamedError("member", memberErr))
		s.botPerms = botPermissions{}
		return s.botPerms
	}

	rolePerms := make(map[snowflake.ID]discord.Permissions, len(roles))
	for _, role := range roles {
		rolePerms[role.ID] = role.Permissions
	}
	s.botPerms = botPermissions{rolePerms: rolePerms, roleIDs: member.RoleIDs, ok: true}
	return s.botPerms
}

func (s *execState) channelOverwrites(ctx context.Context, e *Engine, channelID snowflake.ID) parentLookup {
	s.parentMu.RLock()
	cached, hit := s.parentOverwrites[channelID]
	s.parentMu.RUnlock()
	// Same TTL as the role snapshot: a parent's overwrites can change mid-job too.
	if hit && time.Since(cached.at) < botPermsTTL {
		return cached
	}

	result := parentLookup{at: time.Now()}
	if ch, err := e.client.Rest.GetChannel(channelID, rest.WithCtx(ctx)); err == nil {
		if gc, isGuild := ch.(discord.GuildChannel); isGuild {
			result = parentLookup{overwrites: gc.PermissionOverwrites(), at: time.Now(), ok: true}
		}
	}

	s.parentMu.Lock()
	s.parentOverwrites[channelID] = result
	s.parentMu.Unlock()
	return result
}

// Fails open on every lookup failure: a channel that would have purged must not be skipped because
// Discord was briefly unreachable, and the delete path still rejects a real denial.
func (e *Engine) missingPermissionsForChannel(ctx context.Context, state *execState, guildID, channelID snowflake.ID) discord.Permissions {
	bot := state.botPermissions(ctx, e, guildID)
	if !bot.ok {
		return 0
	}

	ch, err := e.client.Rest.GetChannel(channelID, rest.WithCtx(ctx))
	if err != nil {
		return 0
	}
	gc, isGuild := ch.(discord.GuildChannel)
	if !isGuild {
		return 0
	}

	overwrites := gc.PermissionOverwrites()
	if channelTypeIsThread(gc.Type()) {
		// A thread carries no overwrites of its own; it inherits the parent's.
		parentID := gc.ParentID()
		if parentID == nil || *parentID == 0 {
			return 0
		}
		parent := state.channelOverwrites(ctx, e, *parentID)
		if !parent.ok {
			return 0
		}
		overwrites = parent.overwrites
	}

	base := botGuildPermissions(guildID, bot.roleIDs, bot.rolePerms)
	have := botChannelPermissions(base, guildID, snowflake.ID(e.cfg.ApplicationID), bot.roleIDs, overwrites)
	return missingForChannel(have)
}

func channelTypeIsThread(t discord.ChannelType) bool {
	switch t {
	case discord.ChannelTypeGuildNewsThread, discord.ChannelTypeGuildPublicThread, discord.ChannelTypeGuildPrivateThread:
		return true
	default:
		return false
	}
}

// Duplicated verbatim in interactions/internal/commands/permissions.go, like canCancel, so neither
// service needs a common release. The tests are named identically in both so a diff shows drift.

func botGuildPermissions(guildID snowflake.ID, botRoleIDs []snowflake.ID, rolePerms map[snowflake.ID]discord.Permissions) discord.Permissions {
	perms := rolePerms[guildID]
	for _, id := range botRoleIDs {
		perms |= rolePerms[id]
	}
	if perms.Has(discord.PermissionAdministrator) {
		return discord.PermissionsAll
	}
	return perms
}

func botChannelPermissions(base discord.Permissions, guildID, botID snowflake.ID, botRoleIDs []snowflake.ID, overwrites discord.PermissionOverwrites) discord.Permissions {
	if base.Has(discord.PermissionAdministrator) {
		return discord.PermissionsAll
	}

	perms := base
	if ow, ok := overwrites.Role(guildID); ok {
		perms &^= ow.Deny
		perms |= ow.Allow
	}

	var allow, deny discord.Permissions
	for _, id := range botRoleIDs {
		if id == guildID {
			continue
		}
		if ow, ok := overwrites.Role(id); ok {
			allow |= ow.Allow
			deny |= ow.Deny
		}
	}
	perms &^= deny
	perms |= allow

	// Its own final layer, not folded in with the role ones: a deny aimed at the bot must beat an
	// allow it inherits from a role.
	if ow, ok := overwrites.Member(botID); ok {
		perms &^= ow.Deny
		perms |= ow.Allow
	}
	return perms
}

// Every absent bit, so a bot with nothing is told all three at once rather than one per retry.
func missingForChannel(have discord.Permissions) discord.Permissions {
	return requiredPurgePerms &^ have
}

var permissionNameTable = []struct {
	bit  discord.Permissions
	name string
}{
	{discord.PermissionViewChannel, "View Channel"},
	{discord.PermissionReadMessageHistory, "Read Message History"},
	{discord.PermissionManageMessages, "Manage Messages"},
}

// discord.Permissions.String ranges a map, so its order varies between calls.
func permissionNameList(p discord.Permissions) []string {
	names := make([]string, 0, len(permissionNameTable))
	for _, entry := range permissionNameTable {
		if p.Has(entry.bit) {
			names = append(names, entry.name)
		}
	}
	return names
}

func permissionNames(p discord.Permissions) string {
	return strings.Join(permissionNameList(p), ", ")
}

const (
	maxSkippedGroups   = 10
	maxSkippedMentions = 20
)

// Grouped and capped: Discord refuses an oversized components-v2 message, and updateComponents
// only logs that, leaving the user on the last in-progress render with no completion at all.
func skippedChannelLines(results []channelResult, lang string) []string {
	var order []string
	grouped := make(map[string][]string)
	for _, r := range results {
		if r.err == nil {
			continue
		}
		reason := r.err.Error()
		if _, seen := grouped[reason]; !seen {
			order = append(order, reason)
		}
		grouped[reason] = append(grouped[reason], r.name)
	}

	truncated := len(order) > maxSkippedGroups
	if truncated {
		order = order[:maxSkippedGroups]
	}

	lines := make([]string, 0, len(order)+1)
	for _, reason := range order {
		names := grouped[reason]
		if len(names) > maxSkippedMentions {
			// Its own entry: appended to the reason it would read as another permission name.
			names = append(names[:maxSkippedMentions:maxSkippedMentions], "...")
		}
		lines = append(lines, locale.MsgPurgeCompleteSkippedLine.In(lang, strings.Join(names, " "), reason))
	}
	// Silently dropping the rest would report a clean run for channels that were skipped.
	if truncated {
		lines = append(lines, "...")
	}
	return lines
}

// 50001 means the channel is invisible, not that Manage Messages is missing.
func deniedPermission(err error) discord.Permissions {
	if rest.IsJSONErrorCode(err, rest.JSONErrorCodeMissingAccess) {
		return discord.PermissionViewChannel
	}
	return discord.PermissionManageMessages
}

// The fetch path had no denial predicate, so a 403 there reached the user as a raw disgo string.
func fetchDeniedForChannel(err error) bool {
	return rest.IsJSONErrorCode(err,
		rest.JSONErrorCodeMissingAccess,
		rest.JSONErrorCodeLackPermissionsToPerformAction,
	)
}
