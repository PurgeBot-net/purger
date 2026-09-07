package engine

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
)

const (
	testGuildID snowflake.ID = 1
	testBotID   snowflake.ID = 2
	testRoleID  snowflake.ID = 3
)

// Skipping over more than the invite asks for would silently under-delete.
func TestRequiredPurgePermsMatchTheAdvertisedInvite(t *testing.T) {
	if requiredPurgePerms != 74752 {
		t.Fatalf("required set drifted from the advertised invite: %#v", requiredPurgePerms)
	}
}

func TestMissingForChannelReturnsZeroWhenFullyPermitted(t *testing.T) {
	if missing := missingForChannel(requiredPurgePerms); missing != 0 {
		t.Fatalf("a fully permitted channel must not be skipped: %#v", missing)
	}
}

func TestMissingForChannelReportsOnlyViewChannelWhenViewIsDenied(t *testing.T) {
	have := discord.PermissionManageMessages | discord.PermissionReadMessageHistory

	missing := missingForChannel(have)
	if missing != discord.PermissionViewChannel {
		t.Fatalf("the other two are granted, so only View Channel is absent: %#v", missing)
	}
}

func TestMissingForChannelReportsAllThreeWhenNothingIsGranted(t *testing.T) {
	missing := missingForChannel(0)
	if missing != requiredPurgePerms {
		t.Fatalf("a bot with no permissions must be told all three at once: %#v", missing)
	}
	if got := permissionNames(missing); got != "View Channel, Read Message History, Manage Messages" {
		t.Fatalf("all three must render, or the admin fixes them one retry at a time: %#v", got)
	}
}

func TestBotGuildPermissionsAdministratorFromEveryoneAloneGrantsAll(t *testing.T) {
	rolePerms := map[snowflake.ID]discord.Permissions{
		testGuildID: discord.PermissionAdministrator,
	}

	perms := botGuildPermissions(testGuildID, nil, rolePerms)
	if !perms.Has(requiredPurgePerms) {
		t.Fatalf("Administrator held through @everyone alone must still grant everything: %#v", perms)
	}
}

func TestBotChannelPermissionsAdministratorIgnoresChannelDenies(t *testing.T) {
	overwrites := discord.PermissionOverwrites{
		discord.RolePermissionOverwrite{RoleID: testGuildID, Deny: requiredPurgePerms},
	}

	perms := botChannelPermissions(discord.PermissionAdministrator, testGuildID, testBotID, nil, overwrites)
	if !perms.Has(requiredPurgePerms) {
		t.Fatalf("a channel overwrite must not be able to revoke Administrator: %#v", perms)
	}
}

// disgo's cache folds the two together, turning this exact setup into an allow.
func TestBotChannelPermissionsMemberDenyBeatsRoleAllow(t *testing.T) {
	overwrites := discord.PermissionOverwrites{
		discord.RolePermissionOverwrite{RoleID: testRoleID, Allow: discord.PermissionManageMessages},
		discord.MemberPermissionOverwrite{UserID: testBotID, Deny: discord.PermissionManageMessages},
	}

	perms := botChannelPermissions(requiredPurgePerms, testGuildID, testBotID, []snowflake.ID{testRoleID}, overwrites)
	if perms.Has(discord.PermissionManageMessages) {
		t.Fatalf("a member deny must survive a role allow: %#v", perms)
	}
	if missing := missingForChannel(perms); missing != discord.PermissionManageMessages {
		t.Fatalf("the denied bit should be the only one reported: %#v", missing)
	}
}

func TestBotChannelPermissionsWithNoOverwritesUsesGuildBase(t *testing.T) {
	perms := botChannelPermissions(requiredPurgePerms, testGuildID, testBotID, []snowflake.ID{testRoleID}, nil)
	if perms != requiredPurgePerms {
		t.Fatalf("a channel with no overwrites must keep the guild base unchanged: %#v", perms)
	}
}

func TestChannelTypeIsThreadCoversEveryThreadType(t *testing.T) {
	threads := []discord.ChannelType{
		discord.ChannelTypeGuildNewsThread,
		discord.ChannelTypeGuildPublicThread,
		discord.ChannelTypeGuildPrivateThread,
	}
	for _, tp := range threads {
		if !channelTypeIsThread(tp) {
			t.Fatalf("thread type not recognised, so its parent overwrites would be ignored: %#v", tp)
		}
	}

	// A forum is a thread's parent, never a thread: treating it as one would read the wrong
	// overwrites for every forum in the guild.
	if channelTypeIsThread(discord.ChannelTypeGuildForum) {
		t.Fatalf("a forum must not be treated as a thread: %#v", discord.ChannelTypeGuildForum)
	}
	if channelTypeIsThread(discord.ChannelTypeGuildText) {
		t.Fatalf("a text channel must not be treated as a thread: %#v", discord.ChannelTypeGuildText)
	}
}

func TestFetchDeniedForChannelMatchesMissingAccessAndMissingPermissions(t *testing.T) {
	if !fetchDeniedForChannel(&rest.Error{Code: rest.JSONErrorCodeMissingAccess}) {
		t.Fatalf("50001 must count as a denial or the raw API string reaches the user")
	}
	if !fetchDeniedForChannel(&rest.Error{Code: rest.JSONErrorCodeLackPermissionsToPerformAction}) {
		t.Fatalf("50013 must count as a denial or the raw API string reaches the user")
	}
	if fetchDeniedForChannel(&rest.Error{Code: rest.JSONErrorCodeUnknownChannel}) {
		t.Fatalf("a deleted channel is not a permission problem")
	}
	if fetchDeniedForChannel(errors.New("connection reset")) {
		t.Fatalf("a transport failure is not a permission problem")
	}
}

func TestPermissionNamesRendersInFixedOrder(t *testing.T) {
	got := permissionNames(requiredPurgePerms)
	if got != "View Channel, Read Message History, Manage Messages" {
		t.Fatalf("permission names are not in the fixed table order: %#v", got)
	}
}

func TestSkippedChannelLinesGroupsChannelsSharingAReason(t *testing.T) {
	reason := "I'm missing required permissions: Manage Messages"
	results := []channelResult{
		{name: "<#10>", err: errors.New(reason)},
		{name: "<#11>", err: errors.New("something else")},
		{name: "<#12>", err: errors.New(reason)},
		{name: "<#13>", deleted: 5},
	}

	lines := skippedChannelLines(results, "en-GB")
	if len(lines) != 2 {
		t.Fatalf("two distinct reasons should produce two lines: %#v", lines)
	}
	if !strings.Contains(lines[0], "<#10>") || !strings.Contains(lines[0], "<#12>") {
		t.Fatalf("channels sharing a reason must share a line: %#v", lines[0])
	}
	if strings.Contains(lines[0], "<#13>") {
		t.Fatalf("a channel that purged successfully must not be listed as skipped: %#v", lines[0])
	}
}

func TestSkippedChannelLinesCapsMentionsWithinAGroup(t *testing.T) {
	reason := "I'm missing required permissions: View Channel"
	results := make([]channelResult, 0, maxSkippedMentions+5)
	for i := range cap(results) {
		results = append(results, channelResult{name: fmt.Sprintf("<#%d>", i), err: errors.New(reason)})
	}

	lines := skippedChannelLines(results, "en-GB")
	if len(lines) != 1 {
		t.Fatalf("one reason should still produce one line: %#v", lines)
	}
	if strings.Count(lines[0], "<#") != maxSkippedMentions {
		t.Fatalf("mentions were not capped: %#v", lines[0])
	}
	if !strings.Contains(lines[0], "...") {
		t.Fatalf("truncation must be visible rather than silent: %#v", lines[0])
	}
}

func TestSkippedChannelLinesCapsTheNumberOfGroups(t *testing.T) {
	results := make([]channelResult, 0, maxSkippedGroups+3)
	for i := range cap(results) {
		results = append(results, channelResult{name: "<#1>", err: fmt.Errorf("reason %d", i)})
	}

	lines := skippedChannelLines(results, "en-GB")
	if len(lines) != maxSkippedGroups+1 {
		t.Fatalf("group count was not capped, or the truncation marker is missing: %#v", len(lines))
	}
	if lines[len(lines)-1] != "..." {
		t.Fatalf("dropped groups must be visible or skipped channels vanish from the report: %#v", lines)
	}
}

// deleteDeniedForChannel matches 50001 too, which is not a Manage Messages problem.
func TestDeniedPermissionDistinguishesMissingAccessFromMissingPermissions(t *testing.T) {
	got := deniedPermission(&rest.Error{Code: rest.JSONErrorCodeMissingAccess})
	if got != discord.PermissionViewChannel {
		t.Fatalf("50001 means the channel is invisible, not that Manage Messages is missing: %#v", got)
	}

	got = deniedPermission(&rest.Error{Code: rest.JSONErrorCodeLackPermissionsToPerformAction})
	if got != discord.PermissionManageMessages {
		t.Fatalf("50013 on a delete means Manage Messages: %#v", got)
	}
}

func TestSkippedChannelLinesReturnsNothingWhenNoChannelFailed(t *testing.T) {
	results := []channelResult{{name: "<#10>", deleted: 3}, {name: "<#11>", deleted: 0}}

	if lines := skippedChannelLines(results, "en-GB"); len(lines) != 0 {
		t.Fatalf("a clean run must not render a skipped section: %#v", lines)
	}
}
