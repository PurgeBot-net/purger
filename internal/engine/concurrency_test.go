package engine

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/PurgeBot-net/common/job"
	"github.com/disgoorg/snowflake/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Rejects every command before it reaches the pool, so no dial and no retry
// backoff happens.
type offlineLimiter struct{}

func (offlineLimiter) Allow() error       { return errors.New("offline") }
func (offlineLimiter) ReportResult(error) {}

// Redis is unreachable on purpose: saveProgress logs the failure and carries on,
// so commit still exercises its locking and its marshal without a live server.
func offlineEngine() *Engine {
	return &Engine{
		logger: zap.NewNop(),
		redis:  redis.NewClient(&redis.Options{Addr: "offline", Limiter: offlineLimiter{}}),
	}
}

func TestCommitSerialisesChannelGoroutines(t *testing.T) {
	const channels, perChannel = 4, 40

	progress := &job.PurgeProgress{ChannelIDs: []uint64{100, 200, 300, 400}}
	ensureChannelProgress(progress)
	state := &execState{progress: progress, excluded: map[snowflake.ID]bool{}}
	e := offlineEngine()

	var wg sync.WaitGroup
	for i := range channels {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := &progress.Channels[i]
			for range perChannel {
				e.commit(context.Background(), state, func() {
					addDeleted(progress, ch, 1)
				})
			}
		}()
	}
	wg.Wait()

	if got := state.totalDeleted(); got != channels*perChannel {
		t.Fatalf("lost updates in the shared total: got %d want %d", got, channels*perChannel)
	}
	for i := range progress.Channels {
		if progress.Channels[i].Deleted != perChannel {
			t.Fatalf("channel %d was credited %d deletions, want %d",
				progress.Channels[i].ChannelID, progress.Channels[i].Deleted, perChannel)
		}
	}
}

func TestMemberCacheConcurrentAccess(t *testing.T) {
	state := &execState{members: map[snowflake.ID]memberLookup{}}
	state.storeMember(1, memberLookup{status: memberStatusPresent})

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range 50 {
				state.storeMember(snowflake.ID(i*100+n), memberLookup{status: memberStatusAbsent})
				state.membersMu.RLock()
				_ = state.members[1]
				state.membersMu.RUnlock()
			}
		}()
	}
	wg.Wait()
}

func TestExcludedSetConcurrentAccess(t *testing.T) {
	state := &execState{excluded: map[snowflake.ID]bool{}}

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range 50 {
				state.exclude(snowflake.ID(i*100 + n))
				state.isExcluded(snowflake.ID(n))
			}
		}()
	}
	wg.Wait()

	// Zero is never a real message ID and must never be treated as excluded, or a
	// channel goroutine would skip a message it should delete.
	if state.isExcluded(0) {
		t.Fatal("zero should never be excluded")
	}
}

func TestInFlightNamesFollowsResolveOrder(t *testing.T) {
	var inFlight sync.Map
	inFlight.Store(uint64(300), true)
	inFlight.Store(uint64(100), true)

	if got := inFlightNames([]uint64{100, 200, 300}, &inFlight); got != "<#100>, <#300>" {
		t.Fatalf("status list should follow resolve order: %q", got)
	}
	if got := inFlightNames([]uint64{100, 200, 300}, &sync.Map{}); got != "" {
		t.Fatalf("nothing in flight should render empty: %q", got)
	}
}
