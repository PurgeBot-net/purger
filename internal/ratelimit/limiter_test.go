package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
	"go.uber.org/zap"
)

type fakeInner struct{}

func (fakeInner) MaxRetries() int                                     { return 0 }
func (fakeInner) Close(context.Context)                               {}
func (fakeInner) Reset()                                              {}
func (fakeInner) Wait(context.Context, *rest.CompiledEndpoint) error  { return nil }
func (fakeInner) Unlock(*rest.CompiledEndpoint, *http.Response) error { return nil }

// Without Via, disgo reads the 429 as a Cloudflare limit and applies it process-wide.
func rateLimited(retryAfter string) *http.Response {
	h := http.Header{}
	h.Set("Via", "1.1 google")
	h.Set("X-RateLimit-Bucket", "abc123")
	h.Set("Retry-After", retryAfter)
	return &http.Response{StatusCode: http.StatusTooManyRequests, Header: h}
}

func deleteEndpoint() *rest.CompiledEndpoint {
	return rest.DeleteMessage.Compile(nil, snowflake.ID(123), snowflake.ID(456))
}

func TestNormaliseRetryAfter(t *testing.T) {
	if delay, header := normaliseRetryAfter("5"); delay != 5*time.Second || header != "5" {
		t.Fatalf("whole seconds: got %v %q", delay, header)
	}
	if delay, header := normaliseRetryAfter("0.75"); delay != time.Second || header != "1" {
		t.Fatalf("fractional retry_after must round up, not fail: got %v %q", delay, header)
	}
	if delay, header := normaliseRetryAfter("soon"); delay != fallbackRetryAfter || header != "3" {
		t.Fatalf("unparseable header should fall back: got %v %q", delay, header)
	}
	if delay, header := normaliseRetryAfter(""); delay != fallbackRetryAfter || header != "3" {
		t.Fatalf("missing header should fall back: got %v %q", delay, header)
	}
}

func TestUnlockNormalisesHeaderInPlace(t *testing.T) {
	l := newWith(fakeInner{}, zap.NewNop())
	rs := rateLimited("0.75")

	if err := l.Unlock(deleteEndpoint(), rs); err != nil {
		t.Fatalf("unlock should never fail a request: %v", err)
	}
	if got := rs.Header.Get("Retry-After"); got != "1" {
		t.Fatalf("header was not normalised for disgo's Atoi: %q", got)
	}
	if got := rs.Header.Get("Via"); got != "1.1 google" {
		t.Fatalf("Via must survive, or disgo treats the limit as process-wide: %q", got)
	}
}

func TestUnlockLeavesSuccessAlone(t *testing.T) {
	l := newWith(fakeInner{}, zap.NewNop())
	rs := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}}

	if err := l.Unlock(deleteEndpoint(), rs); err != nil {
		t.Fatal(err)
	}
	if rs.Header.Get("Retry-After") != "" {
		t.Fatal("a successful response should not gain a Retry-After")
	}
}

func TestUnlockSwallowsInnerErrors(t *testing.T) {
	l := newWith(failingInner{}, zap.NewNop())
	if err := l.Unlock(deleteEndpoint(), rateLimited("5")); err != nil {
		t.Fatalf("a header disgo cannot parse must not fail the delete: %v", err)
	}
}

type failingInner struct{ fakeInner }

func (failingInner) Unlock(*rest.CompiledEndpoint, *http.Response) error {
	return errors.New("no reset or reset after header found")
}

func TestIsTooManyRequests(t *testing.T) {
	limited := &rest.Error{Response: &http.Response{StatusCode: http.StatusTooManyRequests}}
	if !IsTooManyRequests(limited) {
		t.Fatal("429 response should be recognised")
	}
	if !IsTooManyRequests(fmt.Errorf("delete messages: %w", limited)) {
		t.Fatal("a wrapped 429 should still be recognised")
	}
	if IsTooManyRequests(&rest.Error{Code: rest.JSONErrorCodeMissingAccess}) {
		t.Fatal("a permission error is not a rate limit")
	}
	if IsTooManyRequests(nil) {
		t.Fatal("nil is not a rate limit")
	}
	if IsTooManyRequests(errors.New("boom")) {
		t.Fatal("an unrelated error is not a rate limit")
	}
}
