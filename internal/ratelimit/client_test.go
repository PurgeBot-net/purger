package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
	"go.uber.org/zap"
)

// Drives a real disgo client to prove the wrapper is on the request path.
func TestClientSurvivesFractionalRetryAfter(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Via", "1.1 test")
		w.Header().Set("X-RateLimit-Bucket", "abc123")
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "0.05")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	limiter := New(zap.NewNop())
	client := rest.NewClient("token", rest.WithURL(srv.URL), rest.WithRateLimiter(limiter))

	if err := rest.New(client).DeleteMessage(snowflake.ID(123), snowflake.ID(456)); err != nil {
		t.Fatalf("fractional retry_after should be retried, not returned: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected one 429 then one success, got %d calls", got)
	}
}
