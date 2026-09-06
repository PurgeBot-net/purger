package ratelimit

import (
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/disgoorg/disgo/rest"
	"go.uber.org/zap"
)

// Used when Discord's Retry-After is missing or unparseable.
const fallbackRetryAfter = 3 * time.Second

var _ rest.RateLimiter = (*Limiter)(nil)

// Limiter keeps disgo's 429 warning off stderr: the inner limiter gets a
// discarding logger, because its own warning goes to slog.Default() and so
// bypasses LOG_LEVEL and LOG_JSON. Waiting out retry_after and retrying stays
// disgo's job, unchanged.
type Limiter struct {
	rest.RateLimiter

	logger *zap.Logger
}

func New(logger *zap.Logger) *Limiter {
	return newWith(rest.NewRateLimiter(rest.WithRateLimiterLogger(slog.New(slog.DiscardHandler))), logger)
}

func newWith(inner rest.RateLimiter, logger *zap.Logger) *Limiter {
	return &Limiter{RateLimiter: inner, logger: logger}
}

func (l *Limiter) Unlock(endpoint *rest.CompiledEndpoint, rs *http.Response) error {
	if rs != nil && rs.Header != nil && rs.StatusCode == http.StatusTooManyRequests {
		delay, header := normaliseRetryAfter(rs.Header.Get("Retry-After"))
		// Rewritten in place before delegating: the inner Unlock parses this with
		// strconv.Atoi and returns before recording the bucket reset when that fails.
		// Rebuilding the header would drop "via", which disgo reads as a Cloudflare limit.
		rs.Header.Set("Retry-After", header)

		switch {
		case rs.Header.Get("X-RateLimit-Global") != "":
			l.logger.Warn("global rate limit exceeded", zap.Duration("retry_after", delay))
		case rs.Header.Get("via") == "":
			l.logger.Warn("cloudflare rate limit exceeded", zap.Duration("retry_after", delay))
		default:
			l.logger.Debug("rate limit exceeded",
				zap.String("endpoint", endpoint.URL), zap.Duration("retry_after", delay))
		}
	}

	// The engine reads any error here as a rejected batch and retries all 100
	// messages one by one.
	if err := l.RateLimiter.Unlock(endpoint, rs); err != nil {
		l.logger.Debug("unlock rest bucket", zap.Error(err))
	}
	return nil
}

// Discord may answer with a fractional retry_after, which disgo's strconv.Atoi rejects.
func normaliseRetryAfter(raw string) (time.Duration, string) {
	seconds, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || seconds <= 0 {
		return fallbackRetryAfter, headerSeconds(fallbackRetryAfter)
	}
	delay := time.Duration(math.Ceil(seconds)) * time.Second
	return delay, headerSeconds(delay)
}

// disgo treats a reset that is not in the future as no reset at all, so this never
// goes below a second.
func headerSeconds(d time.Duration) string {
	return strconv.Itoa(max(int(math.Ceil(d.Seconds())), 1))
}

// A 429 body carries no error code, so rest.IsJSONErrorCode cannot see it.
func IsTooManyRequests(err error) bool {
	var restErr *rest.Error
	if !errors.As(err, &restErr) {
		return false
	}
	return restErr.Response != nil && restErr.Response.StatusCode == http.StatusTooManyRequests
}
