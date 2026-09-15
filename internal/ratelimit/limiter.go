package ratelimit

import (
	"context"
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

// Bucket waits are the normal state of a bulk deleter; only a long one distinguishes
// a throttled purge from a stuck one.
const slowWaitThreshold = 5 * time.Second

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
	if rs != nil && rs.Header != nil {
		preciseReset(rs.Header)
	}
	if rs != nil && rs.Header != nil && rs.StatusCode == http.StatusTooManyRequests {
		delay, header := normaliseRetryAfter(rs.Header.Get("Retry-After"))
		// Rewritten in place before delegating: the inner Unlock parses this with
		// strconv.Atoi and returns before recording the bucket reset when that fails.
		// Rebuilding the header would drop "via", which disgo reads as a Cloudflare limit.
		rs.Header.Set("Retry-After", header)

		fields := append(endpointFields(endpoint), zap.Duration("retry_after", delay))
		switch {
		case rs.Header.Get("X-RateLimit-Global") != "":
			l.logger.Warn("global rate limit exceeded", fields...)
		case rs.Header.Get("via") == "":
			l.logger.Warn("cloudflare rate limit exceeded", fields...)
		default:
			// scope separates a shared limit from one that spends the invalid-request budget.
			l.logger.Debug("rate limit exceeded", append(fields,
				zap.String("scope", rs.Header.Get("X-RateLimit-Scope")),
				zap.String("bucket", rs.Header.Get("X-RateLimit-Bucket")),
				zap.String("remaining", rs.Header.Get("X-RateLimit-Remaining")),
			)...)
		}
	}

	// The engine reads any error here as a rejected batch and retries all 100
	// messages one by one.
	if err := l.RateLimiter.Unlock(endpoint, rs); err != nil {
		l.logger.Debug("unlock rest bucket", append(endpointFields(endpoint), zap.Error(err))...)
	}
	return nil
}

func (l *Limiter) Wait(ctx context.Context, endpoint *rest.CompiledEndpoint) error {
	start := time.Now()
	err := l.RateLimiter.Wait(ctx, endpoint)
	if waited := time.Since(start); waited >= slowWaitThreshold {
		l.logger.Warn("long rate limit wait", append(endpointFields(endpoint), zap.Duration("waited", waited))...)
	}
	return err
}

// URL and MajorParams have the interaction token substituted in, so only the
// uncompiled route is safe to log.
func endpointFields(endpoint *rest.CompiledEndpoint) []zap.Field {
	if endpoint == nil || endpoint.Endpoint == nil {
		return nil
	}
	return []zap.Field{
		zap.String("method", endpoint.Endpoint.Method),
		zap.String("route", endpoint.Endpoint.Route),
	}
}

// disgo converts reset-after with time.Duration(float64), which truncates: 0.462 becomes a
// zero wait, so the next request fires into a window Discord is still counting and earns a
// 429. Its absolute-reset branch keeps sub-second precision, so the reset goes over that way,
// off the local clock because that is why disgo prefers reset-after in the first place.
func preciseReset(header http.Header) {
	raw := strings.TrimSpace(header.Get("X-RateLimit-Reset-After"))
	if raw == "" {
		return
	}
	resetAfter, err := strconv.ParseFloat(raw, 64)
	if err != nil || resetAfter <= 0 {
		return
	}
	reset := time.Now().Add(time.Duration(resetAfter * float64(time.Second)))
	header.Del("X-RateLimit-Reset-After")
	header.Set("X-RateLimit-Reset", strconv.FormatFloat(float64(reset.UnixNano())/1e9, 'f', 3, 64))
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
