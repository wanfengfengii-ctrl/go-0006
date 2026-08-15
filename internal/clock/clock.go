// Package clock provides an injectable time abstraction used throughout
// QuotaRaft. All domain logic depends on a Clock so that deterministic,
// time-zone independent tests can be written without real sleeps.
//
// Time is represented as Time, a signed 64-bit count of nanoseconds since the
// Unix epoch. Working in raw int64 nanos (rather than time.Time) keeps the
// refill and cycle calculations purely arithmetic and free of monotonic-clock
// or location surprises.
package clock

import (
	"sync"
	"time"
)

// Time is a point in time expressed as Unix nanoseconds.
type Time int64

// Duration is a span of time expressed in nanoseconds. It mirrors time.Duration
// but is kept distinct so callers do not accidentally mix the two.
type Duration int64

// FromTime converts a time.Time to a Time. The value is normalized to UTC
// nanoseconds so that the result never depends on the process time zone.
func FromTime(t time.Time) Time {
	return Time(t.UTC().UnixNano())
}

// ToTime converts a Time back to a time.Time in UTC.
func (t Time) ToTime() time.Time {
	return time.Unix(0, int64(t)).UTC()
}

// FromDuration converts a time.Duration to a Duration.
func FromDuration(d time.Duration) Duration {
	return Duration(d)
}

// ToDuration converts a Duration to a time.Duration.
func (d Duration) ToDuration() time.Duration {
	return time.Duration(d)
}

// Clock reports the current logical time.
type Clock interface {
	// Now returns the current logical time.
	Now() Time
}

// Wall is a Clock backed by the system wall clock.
type Wall struct{}

// Now returns the current wall-clock time as UTC nanoseconds.
func (Wall) Now() Time {
	return FromTime(time.Now())
}

// Manual is a deterministic Clock whose value is advanced explicitly. It is
// safe for concurrent use. The value never moves on its own, which lets tests
// assert exact refill and expiry behavior without sleeping.
type Manual struct {
	mu sync.Mutex
	t  Time
}

// NewManual creates a Manual clock set to the given time.
func NewManual(t Time) *Manual {
	return &Manual{t: t}
}

// Now returns the current logical time.
func (m *Manual) Now() Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.t
}

// Set moves the clock to exactly t. Setting the clock backwards is rejected by
// the engine; here it is simply recorded so that tests can assert the
// monotonicity invariant at the layer that enforces it.
func (m *Manual) Set(t Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.t = t
}

// Advance moves the clock forward by d. A non-positive duration is a no-op.
func (m *Manual) Advance(d Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d > 0 {
		m.t += Time(d)
	}
}

// AdvanceDuration is a convenience wrapper accepting a time.Duration.
func (m *Manual) AdvanceDuration(d time.Duration) {
	m.Advance(FromDuration(d))
}
