package clock

import (
	"testing"
	"time"
)

func TestManualAdvanceAndNow(t *testing.T) {
	c := NewManual(FromTime(time.Unix(0, 0).UTC()))
	if got := c.Now(); got != 0 {
		t.Fatalf("initial time = %d, want 0", got)
	}
	c.AdvanceDuration(time.Second)
	if got := c.Now(); got != Time(time.Second) {
		t.Fatalf("after 1s advance = %d, want %d", got, Time(time.Second))
	}
	c.Advance(0)
	c.Advance(-5)
	if got := c.Now(); got != Time(time.Second) {
		t.Fatalf("non-positive advance should be no-op = %d, want %d", got, Time(time.Second))
	}
}

func TestManualSet(t *testing.T) {
	c := NewManual(100)
	c.Set(500)
	if c.Now() != 500 {
		t.Fatalf("Set did not move clock: %d", c.Now())
	}
}

func TestFromTimeIsUTC(t *testing.T) {
	loc, _ := time.LoadLocation("America/New_York")
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, loc)
	got := FromTime(ts)
	want := Time(ts.UTC().UnixNano())
	if got != want {
		t.Fatalf("FromTime not normalized: got %d want %d", got, want)
	}
}

func TestWallAdvances(t *testing.T) {
	var w Clock = Wall{}
	a := w.Now()
	// Wall clock should be representable and positive for 2026.
	if a < 0 {
		t.Fatalf("wall clock returned negative time %d", a)
	}
	b := w.Now()
	if b < a {
		t.Fatalf("wall clock moved backwards %d -> %d", a, b)
	}
}
