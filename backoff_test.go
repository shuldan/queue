package queue

import (
	"testing"
	"time"
)

func TestFixedBackoff_Delay_ReturnsFixedDuration(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		duration time.Duration
		attempt  int
	}{
		{"zero", 0, 0},
		{"one_second", time.Second, 5},
		{"negative_attempt", time.Millisecond, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fb := FixedBackoff{Duration: tc.duration}
			got := fb.Delay(tc.attempt)
			if got != tc.duration {
				t.Errorf("expected %v, got %v", tc.duration, got)
			}
		})
	}
}

func TestExponentialBackoff_Delay_AllBranches(t *testing.T) {
	t.Parallel()
	base := time.Millisecond
	maxD := time.Second
	eb := ExponentialBackoff{Base: base, MaxDelay: maxD}

	cases := []struct {
		name    string
		attempt int
		want    time.Duration
	}{
		{"negative_attempt", -1, base},
		{"zero_attempt", 0, base},
		{"attempt_1", 1, 2 * base},
		{"attempt_5", 5, 32 * base},
		{"attempt_over_maxShift", 63, maxD},
		{"attempt_exactly_maxShift", 62, maxD},
		{"overflow_returns_max", 50, maxD},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := eb.Delay(tc.attempt)
			if got != tc.want {
				t.Errorf("attempt=%d: expected %v, got %v", tc.attempt, tc.want, got)
			}
		})
	}
}

func TestExponentialBackoff_Delay_OverflowWrap(t *testing.T) {
	t.Parallel()
	eb := ExponentialBackoff{Base: time.Hour, MaxDelay: 2 * time.Hour}
	got := eb.Delay(62)
	if got != 2*time.Hour {
		t.Errorf("expected max delay %v, got %v", 2*time.Hour, got)
	}
}

func TestNoBackoff_Delay_AlwaysZero(t *testing.T) {
	t.Parallel()
	nb := NoBackoff{}
	for _, attempt := range []int{-1, 0, 1, 100} {
		got := nb.Delay(attempt)
		if got != 0 {
			t.Errorf("attempt=%d: expected 0, got %v", attempt, got)
		}
	}
}
