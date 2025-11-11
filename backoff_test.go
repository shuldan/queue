package queue

import (
	"testing"
	"time"
)

func TestFixedBackoff_Delay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		duration time.Duration
		attempt  int
		expected time.Duration
	}{
		{"zero_duration_zero_attempt", 0, 0, 0},
		{"one_second_zero_attempt", time.Second, 0, time.Second},
		{"one_second_five_attempts", time.Second, 5, time.Second},
		{"five_seconds_ten_attempts", 5 * time.Second, 10, 5 * time.Second},
		{"negative_attempt", time.Second, -1, time.Second},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fb := FixedBackoff{Duration: tt.duration}
			got := fb.Delay(tt.attempt)
			if got != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, got)
			}
		})
	}
}

func TestExponentialBackoff_Delay_NegativeAttempt(t *testing.T) {
	t.Parallel()

	eb := ExponentialBackoff{
		Base:     time.Second,
		MaxDelay: time.Minute,
	}

	got := eb.Delay(-1)
	if got != time.Second {
		t.Errorf("expected %v, got %v", time.Second, got)
	}
}

func TestExponentialBackoff_Delay_AttemptExceedsMax(t *testing.T) {
	t.Parallel()

	eb := ExponentialBackoff{
		Base:     time.Second,
		MaxDelay: time.Minute,
	}

	got := eb.Delay(63)
	if got != time.Minute {
		t.Errorf("expected %v, got %v", time.Minute, got)
	}
}

func TestExponentialBackoff_Delay_DelayExceedsMax(t *testing.T) {
	t.Parallel()

	eb := ExponentialBackoff{
		Base:     time.Second,
		MaxDelay: 10 * time.Second,
	}

	got := eb.Delay(10)
	if got != 10*time.Second {
		t.Errorf("expected %v, got %v", 10*time.Second, got)
	}
}

func TestExponentialBackoff_Delay_NormalCase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		base     time.Duration
		maxDelay time.Duration
		attempt  int
		expected time.Duration
	}{
		{"attempt_0", time.Second, time.Hour, 0, time.Second},
		{"attempt_1", time.Second, time.Hour, 1, 2 * time.Second},
		{"attempt_2", time.Second, time.Hour, 2, 4 * time.Second},
		{"attempt_3", time.Second, time.Hour, 3, 8 * time.Second},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			eb := ExponentialBackoff{Base: tt.base, MaxDelay: tt.maxDelay}
			got := eb.Delay(tt.attempt)
			if got != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, got)
			}
		})
	}
}

func TestExponentialBackoff_Delay_Overflow(t *testing.T) {
	t.Parallel()

	eb := ExponentialBackoff{
		Base:     1 << 62 * time.Nanosecond,
		MaxDelay: time.Hour,
	}

	got := eb.Delay(2)
	if got != time.Hour {
		t.Errorf("expected %v, got %v", time.Hour, got)
	}
}

func TestNoBackoff_Delay(t *testing.T) {
	t.Parallel()

	tests := []int{-100, -1, 0, 1, 100, 1000}

	nb := NoBackoff{}
	for _, attempt := range tests {
		got := nb.Delay(attempt)
		if got != 0 {
			t.Errorf("attempt %d: expected 0, got %v", attempt, got)
		}
	}
}
