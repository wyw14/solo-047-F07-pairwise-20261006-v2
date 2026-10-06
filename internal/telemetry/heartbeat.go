package telemetry

import (
	"errors"
	"time"
)

type AgeEvaluator struct {
	now     func() time.Time
	timeout time.Duration
}

func NewAgeEvaluator(now func() time.Time, timeout time.Duration) (*AgeEvaluator, error) {
	if now == nil || timeout <= 0 {
		return nil, errors.New("clock and positive heartbeat timeout are required")
	}
	return &AgeEvaluator{now: now, timeout: timeout}, nil
}

func (e *AgeEvaluator) Age(seenAt time.Time) time.Duration {
	if seenAt.IsZero() {
		return time.Duration(1<<63 - 1)
	}
	age := e.now().UTC().Sub(seenAt.UTC())
	if age < 0 {
		return 0
	}
	return age
}

func (e *AgeEvaluator) Timeout() time.Duration {
	return e.timeout
}

func (e *AgeEvaluator) Healthy(seenAt time.Time) bool {
	return e.Age(seenAt) <= e.timeout
}
