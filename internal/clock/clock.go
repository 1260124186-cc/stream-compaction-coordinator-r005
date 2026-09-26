package clock

import (
	"sync"
	"time"
)

// Clock isolates time-dependent behavior from application transitions.
type Clock interface {
	Now() time.Time
}

// System reads the host wall clock.
type System struct{}

func (System) Now() time.Time {
	return time.Now().UTC()
}

// Fixed is useful for deterministic runtime checks.
type Fixed struct {
	mu      sync.Mutex
	current time.Time
	step    time.Duration
}

func NewFixed(start time.Time, step time.Duration) *Fixed {
	return &Fixed{
		current: start.UTC(),
		step:    step,
	}
}

func (f *Fixed) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()

	value := f.current
	if f.step > 0 {
		f.current = f.current.Add(f.step)
	}
	return value
}

func (f *Fixed) Advance(delta time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.current = f.current.Add(delta)
}
