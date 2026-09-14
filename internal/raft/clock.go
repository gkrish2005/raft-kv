package raft

import (
	"sync"
	"time"
)

// Clock is the only source of time used by Raft production logic.
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(time.Duration) bool
}

type realClock struct{}

func (realClock) Now() time.Time                 { return time.Now() }
func (realClock) NewTimer(d time.Duration) Timer { return &realTimer{Timer: time.NewTimer(d)} }

type realTimer struct{ *time.Timer }

func (t *realTimer) C() <-chan time.Time { return t.Timer.C }

func (t *realTimer) Reset(d time.Duration) bool {
	active := t.Timer.Stop()
	if !active {
		select {
		case <-t.Timer.C:
		default:
		}
	}
	t.Timer.Reset(d)
	return active
}

func RealClock() Clock { return realClock{} }

// FakeClock advances only when tests explicitly call Advance.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

func NewFakeClock(start time.Time) *FakeClock { return &FakeClock{now: start} }
func (c *FakeClock) Now() time.Time           { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *FakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{clock: c, ch: make(chan time.Time, 1), deadline: c.now.Add(d), active: true}
	c.timers = append(c.timers, t)
	return t
}
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	timers := append([]*fakeTimer(nil), c.timers...)
	c.mu.Unlock()
	for _, t := range timers {
		t.fire(now)
	}
}

type fakeTimer struct {
	clock    *FakeClock
	mu       sync.Mutex
	ch       chan time.Time
	deadline time.Time
	active   bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }
func (t *fakeTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	was := t.active
	t.active = false
	return was
}
func (t *fakeTimer) Reset(d time.Duration) bool {
	now := t.clock.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	was := t.active
	t.deadline = now.Add(d)
	t.active = true
	select {
	case <-t.ch:
	default:
	}
	return was
}
func (t *fakeTimer) fire(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.active || now.Before(t.deadline) {
		return
	}
	t.active = false
	select {
	case t.ch <- now:
	default:
	}
}
