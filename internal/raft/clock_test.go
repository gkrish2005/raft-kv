package raft

import (
	"testing"
	"time"
)

func TestFakeClockTimerStopResetAndAdvance(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	timer := c.NewTimer(10 * time.Millisecond)
	c.Advance(9 * time.Millisecond)
	select {
	case <-timer.C():
		t.Fatal("timer fired early")
	default:
	}
	if !timer.Stop() {
		t.Fatal("Stop returned false for active timer")
	}
	c.Advance(time.Second)
	select {
	case <-timer.C():
		t.Fatal("stopped timer fired")
	default:
	}
	if timer.Reset(5 * time.Millisecond) {
		t.Fatal("Reset returned true for inactive timer")
	}
	c.Advance(5 * time.Millisecond)
	select {
	case <-timer.C():
	default:
		t.Fatal("reset timer did not fire")
	}
}

func TestFakeClockResetDropsStaleFiring(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	timer := c.NewTimer(5 * time.Millisecond)
	c.Advance(5 * time.Millisecond)
	if timer.Reset(10 * time.Millisecond) {
		t.Fatal("Reset returned true for an already-fired timer")
	}
	select {
	case <-timer.C():
		t.Fatal("stale firing delivered after Reset")
	default:
	}
	c.Advance(10 * time.Millisecond)
	select {
	case <-timer.C():
	default:
		t.Fatal("reset timer did not fire at the new deadline")
	}
}
