package raft

import (
	"testing"
	"time"
)

func TestRaftClock_NewStandardClock(t *testing.T) {
	clock := NewStandardClock()

	_, ok := clock.(*standardClock)
	if !ok {
		t.Errorf("Expected *standardClock type, got %T", clock)
	}
}

func TestRaftClock_StandardClock_Now(t *testing.T) {
	clock := NewStandardClock()

	before := time.Now()
	clockNow := clock.Now()
	after := time.Now()

	if clockNow.Before(before) || clockNow.After(after) {
		t.Errorf("Clock.Now() returned time outside of expected bounds")
	}
}

func TestRaftClock_StandardClock_Since(t *testing.T) {
	clock := NewStandardClock()

	pastTime := time.Now().Add(-50 * time.Millisecond)
	time.Sleep(10 * time.Millisecond)

	durationSince := clock.Since(pastTime)
	if durationSince < 50*time.Millisecond {
		t.Errorf("Clock.Since() returned too small duration: %v", durationSince)
	}
}

func TestRaftClock_StandardClock_After(t *testing.T) {
	clock := NewStandardClock()

	start := time.Now()
	ch := clock.After(10 * time.Millisecond)

	<-ch
	elapsed := time.Since(start)

	if elapsed < 10*time.Millisecond {
		t.Errorf("Clock.After() channel received too early")
	}
}

func TestRaftClock_StandardClock_NewTicker(t *testing.T) {
	clock := NewStandardClock()

	ticker := clock.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	_, ok := ticker.(*standardTicker)
	if !ok {
		t.Fatalf("Expected *standardTicker type, got %T", ticker)
	}

	select {
	case <-ticker.Chan():
		// Got at least one tick
	case <-time.After(30 * time.Millisecond):
		t.Error("Ticker did not produce any ticks")
	}
}

func TestRaftClock_StandardTicker_Reset(t *testing.T) {
	clock := NewStandardClock()

	ticker := clock.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	ticker.Reset(10 * time.Millisecond)

	select {
	case <-ticker.Chan():
		// Got the tick within the new period with some tolerance
	case <-time.After(50 * time.Millisecond):
		t.Error("Ticker did not tick after Reset")
	}
}

func TestRaftClock_StandardClock_NewTimer(t *testing.T) {
	clock := NewStandardClock()

	timer := clock.NewTimer(10 * time.Millisecond)

	_, ok := timer.(*standardTimer)
	if !ok {
		t.Fatalf("Expected *standardTimer type, got %T", timer)
	}

	start := time.Now()
	<-timer.Chan()
	elapsed := time.Since(start)

	if elapsed < 10*time.Millisecond {
		t.Errorf("Timer fired too early")
	}
}

func TestRaftClock_StandardTimer_Stop(t *testing.T) {
	clock := NewStandardClock()

	timer := clock.NewTimer(100 * time.Millisecond)

	stopped := timer.Stop()
	if !stopped {
		t.Error("timer.Stop() returned false, expected true for a new timer")
	}

	select {
	case <-timer.Chan():
		t.Error("Timer fired after being stopped")
	case <-time.After(150 * time.Millisecond):
		// The timer did not fire
	}
}

func TestRaftClock_StandardTimer_Reset(t *testing.T) {
	clock := NewStandardClock()

	timer := clock.NewTimer(100 * time.Millisecond)

	resetResult := timer.Reset(10 * time.Millisecond)
	if !resetResult {
		t.Error("timer.Reset() returned false for a new timer")
	}

	select {
	case <-timer.Chan():
		// Timer fired within the new duration with some tolerance
	case <-time.After(50 * time.Millisecond):
		t.Error("Timer did not fire after Reset")
	}
}

func TestRaftClock_StandardClock_Sleep(t *testing.T) {
	clock := NewStandardClock()

	start := time.Now()
	clock.Sleep(10 * time.Millisecond)
	elapsed := time.Since(start)

	if elapsed < 10*time.Millisecond {
		t.Errorf("Clock.Sleep() returned too early")
	}
}
