package lock

import (
	"time"

	"github.com/jathurchan/raftlock/raft"
)

type mockClock struct {
	currentTime time.Time
}

func newMockClock() *mockClock {
	return &mockClock{currentTime: time.Now()}
}

func (m *mockClock) Now() time.Time                  { return m.currentTime }
func (m *mockClock) Since(t time.Time) time.Duration { return m.currentTime.Sub(t) }
func (m *mockClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- m.currentTime.Add(d)
	return ch
}
func (m *mockClock) NewTicker(d time.Duration) raft.Ticker { return nil }
func (m *mockClock) NewTimer(d time.Duration) raft.Timer   { return nil }
func (m *mockClock) Sleep(d time.Duration) {
	m.currentTime = m.currentTime.Add(d)
}
func (m *mockClock) Advance(d time.Duration) {
	m.currentTime = m.currentTime.Add(d)
}
