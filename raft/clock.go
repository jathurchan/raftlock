package raft

import "time"

// Clock defines an interface for time-related operations, allowing for testing.
// It abstracts away the standard `time` package.
type Clock interface {
	// Now returns the current local time.
	Now() time.Time

	// Since returns the time elapsed since t (equivalent to Now().Sub(t)).
	// Added for convenience and closer parity with the standard `time` package functions.
	Since(t time.Time) time.Duration

	// After waits for the duration to elapse and then sends the current time
	// on the returned channel. It is equivalent to NewTimer(d).Chan(),
	// but simpler to use for one-off waits.
	After(d time.Duration) <-chan time.Time

	// NewTicker returns a new Ticker containing a channel that will send the
	// time with a period specified by the duration argument.
	// It adjusts the intervals or drops ticks to make up for slow receivers.
	// The duration d must be greater than zero; if not, NewTicker will panic.
	// Stop the ticker to release associated resources.
	NewTicker(d time.Duration) Ticker

	// NewTimer creates a new Timer that will send the current time on its channel
	// after at least duration d.
	NewTimer(d time.Duration) Timer

	// Sleep pauses the current goroutine for at least the duration d.
	// A negative or zero duration causes Sleep to return immediately.
	Sleep(d time.Duration)
}

// Ticker is an interface wrapper around time.Ticker for mocking.
// It holds a channel that delivers "ticks" of a clock at intervals.
type Ticker interface {
	// Chan returns the channel on which the ticks are delivered.
	Chan() <-chan time.Time

	// Stop turns off a ticker. After Stop, no more ticks will be sent.
	// Stop does not close the channel, to prevent a concurrent goroutine
	// reading from the channel from seeing an erroneous "tick".
	Stop()

	// Reset stops a ticker and resets its period to the specified duration.
	// The next tick will arrive after the new period elapsed.
	// The duration d must be greater than zero; if not, Reset will panic.
	Reset(d time.Duration)
}

// Timer is an interface wrapper around time.Timer for mocking.
// It represents a single event. When the Timer expires, the current time
// will be sent on C, unless the Timer was created by AfterFunc.
// A Timer must be created with NewTimer or AfterFunc.
type Timer interface {
	// Chan returns the channel on which the time will be delivered.
	Chan() <-chan time.Time

	// Stop prevents the Timer from firing.
	// It returns true if the call stops the timer, false if the timer has already
	// expired or been stopped.
	// Stop does not close the channel, to prevent a read from the channel succeeding
	// incorrectly.
	Stop() bool

	// Reset changes the timer to expire after duration d.
	// It returns true if the timer had been active, false if the timer had
	// expired or been stopped.
	// For a Timer created with NewTimer, Reset should be invoked only on stopped
	// or expired timers with drained channels.
	Reset(d time.Duration) bool
}

// standardClock implements the Clock interface using the standard Go time package.
type standardClock struct{}

// NewStandardClock returns a Clock implementation based on Go's standard time package.
func NewStandardClock() Clock {
	return &standardClock{}
}

func (sc *standardClock) Now() time.Time {
	return time.Now()
}

func (sc *standardClock) Since(t time.Time) time.Duration {
	return time.Since(t) // Uses time.Now().Sub(t) internally
}

func (sc *standardClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func (sc *standardClock) NewTicker(d time.Duration) Ticker {
	return &standardTicker{ticker: time.NewTicker(d)}
}

func (sc *standardClock) NewTimer(d time.Duration) Timer {
	return &standardTimer{timer: time.NewTimer(d)}
}

func (sc *standardClock) Sleep(d time.Duration) {
	time.Sleep(d)
}

// standardTicker wraps time.Ticker to satisfy the Ticker interface.
type standardTicker struct {
	ticker *time.Ticker
}

func (st *standardTicker) Chan() <-chan time.Time {
	return st.ticker.C
}

func (st *standardTicker) Stop() {
	st.ticker.Stop()
}

func (st *standardTicker) Reset(d time.Duration) {
	st.ticker.Reset(d)
}

// standardTimer wraps time.Timer to satisfy the Timer interface.
type standardTimer struct {
	timer *time.Timer
}

func (st *standardTimer) Chan() <-chan time.Time {
	return st.timer.C
}

func (st *standardTimer) Stop() bool {
	return st.timer.Stop()
}

func (st *standardTimer) Reset(d time.Duration) bool {
	return st.timer.Reset(d)
}
