package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Rand defines an interface for random number generation, useful for abstraction and testing.
// It decouples the random number source from concrete implementations like math/rand.
type Rand interface {
	// IntN returns a non-negative pseudo-random integer in [0, n).
	// It panics if n <= 0.
	IntN(n int) int

	// Float64 returns a pseudo-random float64 in [0.0, 1.0).
	Float64() float64
}

// standardRand implements the Rand interface using Go's math/rand package.
// It wraps the random number generator with a mutex to ensure thread safety.
type standardRand struct {
	mu  sync.Mutex
	rng *rand.Rand
}

// NewStandardRand returns a new instance of standardRand seeded with the current time.
// This produces different random sequences on each program execution.
func NewStandardRand() Rand {
	return &standardRand{
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NewStandardRandWithSeed returns a new instance of standardRand seeded with the given value.
// This enables reproducible random sequences, which is useful for testing.
func NewStandardRandWithSeed(seed int64) Rand {
	return &standardRand{
		rng: rand.New(rand.NewSource(seed)),
	}
}

// IntN returns a pseudo-random integer in the range [0, n).
// It panics if n <= 0.
func (sr *standardRand) IntN(n int) int {
	if n <= 0 {
		panic("raft: standardRand.IntN called with n <= 0")
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.rng.Intn(n)
}

// Float64 returns a pseudo-random float64 in the range [0.0, 1.0).
func (sr *standardRand) Float64() float64 {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.rng.Float64()
}
