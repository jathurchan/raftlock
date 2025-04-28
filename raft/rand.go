package raft

// Rand defines an interface for random number generation, allowing for testing.
// It abstracts away sources like `math/rand`.
type Rand interface {
	// IntN returns, as an int, a non-negative pseudo-random number in [0,n).
	// It panics if n <= 0.
	IntN(n int) int

	// Float64 returns, as a float64, a pseudo-random number in [0.0,1.0).
	Float64() float64
}
