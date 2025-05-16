package raft

import "testing"

func TestNewStandardRand(t *testing.T) {
	r := NewStandardRand()
	if r == nil {
		t.Fatal("NewStandardRand() returned nil")
	}

	intVal := r.IntN(10)
	if intVal < 0 || intVal >= 10 {
		t.Errorf("IntN(10) returned %d, which is outside range [0, 10)", intVal)
	}

	floatVal := r.Float64()
	if floatVal < 0.0 || floatVal >= 1.0 {
		t.Errorf("Float64() returned %f, which is outside range [0.0, 1.0)", floatVal)
	}
}

func TestNewStandardRandWithSeed(t *testing.T) {
	r1 := NewStandardRandWithSeed(42)
	r2 := NewStandardRandWithSeed(42)

	v1 := r1.IntN(100)
	v2 := r2.IntN(100)
	if v1 != v2 {
		t.Errorf("Expected same values for same seed, got %d and %d", v1, v2)
	}
}

func TestIntN(t *testing.T) {
	r := NewStandardRandWithSeed(123)

	for i := 1; i <= 5; i++ {
		val := r.IntN(i)
		if val < 0 || val >= i {
			t.Errorf("IntN(%d) returned %d, which is outside range [0, %d)", i, val, i)
		}
	}

	defer func() {
		if recover() == nil {
			t.Error("IntN(0) did not panic as expected")
		}
	}()
	r.IntN(0)
}

func TestFloat64(t *testing.T) {
	r := NewStandardRandWithSeed(456)

	for range 5 {
		val := r.Float64()
		if val < 0.0 || val >= 1.0 {
			t.Errorf("Float64() returned %f, which is outside range [0.0, 1.0)", val)
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	r := NewStandardRand()

	done := make(chan bool)
	go func() {
		for range 100 {
			r.IntN(100)
		}
		done <- true
	}()

	go func() {
		for range 100 {
			r.Float64()
		}
		done <- true
	}()

	<-done
	<-done
}
