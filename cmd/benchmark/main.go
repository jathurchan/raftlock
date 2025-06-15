package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const (
	exitSuccess     = 0
	exitFailure     = 1
	exitInterrupted = 130 // Exit code for SIGINT or SIGTERM
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("🛑 Received signal %v, initiating graceful shutdown...", sig)
		cancel()
	}()

	cfg, err := parseConfig()
	if err != nil {
		log.Printf("❌ Configuration error: %v", err)
		os.Exit(exitFailure)
	}

	if err := cfg.Validate(); err != nil {
		log.Printf("❌ Invalid configuration: %v", err)
		os.Exit(exitFailure)
	}

	suite, err := newBenchmarkSuite(cfg)
	if err != nil {
		log.Fatalf("❌ Initialization failed: %v", err)
	}

	defer func() {
		if r := recover(); r != nil {
			log.Printf("❌ Panic during benchmark: %v", r)
			suite.cleanup()
			os.Exit(exitFailure)
		}
		suite.cleanup()
	}()

	suite.printBanner()

	if err := suite.runWithContext(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			log.Printf("🛑 Benchmark canceled by user")
			os.Exit(exitInterrupted)
		}
		log.Printf("❌ Benchmark failed: %v", err)
		os.Exit(exitFailure)
	}

	log.Printf("✅ Benchmark completed successfully")
	os.Exit(exitSuccess)
}
