package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jathurchan/raftlock/client"
)

const (
	appName    = "RaftLock Payment Processor"
	appVersion = "v1.0.0"
)

// PaymentConfig holds the configuration for the payment processing example.
type PaymentConfig struct {
	// Connection settings
	ServerEndpoints []string

	// Payment details
	PaymentID string
	ClientID  string
	Amount    float64
	Currency  string

	// Lock behavior
	TTL        time.Duration
	Wait       bool
	MaxRetries int
	RetryDelay time.Duration

	// Processing simulation
	ProcessingTime time.Duration
	FailureRate    float64 // Probability of simulated failure (0.0 to 1.0)

	// Display options
	Verbose     bool
	ShowVersion bool
}

// PaymentProcessor handles the distributed payment processing logic.
type PaymentProcessor struct {
	config *PaymentConfig
	client client.RaftLockClient
	logger *log.Logger
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("❌ Error: %v", err)
	}
}

func run() error {
	config, err := parseFlags()
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}

	if config.ShowVersion {
		fmt.Printf("%s %s\n", appName, appVersion)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n🛑 Received shutdown signal, canceling payment...")
		cancel()
	}()

	processor, err := NewPaymentProcessor(config)
	if err != nil {
		return fmt.Errorf("failed to initialize payment processor: %w", err)
	}
	defer processor.Close()

	return processor.ProcessPayment(ctx)
}

// NewPaymentProcessor creates a new payment processor with the given configuration.
func NewPaymentProcessor(config *PaymentConfig) (*PaymentProcessor, error) {
	clientConfig := client.DefaultClientConfig()
	clientConfig.Endpoints = config.ServerEndpoints
	clientConfig.RequestTimeout = 30 * time.Second
	clientConfig.EnableMetrics = true

	raftClient, err := client.NewRaftLockClient(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create RaftLock client: %w", err)
	}

	logger := log.New(os.Stdout, "", log.LstdFlags)
	if !config.Verbose {
		logger.SetOutput(os.Stderr)
	}

	return &PaymentProcessor{
		config: config,
		client: raftClient,
		logger: logger,
	}, nil
}

// Close cleans up resources used by the payment processor.
func (p *PaymentProcessor) Close() {
	if err := p.client.Close(); err != nil {
		p.logger.Printf("⚠️  Warning: failed to close RaftLock client: %v", err)
	}
}

// ProcessPayment executes the payment processing workflow with distributed locking.
func (p *PaymentProcessor) ProcessPayment(ctx context.Context) error {
	cfg := p.config

	fmt.Printf("🏦 %s\n", appName)
	fmt.Printf("%s\n", strings.Repeat("=", len(appName)+2))
	fmt.Printf("  Payment ID:   %s\n", cfg.PaymentID)
	fmt.Printf("  Client ID:    %s\n", cfg.ClientID)
	fmt.Printf("  Amount:       %.2f %s\n", cfg.Amount, cfg.Currency)
	fmt.Printf("  Lock TTL:     %v\n", cfg.TTL)
	fmt.Printf("  Wait Mode:    %t\n", cfg.Wait)
	fmt.Printf("  Endpoints:    %s\n", strings.Join(cfg.ServerEndpoints, ", "))
	fmt.Printf("  Max Retries:  %d\n", cfg.MaxRetries)
	fmt.Printf("  Retry Delay:  %v\n", cfg.RetryDelay)
	fmt.Printf("  Sim. Proc. Time: %v\n", cfg.ProcessingTime)
	fmt.Printf("  Sim. Fail Rate: %.2f%%\n", cfg.FailureRate*100)
	fmt.Println()

	lockID := fmt.Sprintf("payment:%s", cfg.PaymentID)
	handle, err := client.NewLockHandle(p.client, lockID, cfg.ClientID)
	if err != nil {
		return fmt.Errorf("failed to create lock handle: %w", err)
	}
	defer p.safeReleaseLock(handle, lockID)

	return p.processPaymentWithRetries(ctx, handle, lockID)
}

// processPaymentWithRetries implements retry logic for payment processing.
func (p *PaymentProcessor) processPaymentWithRetries(
	ctx context.Context,
	handle client.LockHandle,
	lockID string,
) error {
	var lastErr error

	for attempt := 1; attempt <= p.config.MaxRetries; attempt++ {
		if attempt > 1 {
			fmt.Printf("\n🔄 Retry attempt %d/%d for '%s'...\n", attempt, p.config.MaxRetries, lockID)

			select {
			case <-time.After(p.config.RetryDelay):
			case <-ctx.Done():
				return fmt.Errorf("operation canceled during retry delay: %w", ctx.Err())
			}
		}

		err := p.attemptPaymentProcessing(ctx, handle, lockID, attempt)
		if err == nil {
			return nil
		}

		lastErr = err

		if !p.isRetryableError(err) {
			p.logger.Printf("Non-retryable error encountered for '%s': %v", lockID, err)
			break // Do not retry for non-retryable errors
		}

		if attempt < p.config.MaxRetries {
			fmt.Printf("⏰ Payment processing failed (attempt %d): %v\n", attempt, err)
		}
	}

	return fmt.Errorf("payment processing failed after %d attempts: %w",
		p.config.MaxRetries, lastErr)
}

// attemptPaymentProcessing performs a single payment processing attempt.
func (p *PaymentProcessor) attemptPaymentProcessing(
	ctx context.Context,
	handle client.LockHandle,
	lockID string,
	attempt int,
) error {
	// Step 1: Acquire the distributed lock
	fmt.Printf("🔒 [Attempt %d] Acquiring lock '%s'...\n", attempt, lockID)

	acquireStart := time.Now()
	err := handle.Acquire(ctx, p.config.TTL, p.config.Wait)
	if err != nil {
		if errors.Is(err, client.ErrLockHeld) {
			if p.config.Wait {
				return fmt.Errorf("timed out waiting for lock (another payment in progress)")
			}
			return fmt.Errorf("payment '%s' is already being processed by another client",
				p.config.PaymentID)
		}

		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	acquireDuration := time.Since(acquireStart)
	fmt.Printf("✅ Lock acquired in %v (current ownership)\n", acquireDuration.Round(time.Millisecond))

	// Step 2: Display lock information if verbose mode is enabled
	if p.config.Verbose {
		p.displayLockInfo(handle)
	}

	// Step 3: Simulate actual payment processing work
	fmt.Printf("💳 Processing payment of %.2f %s for '%s'...\n", p.config.Amount, p.config.Currency, p.config.PaymentID)

	err = p.simulatePaymentProcessing(ctx)
	if err != nil {
		fmt.Printf("❌ Payment processing failed for '%s': %v\n", p.config.PaymentID, err)
		return err // Return the error to trigger retry logic if applicable
	}

	// Step 4: Release the lock after successful processing
	fmt.Printf("🔓 Payment for '%s' completed successfully, releasing lock...\n", p.config.PaymentID)
	err = handle.Release(ctx)
	if err != nil {
		if errors.Is(err, client.ErrNotLockOwner) {
			return fmt.Errorf("lost ownership of lock '%s' during processing (possible TTL expiration)", lockID)
		}
		return fmt.Errorf("failed to release lock '%s': %w", lockID, err)
	}

	fmt.Printf("✅ Payment '%s' processed and lock released successfully!\n", p.config.PaymentID)
	return nil
}

// simulatePaymentProcessing simulates the actual payment processing work.
func (p *PaymentProcessor) simulatePaymentProcessing(ctx context.Context) error {
	processingTime := p.config.ProcessingTime
	if processingTime == 0 {
		processingTime = time.Duration(1000+rand.Intn(2000)) * time.Millisecond
	}

	if processingTime > 2*time.Second && p.config.Verbose {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		done := make(chan struct{})
		go func() {
			defer close(done)
			time.Sleep(processingTime)
		}()

		dots := 0
		for {
			select {
			case <-done:
				fmt.Println()
				goto processing_complete
			case <-ticker.C:
				fmt.Print(".")
				dots++
				if dots%10 == 0 {
					fmt.Print(" ")
				}
			case <-ctx.Done():
				fmt.Println()
				return fmt.Errorf("payment processing canceled: %w", ctx.Err())
			}
		}
	} else {
		select {
		case <-time.After(processingTime):
		case <-ctx.Done():
			return fmt.Errorf("payment processing canceled: %w", ctx.Err())
		}
	}

processing_complete:
	if p.config.FailureRate > 0 && rand.Float64() < p.config.FailureRate {
		return errors.New("simulated payment gateway error")
	}

	return nil
}

// displayLockInfo shows information about the acquired lock.
func (p *PaymentProcessor) displayLockInfo(handle client.LockHandle) {
	lock := handle.Lock()
	if lock != nil {
		fmt.Printf("🔍 Lock Details:\n")
		fmt.Printf("  Version:    %d\n", lock.Version)
		fmt.Printf("  Acquired At: %v\n", lock.AcquiredAt.Format(time.RFC3339))
		fmt.Printf("  Expires At:  %v\n", lock.ExpiresAt.Format(time.RFC3339))
		fmt.Printf("  Time Left:   %v\n", time.Until(lock.ExpiresAt).Round(time.Second))

		if len(lock.Metadata) > 0 {
			fmt.Printf("  Metadata:    %v\n", lock.Metadata)
		}
		fmt.Println()
	}
}

// safeReleaseLock safely releases a lock handle with error handling on defer.
func (p *PaymentProcessor) safeReleaseLock(handle client.LockHandle, lockID string) {
	if !handle.IsHeld() {
		return // Lock is not held, nothing to release
	}

	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := handle.Close(releaseCtx); err != nil {
		p.logger.Printf("⚠️  Warning: failed to release lock '%s' on shutdown: %v", lockID, err)
	} else if p.config.Verbose {
		fmt.Printf("🔓 Lock '%s' released successfully on defer\n", lockID)
	}
}

// isRetryableError determines if an error should trigger a retry.
func (p *PaymentProcessor) isRetryableError(err error) bool {
	if errors.Is(err, client.ErrNotLockOwner) ||
		errors.Is(err, client.ErrVersionMismatch) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	return true
}

// parseFlags parses command-line flags and returns a populated PaymentConfig.
func parseFlags() (*PaymentConfig, error) {
	config := &PaymentConfig{
		ServerEndpoints: []string{"localhost:8080"},
		TTL:             30 * time.Second,
		MaxRetries:      3,
		RetryDelay:      2 * time.Second,
		ProcessingTime:  3 * time.Second,
		FailureRate:     0.0,
		Currency:        "USD",
		Amount:          100.00,
	}

	var (
		serverAddrs  string
		ttlSeconds   int
		retryDelayMs int
		processingMs int
	)

	flag.StringVar(&serverAddrs, "servers", "localhost:8080",
		"Comma-separated list of RaftLock server addresses")
	flag.StringVar(&config.PaymentID, "payment-id", "",
		"Unique identifier for the payment transaction (required)")
	flag.StringVar(&config.ClientID, "client-id", "",
		"Unique identifier for this client (required)")
	flag.Float64Var(&config.Amount, "amount", config.Amount,
		"Payment amount")
	flag.StringVar(&config.Currency, "currency", config.Currency,
		"Payment currency")
	flag.IntVar(&ttlSeconds, "ttl", int(config.TTL.Seconds()),
		"Lock time-to-live in seconds")
	flag.BoolVar(&config.Wait, "wait", false,
		"Wait for lock if currently held by another client")
	flag.IntVar(&config.MaxRetries, "max-retries", config.MaxRetries,
		"Maximum number of retry attempts")
	flag.IntVar(&retryDelayMs, "retry-delay", int(config.RetryDelay.Milliseconds()),
		"Delay between retry attempts in milliseconds")
	flag.IntVar(&processingMs, "processing-time", int(config.ProcessingTime.Milliseconds()),
		"Simulated payment processing time in milliseconds (0 for random)")
	flag.Float64Var(&config.FailureRate, "failure-rate", config.FailureRate,
		"Simulated failure rate (0.0 to 1.0)")
	flag.BoolVar(&config.Verbose, "verbose", false,
		"Enable verbose output")
	flag.BoolVar(&config.ShowVersion, "version", false,
		"Show version and exit")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s %s\n\n", appName, appVersion)
		fmt.Fprintf(os.Stderr, "USAGE:\n")
		fmt.Fprintf(os.Stderr, "  %s [OPTIONS]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "EXAMPLES:\n")
		fmt.Fprintf(os.Stderr, "  # Basic payment processing\n")
		fmt.Fprintf(os.Stderr, "  %s --payment-id payment123 --client-id client001\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Wait for lock with custom amount and higher failure rate\n")
		fmt.Fprintf(os.Stderr, "  %s --payment-id payment456 --client-id client002 --wait --amount 250.50 --failure-rate 0.2\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Connect to multiple servers with verbose output and longer processing time\n")
		fmt.Fprintf(os.Stderr, "  %s --servers localhost:8080,localhost:8081,localhost:8082 --payment-id payment789 --client-id client003 --verbose --processing-time 5000\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if config.ShowVersion {
		return config, nil
	}

	if config.PaymentID == "" {
		return nil, errors.New("--payment-id is required")
	}
	if config.ClientID == "" {
		return nil, errors.New("--client-id is required")
	}
	if config.Amount <= 0 {
		return nil, errors.New("--amount must be positive")
	}
	if ttlSeconds <= 0 {
		return nil, errors.New("--ttl must be positive")
	}
	if config.FailureRate < 0 || config.FailureRate > 1 {
		return nil, errors.New("--failure-rate must be between 0.0 and 1.0")
	}

	if serverAddrs != "" {
		config.ServerEndpoints = strings.Split(serverAddrs, ",")
		for i, addr := range config.ServerEndpoints {
			config.ServerEndpoints[i] = strings.TrimSpace(addr)
		}
	}

	config.TTL = time.Duration(ttlSeconds) * time.Second
	config.RetryDelay = time.Duration(retryDelayMs) * time.Millisecond
	config.ProcessingTime = time.Duration(processingMs) * time.Millisecond

	return config, nil
}
