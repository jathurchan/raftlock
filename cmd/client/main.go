package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jathurchan/raftlock/client"
	"github.com/spf13/cobra"
)

const version = "1.0.0"

// globalOpts stores global flags and the initialized RaftLock client.
// It provides a convenient way to pass both configuration options
// and a live client instance to subcommands.
type globalOpts struct {
	endpoints      string
	timeout        time.Duration
	dialTimeout    time.Duration
	requestTimeout time.Duration
	maxRetries     int
	verbose        bool

	// raftClient is initialized in PersistentPreRunE and shared across commands.
	raftClient client.RaftLockClient
}

// main is the entry point of the application.
// It sets up and executes the root Cobra command.
func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

// newRootCmd constructs the root Cobra command for the RaftLock client CLI.
// It configures global flags, sets up shared resources, and attaches all
// subcommands that implement client functionality.
func newRootCmd() *cobra.Command {
	opts := &globalOpts{}

	rootCmd := &cobra.Command{
		Use:   "raftlock-client",
		Short: "A command-line client for the RaftLock distributed lock service.",
		Long: `RaftLock Client CLI provides commands to interact with a RaftLock cluster,
allowing you to acquire, release, renew, and inspect distributed locks.`,
		Version: version,

		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			endpoints := strings.Split(opts.endpoints, ",")
			for i := range endpoints {
				endpoints[i] = strings.TrimSpace(endpoints[i])
			}

			builder := client.NewRaftLockClientBuilder(endpoints).
				WithTimeouts(opts.dialTimeout, opts.requestTimeout).
				WithRetryOptions(opts.maxRetries, 100*time.Millisecond, 5*time.Second, 2.0)

			c, err := builder.Build()
			if err != nil {
				return fmt.Errorf("failed to build client: %w", err)
			}
			opts.raftClient = c

			ctx := context.WithValue(cmd.Context(), "globalOpts", opts)
			cmd.SetContext(ctx)
			return nil
		},

		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			if opts.raftClient != nil {
				opts.raftClient.Close()
			}
		},
	}

	pflags := rootCmd.PersistentFlags()
	pflags.StringVarP(&opts.endpoints, "endpoints", "e", "localhost:8080", "Comma-separated list of server endpoints")
	pflags.DurationVar(&opts.timeout, "timeout", 30*time.Second, "Overall operation timeout")
	pflags.DurationVar(&opts.dialTimeout, "dial-timeout", 5*time.Second, "Connection establishment timeout")
	pflags.DurationVar(&opts.requestTimeout, "request-timeout", 30*time.Second, "Per-request timeout")
	pflags.IntVar(&opts.maxRetries, "max-retries", 3, "Maximum number of retry attempts")
	pflags.BoolVarP(&opts.verbose, "verbose", "v", false, "Enable verbose output")

	rootCmd.SetVersionTemplate(`{{printf "%s %s\n" .Name .Version}}`)

	rootCmd.AddCommand(
		newAcquireCmd(),
		newReleaseCmd(),
		newRenewCmd(),
		newInfoCmd(),
		newListCmd(),
	)

	return rootCmd
}

// getGlobalOpts extracts the globalOpts struct from the given command's context.
// Panics if the value is missing or has an unexpected type.
func getGlobalOpts(cmd *cobra.Command) *globalOpts {
	return cmd.Context().Value("globalOpts").(*globalOpts)
}

// outputJSON encodes the given value as indented JSON and writes it to stdout.
// Returns an error if encoding or writing fails.
func outputJSON(v interface{}) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(v)
}

// newAcquireCmd defines the "acquire" subcommand.
// It requests ownership of a distributed lock from the RaftLock service,
// supporting options such as TTL, wait behavior, priority, and metadata.
func newAcquireCmd() *cobra.Command {
	var (
		lockID       string
		clientID     string
		ttl          time.Duration
		wait         bool
		waitTimeout  time.Duration
		priority     int
		metadata     string
		requestID    string
		outputAsJSON bool
	)

	cmd := &cobra.Command{
		Use:   "acquire",
		Short: "Acquire a distributed lock",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := getGlobalOpts(cmd)

			var metadataMap map[string]string
			if metadata != "" {
				if err := json.Unmarshal([]byte(metadata), &metadataMap); err != nil {
					return fmt.Errorf("invalid metadata JSON: %w", err)
				}
			}

			req := &client.AcquireRequest{
				LockID:      lockID,
				ClientID:    clientID,
				TTL:         ttl,
				Wait:        wait,
				WaitTimeout: waitTimeout,
				Priority:    int32(priority),
				Metadata:    metadataMap,
				RequestID:   requestID,
			}

			ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
			defer cancel()

			if opts.verbose {
				fmt.Fprintf(os.Stderr, "Acquiring lock %s for client %s...\n", lockID, clientID)
			}

			result, err := opts.raftClient.Acquire(ctx, req)
			if err != nil {
				return fmt.Errorf("failed to acquire lock: %w", err)
			}

			if outputAsJSON {
				return outputJSON(result)
			}

			if result.Acquired {
				fmt.Println("✓ Lock acquired successfully")
				fmt.Printf("  Lock ID: %s\n", result.Lock.LockID)
				fmt.Printf("  Owner: %s\n", result.Lock.OwnerID)
				fmt.Printf("  Version: %d\n", result.Lock.Version)
				fmt.Printf("  Acquired At: %s\n", result.Lock.AcquiredAt.Format(time.RFC3339))
				fmt.Printf("  Expires At: %s\n", result.Lock.ExpiresAt.Format(time.RFC3339))
				if len(result.Lock.Metadata) > 0 {
					fmt.Printf("  Metadata: %v\n", result.Lock.Metadata)
				}
			} else {
				fmt.Println("✗ Lock not acquired")
				if result.Error != nil {
					fmt.Printf("  Error: %s (code: %v)\n", result.Error.Message, result.Error.Code)
				}
				if result.QueuePosition > 0 {
					fmt.Printf("  Queue Position: %d\n", result.QueuePosition)
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&lockID, "lock-id", "", "Unique lock identifier (required)")
	cmd.Flags().StringVar(&clientID, "client-id", "", "Client identifier (required)")
	cmd.Flags().DurationVar(&ttl, "ttl", 30*time.Second, "Time-to-live for the lock")
	cmd.Flags().BoolVar(&wait, "wait", false, "Wait in queue if lock is already held")
	cmd.Flags().DurationVar(&waitTimeout, "wait-timeout", 30*time.Second, "Maximum time to wait for lock acquisition")
	cmd.Flags().IntVar(&priority, "priority", 0, "Request priority in wait queue (higher = higher priority)")
	cmd.Flags().StringVar(&metadata, "metadata", "", "Optional JSON metadata to attach to the lock")
	cmd.Flags().StringVar(&requestID, "request-id", "", "Optional request ID for idempotency")
	cmd.Flags().BoolVar(&outputAsJSON, "json", false, "Print result as JSON instead of human-readable text")

	cmd.MarkFlagRequired("lock-id")
	cmd.MarkFlagRequired("client-id")

	return cmd
}

// newReleaseCmd defines the "release" subcommand.
// It releases a previously acquired lock, identified by lock ID, client ID,
// and version (fencing token). On success, the lock may be handed over
// to a waiting client if one exists.
func newReleaseCmd() *cobra.Command {
	var (
		lockID       string
		clientID     string
		version      int64
		outputAsJSON bool
	)

	cmd := &cobra.Command{
		Use:   "release",
		Short: "Release a distributed lock",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := getGlobalOpts(cmd)

			req := &client.ReleaseRequest{
				LockID:   lockID,
				ClientID: clientID,
				Version:  version,
			}

			ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
			defer cancel()

			if opts.verbose {
				fmt.Fprintf(os.Stderr, "Releasing lock %s for client %s (version %d)...\n",
					lockID, clientID, version)
			}

			result, err := opts.raftClient.Release(ctx, req)
			if err != nil {
				return fmt.Errorf("failed to release lock: %w", err)
			}

			if outputAsJSON {
				return outputJSON(result)
			}

			if result.Released {
				fmt.Println("✓ Lock released successfully")
				if result.WaiterPromoted {
					fmt.Println("  A waiting client was promoted to lock holder")
				}
			} else {
				fmt.Println("✗ Lock not released")
				if result.Error != nil {
					fmt.Printf("  Error: %s (code: %v)\n", result.Error.Message, result.Error.Code)
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&lockID, "lock-id", "", "Unique lock identifier (required)")
	cmd.Flags().StringVar(&clientID, "client-id", "", "Client identifier (required)")
	cmd.Flags().Int64Var(&version, "version", 0, "Lock version or fencing token (required)")
	cmd.Flags().BoolVar(&outputAsJSON, "json", false, "Print result as JSON instead of human-readable text")

	cmd.MarkFlagRequired("lock-id")
	cmd.MarkFlagRequired("client-id")
	cmd.MarkFlagRequired("version")

	return cmd
}

// newRenewCmd defines the "renew" subcommand.
// It extends the lifetime of an existing lock by updating its TTL.
// The lock must be identified by lock ID, client ID, and version (fencing token).
func newRenewCmd() *cobra.Command {
	var (
		lockID       string
		clientID     string
		version      int64
		newTTL       time.Duration
		outputAsJSON bool
	)

	cmd := &cobra.Command{
		Use:   "renew",
		Short: "Renew a distributed lock",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := getGlobalOpts(cmd)

			req := &client.RenewRequest{
				LockID:   lockID,
				ClientID: clientID,
				Version:  version,
				NewTTL:   newTTL,
			}

			ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
			defer cancel()

			if opts.verbose {
				fmt.Fprintf(os.Stderr, "Renewing lock %s for client %s (version %d) with TTL %v...\n",
					lockID, clientID, version, newTTL)
			}

			result, err := opts.raftClient.Renew(ctx, req)
			if err != nil {
				return fmt.Errorf("failed to renew lock: %w", err)
			}

			if outputAsJSON {
				return outputJSON(result)
			}

			if result.Renewed {
				fmt.Println("✓ Lock renewed successfully")
				fmt.Printf("  Lock ID: %s\n", result.Lock.LockID)
				fmt.Printf("  Expires At: %s\n", result.Lock.ExpiresAt.Format(time.RFC3339))
			} else {
				fmt.Println("✗ Lock not renewed")
				if result.Error != nil {
					fmt.Printf("  Error: %s (code: %v)\n", result.Error.Message, result.Error.Code)
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&lockID, "lock-id", "", "Unique lock identifier (required)")
	cmd.Flags().StringVar(&clientID, "client-id", "", "Client identifier (required)")
	cmd.Flags().Int64Var(&version, "version", 0, "Lock version or fencing token (required)")
	cmd.Flags().DurationVar(&newTTL, "ttl", 30*time.Second, "New time-to-live duration for the lock")
	cmd.Flags().BoolVar(&outputAsJSON, "json", false, "Print result as JSON instead of human-readable text")

	cmd.MarkFlagRequired("lock-id")
	cmd.MarkFlagRequired("client-id")
	cmd.MarkFlagRequired("version")

	return cmd
}

// newInfoCmd defines the "info" subcommand.
// It retrieves the current state of a lock, including ownership,
// expiration, and optional waiter details.
func newInfoCmd() *cobra.Command {
	var (
		lockID         string
		includeWaiters bool
		outputAsJSON   bool
	)

	cmd := &cobra.Command{
		Use:   "info",
		Short: "Get information about a specific lock",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := getGlobalOpts(cmd)

			req := &client.GetLockInfoRequest{
				LockID:         lockID,
				IncludeWaiters: includeWaiters,
			}

			ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
			defer cancel()

			if opts.verbose {
				fmt.Fprintf(os.Stderr, "Fetching info for lock %s...\n", lockID)
			}

			info, err := opts.raftClient.GetLockInfo(ctx, req)
			if err != nil {
				return fmt.Errorf("failed to get lock info: %w", err)
			}

			if outputAsJSON {
				return outputJSON(info)
			}

			fmt.Println("Lock Information:")
			fmt.Printf("  Lock ID: %s\n", info.LockID)
			if info.OwnerID != "" {
				fmt.Println("  Status: HELD")
				fmt.Printf("  Owner: %s\n", info.OwnerID)
				fmt.Printf("  Version: %d\n", info.Version)
				fmt.Printf("  Expires At: %s\n", info.ExpiresAt.Format(time.RFC3339))
			} else {
				fmt.Println("  Status: AVAILABLE")
			}
			fmt.Printf("  Waiter Count: %d\n", info.WaiterCount)

			if includeWaiters && len(info.WaitersInfo) > 0 {
				fmt.Println("  Waiters:")
				for _, w := range info.WaitersInfo {
					fmt.Printf("    - Client: %s, Priority: %d\n", w.ClientID, w.Priority)
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&lockID, "lock-id", "", "Unique lock identifier (required)")
	cmd.Flags().BoolVar(&includeWaiters, "include-waiters", false, "Include detailed waiter information in output")
	cmd.Flags().BoolVar(&outputAsJSON, "json", false, "Print result as JSON instead of human-readable text")

	cmd.MarkFlagRequired("lock-id")

	return cmd
}

// newListCmd defines the "list" subcommand.
// It retrieves a list of locks that match optional filters such as lock ID,
// owner ID, held status, or contested status. Results can be paginated
// and returned in either human-readable or JSON format.
func newListCmd() *cobra.Command {
	var (
		lockPattern    string
		ownerPattern   string
		onlyHeld       bool
		onlyContested  bool
		limit          int
		offset         int
		includeWaiters bool
		outputAsJSON   bool
	)

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List locks matching filter criteria",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := getGlobalOpts(cmd)

			req := &client.GetLocksRequest{
				Filter: &client.LockFilter{
					LockIDPattern:  lockPattern,
					OwnerIDPattern: ownerPattern,
					OnlyHeld:       onlyHeld,
					OnlyContested:  onlyContested,
				},
				Limit:          int32(limit),
				Offset:         int32(offset),
				IncludeWaiters: includeWaiters,
			}

			ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
			defer cancel()

			if opts.verbose {
				fmt.Fprintln(os.Stderr, "Listing locks with filters...")
			}

			result, err := opts.raftClient.GetLocks(ctx, req)
			if err != nil {
				return fmt.Errorf("failed to list locks: %w", err)
			}

			// Support JSON or human-readable output.
			if outputAsJSON {
				return outputJSON(result)
			}

			if len(result.Locks) == 0 {
				fmt.Println("No locks found matching criteria.")
				return nil
			}

			fmt.Printf("Found %d locks (showing %d)\n\n", result.TotalMatching, len(result.Locks))
			fmt.Printf("%-30s %-20s %-10s %-8s %-20s\n", "LOCK ID", "OWNER", "VERSION", "WAITERS", "EXPIRES")
			fmt.Println(strings.Repeat("-", 90))

			for _, lock := range result.Locks {
				owner := lock.OwnerID
				if owner == "" {
					owner = "<available>"
				}

				expires := "-"
				if !lock.ExpiresAt.IsZero() && lock.OwnerID != "" {
					remaining := time.Until(lock.ExpiresAt).Round(time.Second)
					if remaining > 0 {
						expires = fmt.Sprintf("in %v", remaining)
					} else {
						expires = "EXPIRED"
					}
				}

				fmt.Printf("%-30s %-20s %-10d %-8d %-20s\n",
					lock.LockID, owner, lock.Version, lock.WaiterCount, expires)
			}

			if result.HasMore {
				fmt.Printf("\nMore results available. Use --offset %d to see the next page.\n", offset+limit)
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&lockPattern, "lock-pattern", "", "Filter by lock ID (supports wildcards)")
	cmd.Flags().StringVar(&ownerPattern, "owner-pattern", "", "Filter by owner ID (supports wildcards)")
	cmd.Flags().BoolVar(&onlyHeld, "only-held", false, "Show only locks that are currently held")
	cmd.Flags().BoolVar(&onlyContested, "only-contested", false, "Show only locks that have waiters")
	cmd.Flags().IntVar(&limit, "limit", 100, "Maximum number of results to return")
	cmd.Flags().IntVar(&offset, "offset", 0, "Number of results to skip (for pagination)")
	cmd.Flags().BoolVar(&includeWaiters, "include-waiters", false, "Include detailed waiter information in output")
	cmd.Flags().BoolVar(&outputAsJSON, "json", false, "Print results as JSON instead of human-readable text")

	return cmd
}
