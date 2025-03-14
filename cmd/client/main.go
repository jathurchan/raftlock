package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	client "github.com/jathurchan/raftlock/client"
)

const (
	defaultHost     = "localHost"
	defaultPort     = "50051"
	defaultClientID = "cli-client"
	defaultTimeout  = 5 * time.Second
)

// Command-line flags
var (
	host     = flag.String("host", defaultHost, "Server hostname or IP address")
	port     = flag.String("port", defaultPort, "Server port")
	clientID = flag.String("id", defaultClientID, "Client ID")
)

func main() {
	flag.Usage = showUsage
	flag.Parse()

	// Ensures command (lock, unlock...) is provided
	if flag.NArg() < 1 {
		showUsage()
		os.Exit(1)
	}
	command := flag.Arg(0)
	args := flag.Args()[1:]

	rlClient, err := createClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating client: %v\n", err)
		os.Exit(1)
	}
	defer rlClient.Close()

	switch command {
	case "lock":
		handleLockCommand(args, rlClient)
	case "unlock":
		handleUnlockCommand(args, rlClient)
	case "help":
		showUsage()
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		showUsage()
		os.Exit(1)
	}
}

// Creates a new RaftLock client
func createClient() (*client.RaftLockClient, error) {
	serverAddr := fmt.Sprintf("%s:%s", *host, *port)

	rlClient, err := client.NewRaftLockClient(serverAddr, *clientID)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v\n", serverAddr, err)
	}

	return rlClient, nil
}

func getResourceID(cmd *flag.FlagSet, args []string, commandName string) string {
	cmd.Parse(args)
	if cmd.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Resource ID required for %s command\n", commandName)
		os.Exit(1)
	}

	return cmd.Arg(0)
}

func handleLockCommand(args []string, rlClient *client.RaftLockClient) {
	lockCmd := flag.NewFlagSet("lock", flag.ExitOnError)
	ttlSeconds := lockCmd.Int("ttl", 0, "Lock TTL in seconds (0 for server default)")
	resourceID := getResourceID(lockCmd, args, "lock")

	options := client.DefaultLockOptions()
	if *ttlSeconds > 0 {
		options.TtlSeconds = int64(*ttlSeconds)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	res, err := rlClient.Lock(ctx, resourceID, options)
	if err != nil {
		exitWithError("Error locking resource", err)
	}

	if !res.Success {
		fmt.Fprintf(os.Stderr, "Failed to acquire lock: %s\n", res.Message)
		os.Exit(1)
	}
	fmt.Printf("Successfully acquired lock on '%s'\n", resourceID)

	if options.TtlSeconds > 0 {
		fmt.Printf("Lock will automatically expire in %d seconds\n", options.TtlSeconds)
	}
	os.Exit(0)
}

func handleUnlockCommand(args []string, rlClient *client.RaftLockClient) {
	unlockCmd := flag.NewFlagSet("unlock", flag.ExitOnError)
	resourceID := getResourceID(unlockCmd, args, "unlock")

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	res, err := rlClient.Unlock(ctx, resourceID)
	if err != nil {
		exitWithError("Error unlocking resource", err)
	}

	if !res.Success {
		fmt.Fprintf(os.Stderr, "Failed to release lock: %s\n", res.Message)
		os.Exit(1)
	}
	fmt.Printf("Successfully released lock on '%s'\n", resourceID)
	os.Exit(0)
}

// showUsage prints help information for the CLI
func showUsage() {
	fmt.Println("RaftLock CLI")
	fmt.Println("\nUsage:")
	fmt.Println(" go run cmd/client/main.go [global-options] <command> [command-options] <resource-id>")
	fmt.Println("\nGlobal Options:")
	fmt.Println(" -host string Server hostname or IP address (default \"localhost\")")
	fmt.Println(" -port string Server port (default \"50051\")")
	fmt.Println(" -id string Client ID (default \"cli-client\")")
	fmt.Println("\nCommands:")
	fmt.Println(" lock <resource-id>")
	fmt.Println("   Acquire a lock on the specified resource")
	fmt.Println("   Options:")
	fmt.Println("     -ttl int Lock TTL in seconds (default 0, using server default)")
	fmt.Println(" unlock <resource-id>")
	fmt.Println("   Release a lock on the specified resource")
	fmt.Println(" help")
	fmt.Println("   Show this help message")
	fmt.Println("\nExamples:")
	fmt.Println(" # Acquire a lock on 'my-resource'")
	fmt.Println(" go run cmd/client/main.go lock my-resource")
	fmt.Println(" # Acquire a lock with 60-second TTL")
	fmt.Println(" go run cmd/client/main.go lock -ttl 60 my-resource")
	fmt.Println(" # Release a lock on 'my-resource'")
	fmt.Println(" go run cmd/client/main.go unlock my-resource")
	fmt.Println(" # Connect to a specific server")
	fmt.Println(" go run cmd/client/main.go -host 192.168.1.100 -port 8080 lock my-resource")
}

// Prints error message and exits
func exitWithError(message string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", message, err)
	} else {
		fmt.Fprintf(os.Stderr, "%s\n", message)
	}
	os.Exit(1)
}
