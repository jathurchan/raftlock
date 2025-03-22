# RaftLock [![Go Test and Coverage](https://github.com/jathurchan/raftlock/actions/workflows/go-test.yml/badge.svg)](https://github.com/jathurchan/raftlock/actions/workflows/go-test.yml)

RaftLock is a **distributed lock service** built using Raft consensus. It provides **mutual exclusion** for distributed applications.

## Use Cases

RaftLock is useful in scenarios that require **coordination** across distributed components.

### 1. Payment Processing

- Ensures **only one payment processor** handles a transaction.
- Prevents **double charging** when handling multiple payment requests.

### 2. Distributed Job Scheduling

- Ensures that **only one worker** executes a critical job.
- Prevents duplicate execution in a **multi-server environment**.

## API Endpoints

RaftLock exposes **gRPC APIs** for acquiring and releasing locks.

### `Lock`

**Request:**

```json
{
"resource_id": "job-123",
"client_id": "worker-42",
"ttl_seconds": 30
}
```

**Response:**

```json
{
"success": true,
"message": "Lock acquired"
}
```

### `Unlock`

**Request:**

```json
{
"resource_id": "job-123",
"client_id": "worker-42"
}
```

**Response:**

```json
{
"success": true,
"message": "Lock acquired"
}
```

## Running RaftLock with Docker

RaftLock can run as a **single instance** or as a **multi-node cluster**.

### Build and Run a Single Node

```sh
docker build -t raftlock .
docker run -p 50051:50051 raftlock
```

### Build and Run Multiple RaftLock Nodes (Cluster Mode)

```sh
docker-compose up --build
```

This starts:

- **3 RaftLock nodes** (node1, node2, node3)
- Nodes communicate via gRPC
- Exposes ports **5001**, **5002**, and **5003**.

Check logs using (for example):

```sh
docker ps
docker logs raftlock_node1
```

## Using the CLI Client

RaftLock includes a command-line client for interacting with the service.

### Usage

```sh
RaftLock CLI

Usage:
 go run cmd/client/main.go [global-options] <command> [command-options] <resource-id>

Global Options:
 -host string Server hostname or IP address (default "localhost")
 -port string Server port (default "50051")
 -id string Client ID (default "cli-client")

Commands:
 lock <resource-id>
     Acquire a lock on the specified resource
     Options:
     -ttl int Lock TTL in seconds (default 0, using server default)
 unlock <resource-id>
     Release a lock on the specified resource
 help
     Show this help message

Examples:
 # Acquire a lock on 'my-resource'
 go run cmd/client/main.go lock my-resource
 
 # Acquire a lock with 60-second TTL
 go run cmd/client/main.go lock -ttl 60 my-resource
 
 # Release a lock on 'my-resource'
 go run cmd/client/main.go unlock my-resource
 
 # Connect to a specific server
 go run cmd/client/main.go -host 192.168.1.100 -port 8080 lock my-resource
```
