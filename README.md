# RaftLock

[![Unit Tests](https://github.com/jathurchan/raftlock/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/jathurchan/raftlock/actions/workflows/unit-tests.yml)
[![Integration Tests](https://github.com/jathurchan/raftlock/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/jathurchan/raftlock/actions/workflows/integration-tests.yml)
[![codecov](https://codecov.io/gh/jathurchan/raftlock/graph/badge.svg?token=RW0H2MKNMV)](https://codecov.io/gh/jathurchan/raftlock)
[![Lint](https://github.com/jathurchan/raftlock/actions/workflows/lint.yml/badge.svg)](https://github.com/jathurchan/raftlock/actions/workflows/lint.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`raftlock` is a fault-tolerant distributed lock service for coordinating distributed applications. It is written in Go and is designed to be a reliable and highly available solution for managing distributed locks.

## What the project does

In distributed systems, coordinating actions between different services or nodes can be a complex challenge. `raftlock` provides a simple and robust solution for this by offering a distributed locking mechanism. This ensures that only one process can access a particular resource at a time, preventing data corruption and ensuring consistency across your distributed application.

The project is built on top of the **Raft consensus algorithm**, which guarantees fault tolerance. This means that `raftlock` can withstand failures of some of its nodes without losing availability or data.

## Why the project is useful

`raftlock` is useful for a variety of use cases in distributed systems, including:

* **Leader Election:** Electing a single leader from a group of nodes to perform a specific task.
* **Distributed Cron Jobs:** Ensuring that a scheduled task is executed by only one node in a cluster.
* **Resource Locking:** Preventing multiple processes from concurrently modifying a shared resource, such as a file or a database record.
* **Distributed Semaphores:** Limiting the number of concurrent processes that can access a pool of resources.

By providing a reliable and easy-to-use distributed lock service, `raftlock` simplifies the development of robust and scalable distributed applications.

## How users can get started with the project

You can run the `raftlock` server using either Docker or by building from the source.

### Running with Docker 🐳

This is the recommended way to run `raftlock`. We use `docker-compose` to easily manage a multi-node cluster.

1. **Start the cluster:**
    This command will build the Docker images and start a 3-node `raftlock` cluster in the background.

    ```bash
    docker-compose up --build -d
    ```

2. **Check the logs:**
    You can monitor the logs of each node to see the cluster formation and leader election process.

    ```bash
    docker-compose logs node1
    docker-compose logs node2
    docker-compose logs node3
    ```

3. **Stop the cluster:**
    To stop and remove the containers, networks, and volumes, run:

    ```bash
    docker-compose down -v
    ```

### Building from Source

If you prefer to build from the source code:

1. **Clone the repository:**

    ```bash
    git clone https://github.com/jathurchan/raftlock.git
    cd raftlock
    ```

2. **Build the binary:**

    ```bash
    go build
    ```

3. **Run a server node:**
    You'll need to run multiple instances on different ports to form a cluster. See the **Configuration** section for more details.

    ```bash
    ./raftlock --id node1 --api-addr ":8080" --raft-addr ":12379" --raft-bootstrap
    ```

## Example: Distributed Payment 💳

To see `raftlock` in action, you can run the payment example located in the `examples/payment` directory. This example demonstrates how to use a distributed lock to ensure a payment process is handled by only one node at a time, preventing double spending.

**Run the example with the following command:**

```bash
go run examples/payment/main.go --payment-id payment456 --client-id client002 --servers localhost:8080
```

This will start a client that interacts with the `raftlock` server to acquire a lock before processing a mock payment. You will see output indicating whether the lock was acquired and the payment was processed.

## API Usage

You can interact with `raftlock` using its simple REST API. Here are some examples using `curl`.

### Acquire a Lock

To acquire a lock, send a `POST` request to the `/lock` endpoint with the `resource` you want to lock and a `ttl` (time-to-live) in seconds.

```bash
curl -X POST -H "Content-Type: application/json" -d '{
  "resource": "my-critical-resource",
  "ttl": 30
}' http://localhost:8080/lock
```

If successful, you will receive a `lock_id`.

### Release a Lock

To release a lock, send a `POST` request to the `/unlock` endpoint with the `lock_id` you received when acquiring the lock.

```bash
curl -X POST -H "Content-Type: application/json" -d '{
  "lock_id": "your-lock-id-here"
}' http://localhost:8080/unlock
```

## Where users can get help with your project

If you have any questions or encounter any issues while using `raftlock`, please feel free to open an issue on the [**GitHub repository**](https://github.com/jathurchan/raftlock/issues). We will do our best to help you as soon as possible.

## Contributing

We welcome contributions from the community\! If you are interested in contributing to `raftlock`, please follow these steps:

1. **Fork the repository** on GitHub.
2. **Create a new branch** for your feature or bug fix: `git checkout -b my-new-feature`.
3. **Make your changes** and commit them with clear, descriptive messages.
4. **Run the tests** to ensure everything is working: `go test ./...`.
5. **Push your branch** to your fork: `git push origin my-new-feature`.
6. **Submit a pull request** to the `main` branch of the `jathurchan/raftlock` repository.

## Who maintains and contributes to the project

`raftlock` is maintained by a team of dedicated developers:

* **Jathurchan Selvakumar**
* **Patrice Zhou**
* **Mathusan Selvakumar**

We welcome contributions from the community\! If you are interested in contributing to `raftlock`, please fork the repository and submit a pull request.
