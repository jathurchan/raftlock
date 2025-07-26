# RaftLock

[![Unit Tests](https://github.com/jathurchan/raftlock/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/jathurchan/raftlock/actions/workflows/unit-tests.yml)
[![Integration Tests](https://github.com/jathurchan/raftlock/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/jathurchan/raftlock/actions/workflows/integration-tests.yml)
[![codecov](https://codecov.io/gh/jathurchan/raftlock/graph/badge.svg?token=RW0H2MKNMV)](https://codecov.io/gh/jathurchan/raftlock)
[![Lint](https://github.com/jathurchan/raftlock/actions/workflows/lint.yml/badge.svg)](https://github.com/jathurchan/raftlock/actions/workflows/lint.yml)

A distributed lock manager built on top of the Raft consensus algorithm in Go.

## Overview

RaftLock is a distributed lock manager service that provides a simple and reliable way to coordinate access to shared resources in a distributed system. It leverages the Raft consensus algorithm to ensure that all lock operations are replicated across a cluster of nodes in a fault-tolerant manner. This means that even if some of the nodes in the cluster fail, the lock service will remain available and consistent.
