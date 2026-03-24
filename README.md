# skiffy

A c++ implementation of the Raft consensus algorithm, closely following
the TLA+ specification. Packaged as a single header (`src/skiffy.hpp`)
for easily including in any project.

## Quick start

### 1. Define your command type

Must be msgpack-serialisable (intrusive or `MSGPACK_DEFINE` macro).

```cpp
#include "skiffy.hpp"

struct my_cmd {
    std::string key;
    std::string value;
    MSGPACK_DEFINE(key, value)
};
```

### 2. Create a node and register callbacks

```cpp
skiffy::node<my_cmd, skiffy::memory_log_store> node("localhost", 9001);

node.on_apply([](const my_cmd& c) {
    // called on every committed command, on all nodes
});
```

Or use `file_log_store` for persistent WAL + snapshot storage:

```cpp
skiffy::node<my_cmd, skiffy::file_log_store> node(
    "localhost", 9001, "data");
```

### 3. Bootstrap or join, then run

```cpp
// first node — will self-elect after election timeout
node.run();   // blocks; handles SIGINT / SIGTERM

// subsequent nodes
node.join("localhost:9001");   // call before run()
node.run();
```

### 4. Submit commands

`submit()` is thread-safe, blocks until a leader is known, and
forwards automatically if this node is not the leader.

```cpp
node.submit({"hello", "world"});
```

### Three-node cluster example

```sh
./bld/myapp 9001 &
./bld/myapp 9002 localhost:9001 &
./bld/myapp 9003 localhost:9001 &
```

`node.leave()` initiates a graceful config change to remove the node;
`run()` returns when shutdown completes.

## Features

- All core TLA+ state transitions (Timeout, RequestVote, BecomeLeader,
  ClientRequest, AdvanceCommitIndex, AppendEntries, Restart)
- Joint consensus membership changes (`config_request()`)
- Log compaction with InstallSnapshot RPC
- Pluggable log store (`memory_log_store` / `file_log_store`)
- Pluggable state machine (bring your own or use `log_state_machine`)
- Asio-based networking with automatic leader forwarding
- Out-of-band join/leave protocol via `membership_manager`

## Dependencies

### dist release (`dist/skiffy.hpp`)

`dist/skiffy.hpp` is a self-contained header with sml and msgpack
inlined. You must supply the remaining deps yourself:

| Library | Role | How to get |
|---------|------|------------|
| Asio (standalone) | networking | `make deps` or bring your own |
| spdlog | logging (optional) | `make deps` or bring your own |

Define `SKIFFY_ENABLE_ASIO` and `ASIO_STANDALONE` when using asio.
Define `SKIFFY_ENABLE_SPDLOG` and `FMT_HEADER_ONLY` when using spdlog.

### building from source

All deps are fetched into `inc/` via:

```sh
make deps
```

| Library | Notes |
|---------|-------|
| Asio (standalone) | networking |
| msgpack-c | wire encoding |
| Boost.SML | state machine |
| spdlog | logging |
| cxxopts | CLI parsing (examples only) |
| doctest | test framework |
| nanobench | benchmarking |

C++17 and `g++` (or compatible) required.

## Building

```sh
make deps     # fetch third-party headers into inc/
make          # build tests + examples
make test     # run test suite
make lint     # syntax check
make format   # clang-format in-place
```

Binaries are written to `bld/`.

## Log store options

| Type | Persistence | Use case |
|------|------------|---------|
| `memory_log_store` | none | tests, ephemeral nodes |
| `file_log_store` | WAL + snapshot file | production |

`cluster_node` derives the file prefix from the node id automatically
when you pass a `data_dir`.

## Log compaction

```cpp
node.compact_threshold(1000);   // snapshot after N committed entries
```

## Logging

skiffy uses spdlog under the logger name `"skiffy"`. Register a
logger before constructing the node to capture output:

```cpp
auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
auto log  = std::make_shared<spdlog::logger>("skiffy", sink);
log->set_level(spdlog::level::info);
spdlog::register_logger(log);
```

If no logger is registered, output is silently discarded.

## Examples

| Binary | Description |
|--------|-------------|
| `bld/kv` | distributed key-value store (in-memory) |
| `bld/queue` | distributed work queue |
| `bld/http_server` | HTTP KV store with leader forwarding |
| `bld/http_client` | load client for http_server |

```sh
make run-kv     # three-node kv cluster
make run-http   # HTTP cluster + load client
```

## Reference

The TLA+ specification `doc/raft.tla` by Diego Ongaro is the
authoritative reference for the protocol logic implemented here.
