# PartitionedBuffer :bubbles:

![CI](https://github.com/appcues/partitioned_buffer/workflows/CI/badge.svg)
[![Hex.pm](http://img.shields.io/hexpm/v/partitioned_buffer.svg)](http://hex.pm/packages/partitioned_buffer)
[![Documentation](http://img.shields.io/badge/Documentation-ff69b4)](https://hexdocs.pm/partitioned_buffer)

An ETS-based partitioned buffer library for high-throughput data processing in
Elixir.

`PartitionedBuffer` is a generic, reusable buffering system inspired by the
[OpenTelemetry Batch Processor](https://github.com/open-telemetry/opentelemetry-erlang/blob/main/apps/opentelemetry/src/otel_batch_processor.erl).
It efficiently buffers arbitrary data and periodically processes it using a
configurable processor function at regular intervals.

It ships with two concrete buffer implementations:

- **`PartitionedBuffer.Queue`** — Ordered queue buffer (insertion-time ordered,
  backed by `:ordered_set` ETS tables).
- **`PartitionedBuffer.Map`** — Key-value map buffer (last-write-wins semantics,
  backed by `:set` ETS tables).

Both use partitioning to reduce lock contention and double-buffering for
zero-downtime processing.

## Installation

Add `:partitioned_buffer` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:partitioned_buffer, "~> 0.3"}
  ]
end
```

## Usage

### Queue

`PartitionedBuffer.Queue` buffers items in insertion order and processes them
in batches:

```elixir
# Start a queue buffer
{:ok, _pid} =
  PartitionedBuffer.Queue.start_link(
    name: :my_queue,
    processor: fn batch -> IO.inspect(batch) end
  )

# Push items into the buffer
:ok = PartitionedBuffer.Queue.push(:my_queue, "message1")
:ok = PartitionedBuffer.Queue.push(:my_queue, ["message2", "message3"])

# Check buffer size
PartitionedBuffer.Queue.size(:my_queue)
```

### Map

`PartitionedBuffer.Map` buffers key-value entries with last-write-wins
semantics. Entries with the same key overwrite previous values:

```elixir
# Start a map buffer
# The processor receives a list of {key, value, version, updates} tuples
{:ok, _pid} =
  PartitionedBuffer.Map.start_link(
    name: :my_map,
    processor: fn batch -> IO.inspect(batch) end
  )

# Put entries into the buffer
:ok = PartitionedBuffer.Map.put(:my_map, :key1, "value1")
:ok = PartitionedBuffer.Map.put_all(:my_map, %{key2: "value2", key3: "value3"})

# Read and delete entries
"value1" = PartitionedBuffer.Map.get(:my_map, :key1)
:ok = PartitionedBuffer.Map.delete(:my_map, :key1)

# Check buffer size
PartitionedBuffer.Map.size(:my_map)
```

#### Versioned Updates

For scenarios requiring "newer version wins" semantics (e.g., event sourcing,
state synchronization), use `put_newer/5` and `put_all_newer/3`:

```elixir
# Only updates if the version is greater than the existing one
:ok = PartitionedBuffer.Map.put_newer(:my_map, :key1, "v1", 100)
:ok = PartitionedBuffer.Map.put_newer(:my_map, :key1, "v2", 200)  # overwrites
:ok = PartitionedBuffer.Map.put_newer(:my_map, :key1, "v3", 50)   # ignored (50 < 200)

"v2" = PartitionedBuffer.Map.get(:my_map, :key1)

# Batch versioned updates
entries = [
  {:user_1, %{name: "Alice"}, 100},
  {:user_2, %{name: "Bob"}, 200}
]
:ok = PartitionedBuffer.Map.put_all_newer(:my_map, entries)
```

### Adding to a Supervision Tree

In most applications, you'll want to add a buffer as a child in your
application's supervision tree:

```elixir
defmodule MyApp.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Queue buffer
      {PartitionedBuffer.Queue,
       name: :event_queue,
       processor: &MyApp.EventProcessor.process_batch/1,
       processing_interval_ms: 1000,
       partitions: 4},

      # Map buffer
      {PartitionedBuffer.Map,
       name: :state_map,
       processor: &MyApp.StateProcessor.process_batch/1}
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

The buffer will be automatically started with your application and will process
any remaining items during graceful shutdown.

### Configuration Options

```elixir
{:ok, _pid} = PartitionedBuffer.Queue.start_link(
  name: :my_buffer,
  partitions: 4,                        # Number of partitions (default: schedulers_online)
  processing_interval_ms: 1000,         # Process every second (default: 5000)
  processing_batch_size: 100,           # Batch size for processing (default: 10)
  processing_timeout_ms: 5000,          # Timeout for processing tasks (default: 60000)
  processor: &MyApp.Exporter.export/1
)
```

See the `PartitionedBuffer` module documentation for the full list of start and
runtime options.
