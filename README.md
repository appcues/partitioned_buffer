# PartitionedBuffer

An ETS-based partitioned buffer library for high-throughput data processing in
Elixir.

`PartitionedBuffer` is a generic, reusable buffering system inspired by the
[OpenTelemetry Batch Processor](https://github.com/open-telemetry/opentelemetry-erlang/blob/main/apps/opentelemetry/src/otel_batch_processor.erl). It efficiently buffers arbitrary data and
periodically processes it using a configurable processor function at
regular intervals.

## Installation


Add `:partitioned_buffer` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:partitioned_buffer, "~> 0.1.0"}
  ]
end
```

## Usage

### Basic Example

```elixir
# Start a buffer with a custom processor
{:ok, _pid} = PartitionedBuffer.start_link(
  name: :my_buffer,
  processor: fn batch -> IO.inspect(batch) end
)

# Write messages to the buffer
:ok = PartitionedBuffer.write(:my_buffer, "message1")
:ok = PartitionedBuffer.write(:my_buffer, ["message2", "message3"])

# Check buffer size
size = PartitionedBuffer.buffer_size(:my_buffer)
```

### Adding to a Supervision Tree

In most applications, you'll want to add `PartitionedBuffer` as a child in your
application's supervision tree:

```elixir
# In your application supervisor (lib/my_app/application.ex)
defmodule MyApp.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # ... other children ...
      {PartitionedBuffer,
       name: :my_buffer,
       processor: &MyApp.EventProcessor.process_batch/1,
       processing_interval_ms: 1000,
       partitions: 4}
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

The buffer will be automatically started with your application and will process
any remaining messages during graceful shutdown.

### Advanced Configuration

```elixir
{:ok, _pid} = PartitionedBuffer.start_link(
  name: :kafka_buffer,
  partitions: 4,                        # Custom number of partitions
  processing_interval_ms: 1000,         # Process every second
  processing_batch_size: 100,           # Batch size for processing
  processing_timeout_ms: 5000,          # Timeout for processing tasks
  processor: &MyApp.KafkaExporter.process/1
)
```
