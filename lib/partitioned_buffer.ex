defmodule PartitionedBuffer do
  @moduledoc """
  ETS-based partitioned buffer for high-throughput data processing.

  `PartitionedBuffer` is a generic, reusable buffering system that efficiently
  buffers arbitrary data and periodically processes it using a configurable
  processor function. It implements partitioning to reduce lock contention
  during high-throughput writes, and uses double-buffering to ensure
  zero-downtime processing.

  ## Key Features

    * **Partitioned buffering**: Messages are distributed across multiple
      partitions using configurable routing, reducing lock contention.
    * **Flexible routing**: Route messages by content (default), custom function,
      or fixed key.
    * **Double-buffering**: While one partition buffer is being processed,
      another continues accepting writes for true concurrent operation.
    * **Async processing**: Processing happens in separate tasks to avoid
      blocking writes.
    * **Configurable processor**: Provide any handler function to process
      batches of data.
    * **Graceful shutdown**: Remaining buffered data is processed before
      the buffer terminates.
    * **Telemetry**: Structured observability through telemetry events.

  ## Architecture

  This is what a buffer supervision tree looks like:

  ```asciidoc
                    [buffer_supervisor]
                            |
                +-----------+------------------+
                |           |                  |
            [Registry] [TaskSupervisor] [PartitionSupervisor]
                                               |
                                +--------------+-------------------+
                                |              |                   |
                      [partition.0]    [partition.1]   ...   [partition.N-1]
                            |                |                     |
                      [ETS table pair] [ETS table pair]      [ETS table pair]
  ```

  The buffer supervisor spawns a `Registry` for partition discovery, a
  `Task.Supervisor` for async processing, and a `PartitionSupervisor` to manage
  the partition pool. The partition supervisor spawns N number of partition
  `GenServer` children. Each partition manages two ETS tables that alternate
  roles during processing:

    * One table receives new writes.
    * The other is processed asynchronously in batches.

  Message routing is determined by hashing a "routing key" to select which
  partition receives the message. The `:partition_key` option controls what
  value is used as the routing key:

    * **Default (no partition_key)**: The routing key is the message itself.
      Messages with the same content are routed to the same partition.

    * **Function**: The routing key is computed by applying the function to
      each message (e.g., `fn msg -> msg.user_id end`). This groups related
      messages together. For example, all messages from the same user go to
      the same partition.

    * **Static value**: You provide a fixed term (e.g., `:logs`) that is
      used as the routing key for all messages. This gives you explicit
      control over message routing.

  The `:partition_key` option is fundamentally about deciding what value
  determines which partition a message goes to. Use it to keep related
  messages together (for ordering or state locality) or spread unrelated
  messages across partitions (for parallelism).

  Every `:processing_interval_ms`, each partition checks its buffer. If
  messages exist, a processing cycle begins:

    1. The current write table is swapped for the other table.
    2. A processing task is spawned asynchronously.
    3. The processor callback is invoked with batches of messages.
    4. Once complete, the processed table is reset and becomes the write table
      for the next cycle.

  This design ensures writes are never blocked by processing operations.

  ## Examples

  ### Standalone Usage

      # Start a buffer with a custom processor
      {:ok, _sup_pid} = PartitionedBuffer.start_link(
        name: :my_buffer,
        processor: fn batch -> IO.inspect(batch) end
      )

      # Write messages to the buffer
      :ok = PartitionedBuffer.write(:my_buffer, "message1")
      :ok = PartitionedBuffer.write(:my_buffer, ["message2", "message3"])

      # Check buffer size
      size = PartitionedBuffer.buffer_size(:my_buffer)

      # Stop the buffer gracefully (processes remaining messages)
      :ok = PartitionedBuffer.stop(:my_buffer)

  ### Adding to a Supervision Tree

  To add a `PartitionedBuffer` as a child in your application's supervision
  tree:

      # In your application supervisor
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

  The buffer will be started as part of your application and its lifecycle will
  be managed by your application supervisor. All remaining messages will be
  processed automatically during shutdown.

  ## Telemetry

  `PartitionedBuffer` emits the following telemetry events.

    * `[:partitioned_buffer, :partition, :start]` - Dispatched when a partition
      is started.

      * Measurement: `%{system_time: integer}`
      * Metadata: `%{buffer: atom, partition: atom}`

    * `[:partitioned_buffer, :partition, :stop]` - Dispatched when a partition
      terminates (gracefully or abnormally).

      * Measurement: `%{duration: native_time}`
      * Metadata: `%{buffer: atom, partition: atom, reason: term}`

    * `[:partitioned_buffer, :partition, :processing, :start]` - Dispatched
      when a partition begins processing a batch of messages.

      * Measurement: `%{system_time: integer}`
      * Metadata: `%{buffer: atom, partition: atom, size: non_neg_integer}`

    * `[:partitioned_buffer, :partition, :processing, :stop]` - Dispatched
      when a partition completes processing a batch of messages.

      * Measurement: `%{duration: native_time}`
      * Metadata: `%{buffer: atom, partition: atom, size: non_neg_integer}`

    * `[:partitioned_buffer, :partition, :processing_failed]` - Dispatched
      when a processing task encounters an error and fails.

      * Measurement: `%{system_time: integer}`
      * Metadata: `%{buffer: atom, partition: atom, reason: any}`

  """

  alias PartitionedBuffer.{Options, Partition}

  @typedoc "Buffer name"
  @type buffer() :: atom()

  @typedoc "Any term that will be buffered and processed"
  @type message() :: any()

  ## API

  @doc """
  Starts a new buffer.

  ## Options

  #{PartitionedBuffer.Options.start_options_docs()}

  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  defdelegate start_link(opts \\ []), to: PartitionedBuffer.Supervisor

  @doc """
  Returns the buffer child spec.

  ## Options

  #{PartitionedBuffer.Options.start_options_docs()}

  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  defdelegate child_spec(opts \\ []), to: PartitionedBuffer.Supervisor

  @doc """
  Stops a buffer gracefully.

  ## Examples

      :ok = PartitionedBuffer.stop(:my_buffer)

  """
  @spec stop(buffer() | pid(), reason :: any(), timeout()) :: :ok
  def stop(buffer, reason \\ :normal, timeout \\ :infinity)

  def stop(buffer, reason, timeout) when is_atom(buffer) do
    [buffer, Supervisor]
    |> Module.concat()
    |> Supervisor.stop(reason, timeout)
  end

  def stop(buffer, reason, timeout) when is_pid(buffer) do
    Supervisor.stop(buffer, reason, timeout)
  end

  @doc """
  Writes a message or a batch of messages into the buffer.

  ## Parameters

    * `buffer` - The buffer name (atom).
    * `msg_or_batch` - A single message or a list of messages to write.
    * `opts` - Optional write options.

  ## Options

  #{PartitionedBuffer.Options.write_options_docs()}

  ## Examples

      # Simple write with default routing
      PartitionedBuffer.write(:my_buffer, "message1")
      PartitionedBuffer.write(:my_buffer, ["msg2", "msg3"])

      # Custom partition routing using function
      PartitionedBuffer.write(:my_buffer, user_event, partition_key: &(&1.user_id))

      # Custom partition routing with fixed key (all messages to same partition)
      PartitionedBuffer.write(:my_buffer, log_entry, partition_key: :logs)

  """
  @spec write(buffer(), message() | [message()], keyword()) :: :ok
  def write(buffer, msg_or_batch, opts \\ [])

  def write(buffer, batch, opts) when is_list(batch) do
    opts = Options.validate_write_options!(opts)
    partition_key = Keyword.fetch!(opts, :partition_key)

    batch
    |> Enum.group_by(&get_partition(buffer, partition_key, &1))
    |> Enum.each(&Partition.write(elem(&1, 0), elem(&1, 1)))
  end

  def write(buffer, msg, opts) do
    write(buffer, [msg], opts)
  end

  @doc """
  Returns the buffer size (total number of messages across all partitions).
  """
  @spec buffer_size(buffer()) :: non_neg_integer()
  def buffer_size(buffer) do
    buffer
    |> lookup()
    |> Enum.map(&Partition.buffer_size(elem(&1, 1)))
    |> Enum.sum()
  end

  ## Private functions

  @compile {:inline, lookup: 1}
  defp lookup(buffer) do
    Registry.lookup(buffer, buffer)
  end

  defp get_partition(buffer, partition_key, msg) do
    key = partition_key(partition_key, msg)

    case lookup(buffer) do
      [] ->
        raise "no partitions available for buffer #{inspect(buffer)}. " <>
                "The buffer is not running, possibly because it is not " <>
                "started or does not exist"

      partitions ->
        partitions
        |> Enum.at(:erlang.phash2(key, length(partitions)))
        |> elem(1)
    end
  end

  defp partition_key(nil, msg) do
    :erlang.phash2(msg)
  end

  defp partition_key(partition_key, msg) when is_function(partition_key, 1) do
    partition_key.(msg)
  end

  defp partition_key(partition_key, _msg) do
    partition_key
  end
end
