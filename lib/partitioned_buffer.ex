defmodule PartitionedBuffer do
  @moduledoc """
  ETS-based partitioned buffer for high-throughput data processing.

  `PartitionedBuffer` provides the shared infrastructure (routing, partition
  lookup, lifecycle management) used by concrete buffer implementations:

    * `PartitionedBuffer.Queue` - Ordered queue buffer (insertion-time ordered).
    * `PartitionedBuffer.Map` - Key-value map buffer (last-write-wins semantics).

  ## Architecture

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

  ## Start options

  #{PartitionedBuffer.Options.start_options_docs()}

  ## Runtime options

  The following runtime options are shared by `PartitionedBuffer.Queue` and
  `PartitionedBuffer.Map`:

  #{PartitionedBuffer.Options.runtime_options_docs()}

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
      * Metadata: `%{buffer: atom, partition: atom}`

    * `[:partitioned_buffer, :partition, :processing, :stop]` - Dispatched
      when a partition completes processing a batch of messages.

      * Measurement: `%{duration: native_time, size: non_neg_integer}`
      * Metadata: `%{buffer: atom, partition: atom}`

    * `[:partitioned_buffer, :partition, :processing, :exception]` - Dispatched
      when an exception occurs during processing.

      * Measurement: `%{duration: native_time}`
      * Metadata:

      ```
      %{
        buffer: atom,
        partition: atom,
        kind: atom,
        reason: term,
        stacktrace: list
      }
      ```

    * `[:partitioned_buffer, :partition, :processing_failed]` - Dispatched
      when a processing task encounters an error and fails.

      * Measurement: `%{system_time: integer}`
      * Metadata: `%{buffer: atom, partition: atom, reason: any}`

  """

  alias PartitionedBuffer.{Options, Partition}

  @typedoc "Buffer name"
  @type buffer() :: atom()

  ## Callbacks

  @doc """
  Returns the ETS table type used by this buffer implementation.
  """
  @callback ets_type() :: :ordered_set | :set | :bag | :duplicate_bag

  ## API

  @doc """
  Starts a new buffer.

  > #### Prefer implementation-specific functions {: .tip}
  >
  > It is recommended to use `PartitionedBuffer.Queue.start_link/1` or
  > `PartitionedBuffer.Map.start_link/1` instead, as they automatically
  > set the `:module` option for you.

  ## Examples

      iex> PartitionedBuffer.start_link(
      ...>   module: PartitionedBuffer.Queue,
      ...>   name: :my_buffer
      ...> )
      {:ok, #PID<0.123.0>}

  > Notice that the `:module` option must be set to `PartitionedBuffer.Queue` or
  > `PartitionedBuffer.Map`.

  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  defdelegate start_link(opts), to: PartitionedBuffer.Supervisor

  @doc """
  Stops a buffer gracefully.

  ## Examples

      iex> PartitionedBuffer.stop(:my_buffer)
      :ok

  """
  @spec stop(buffer() | pid(), reason :: any(), timeout()) :: :ok
  def stop(buffer, reason, timeout)

  def stop(buffer, reason, timeout) when is_atom(buffer) do
    [buffer, Supervisor]
    |> Module.concat()
    |> Supervisor.stop(reason, timeout)
  end

  def stop(buffer, reason, timeout) when is_pid(buffer) do
    Supervisor.stop(buffer, reason, timeout)
  end

  @doc """
  Returns the buffer size (total number of messages across all partitions).

  ## Examples

      iex> PartitionedBuffer.buffer_size(:my_buffer)
      10

  """
  @spec buffer_size(buffer()) :: non_neg_integer()
  def buffer_size(buffer) do
    buffer
    |> lookup()
    |> Enum.map(&Partition.buffer_size(elem(&1, 1)))
    |> Enum.sum()
  end

  @doc """
  Updates the options for the buffer.

  ## Options

  #{Options.updatable_options_docs()}

  ## Examples

      iex> PartitionedBuffer.update_options(:my_buffer, processing_interval_ms: 1000)
      :ok

  > Notice that the options are updated for all partitions of the buffer.
  """
  @spec update_options(buffer(), keyword()) :: :ok
  def update_options(buffer, opts) do
    opts = Options.validate_updatable_options!(opts)

    buffer
    |> lookup()
    |> Enum.each(&Partition.update_options(elem(&1, 0), opts))
  end

  ## Shared routing helpers (used by Queue, Map, etc.)

  @doc """
  Returns the partition based on the given arguments.
  """
  @spec get_partition(buffer(), any(), any()) :: atom()
  def get_partition(buffer, partition_key, object) do
    key = partition_key(partition_key, object)

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

  ## Private functions

  @compile [inline: [lookup: 1]]
  defp lookup(buffer) do
    Registry.lookup(buffer, buffer)
  end

  # Compute the partition key
  defp partition_key(partition_key, object)

  # The partition key is not provided, use the message hash as the key
  defp partition_key(nil, object) do
    :erlang.phash2(object)
  end

  # The partition key is a function, apply it to the message
  defp partition_key(partition_key, object) when is_function(partition_key, 1) do
    partition_key.(object)
  end

  # The partition key is an MFA tuple, apply it (the message is prepended to the args)
  defp partition_key({m, f, a}, object) when is_atom(m) and is_atom(f) and is_list(a) do
    apply(m, f, [object | a])
  end

  # The partition key is a static value, return it
  defp partition_key(partition_key, _object) do
    partition_key
  end
end
