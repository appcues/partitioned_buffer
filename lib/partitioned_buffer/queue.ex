defmodule PartitionedBuffer.Queue do
  @moduledoc """
  ETS-based partitioned queue buffer for high-throughput data processing.

  `PartitionedBuffer.Queue` buffers arbitrary data in insertion order and
  periodically processes it using a configurable processor function.
  It implements partitioning to reduce lock contention during high-throughput
  writes, and uses double-buffering to ensure zero-downtime processing.

  ## Examples

  ### Standalone Usage

      # Start a queue buffer with a custom processor
      iex> {:ok, _sup_pid} =
      ...>   PartitionedBuffer.Queue.start_link(
      ...>     name: :my_buffer,
      ...>     processor: fn batch -> IO.inspect(batch) end
      ...>   )

      # Push a single item into the buffer
      iex> PartitionedBuffer.Queue.push(:my_buffer, "item1")
      :ok

      # Push a batch of items
      iex> PartitionedBuffer.Queue.push(:my_buffer, ["item2", "item3"])
      :ok

      # Check buffer size
      iex> PartitionedBuffer.Queue.size(:my_buffer)
      3

      # Stop the buffer gracefully (processes remaining items)
      iex> PartitionedBuffer.Queue.stop(:my_buffer)
      :ok

  ### Adding to a Supervision Tree

      children = [
        {PartitionedBuffer.Queue,
         name: :my_buffer,
         processor: &MyApp.EventProcessor.process_batch/1}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  ## Processor

  The processor function receives a list of values
  (the items pushed to the buffer):

      fn batch ->
        # batch is [value1, value2, ...]
        Enum.each(batch, fn value -> process(value) end)
      end

  ## Options

  See `PartitionedBuffer` for
  [start](`m:PartitionedBuffer#module-start-options`) and
  [runtime](`m:PartitionedBuffer#module-runtime-options`) options.

  """

  @behaviour PartitionedBuffer

  import Record, only: [defrecordp: 2]

  alias PartitionedBuffer.{Options, Partition}

  # Queue-specific key record (ordered by insertion time).
  # The `timestamp` ensures order by insertion time (asc) while the
  # `ref` makes each entry unique since there may be multiple entries
  # with the same timestamp.
  defrecordp(:key, timestamp: nil, ref: nil)

  @typedoc "Any term that will be buffered and processed"
  @type item() :: any()

  @typedoc "Proxy type for a buffer"
  @type buffer() :: PartitionedBuffer.buffer()

  ## API

  @doc """
  Starts a new queue buffer.

  ## Options

  See `PartitionedBuffer` for
  [start](`m:PartitionedBuffer#module-start-options`) options.

  ## Examples

      PartitionedBuffer.Queue.start_link(name: :my_queue_buffer)

  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    opts
    |> Keyword.put(:module, __MODULE__)
    |> PartitionedBuffer.start_link()
  end

  @doc """
  Stops a queue buffer gracefully.

  ## Examples

      PartitionedBuffer.Queue.stop(:my_queue_buffer)

  """
  @spec stop(buffer() | pid(), reason :: any(), timeout()) :: :ok
  defdelegate stop(buffer, reason \\ :normal, timeout \\ :infinity), to: PartitionedBuffer

  @doc """
  Returns the queue buffer child spec.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: opts[:name] || __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  Pushes an item or a batch of items into the buffer.

  ## Parameters

    * `buffer` - The buffer name (atom).
    * `item_or_batch` - A single item or a list of items to push.
    * `opts` - Optional runtime options.

  ## Options

  See `PartitionedBuffer` for
  [runtime](`m:PartitionedBuffer#module-runtime-options`) options.

  ## Examples

      # Simple push with default routing
      push(:my_buffer, "item1")
      push(:my_buffer, ["item2", "item3"])

      # Custom partition routing using function
      push(:my_buffer, user_event, partition_key: &(&1.user_id))

      # Custom partition routing using MFA tuple (item prepended to args)
      push(:my_buffer, event, partition_key: {MyApp.Router, :get_partition, []})

      # Custom partition routing with fixed key (all items to same partition)
      push(:my_buffer, log_entry, partition_key: :logs)

  """
  @spec push(buffer(), item() | [item()], keyword()) :: :ok
  def push(buffer, item_or_batch, opts \\ [])

  def push(buffer, batch, opts) when is_list(batch) do
    opts = Options.validate_runtime_options!(opts)
    partition_key = Keyword.fetch!(opts, :partition_key)

    batch
    |> Enum.group_by(&PartitionedBuffer.get_partition(buffer, partition_key, &1))
    |> Enum.each(fn {partition, items} ->
      entries = Enum.map(items, &Partition.new_entry(build_key(), &1))

      Partition.put(partition, entries)
    end)
  end

  def push(buffer, item, opts) do
    push(buffer, [item], opts)
  end

  @doc """
  Returns the queue size (total number of items across all partitions).

  ## Examples

      size(:my_buffer)

  """
  @spec size(buffer()) :: non_neg_integer()
  defdelegate size(buffer), to: PartitionedBuffer, as: :buffer_size

  ## Callbacks

  @impl true
  def ets_type, do: :ordered_set

  ## Private functions

  @compile [inline: [build_key: 0]]
  defp build_key, do: key(timestamp: System.monotonic_time(), ref: make_ref())
end
