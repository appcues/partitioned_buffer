defmodule PartitionedBuffer.Map do
  @moduledoc """
  ETS-based partitioned map buffer for high-throughput key-value data processing.

  `PartitionedBuffer.Map` buffers key-value entries using `:set` ETS tables,
  giving last-write-wins semantics for duplicate keys. It periodically processes
  buffered entries using a configurable processor function. Like `Queue`, it
  implements partitioning to reduce lock contention during high-throughput
  writes, and uses double-buffering to ensure zero-downtime processing.

  It also supports versioned conditional updates via `put_newer/5` and
  `put_all_newer/3`, which use "newer version wins" semantics — an entry is
  only written if the key doesn't exist or the new version is greater than
  the existing one.

  ## Data Flow

  ```asciidoc
  put(buffer, key, value)          put_newer(buffer, key, value, version)
         |                                    |
         v                                    v
  +--------------------+            +--------------------+
  | Partition Routing  |            | Partition Routing  |
  | phash2(key, N)     |            | phash2(key, N)     |
  +--------------------+            +--------------------+
         |                                    |
         v                                    v
  +------------------+              +---------------------------+
  | :ets.insert      |              | 1. :ets.insert_new        |
  | (last-write-wins)|              |    Key new? -> inserted   |
  +------------------+              | 2. :ets.select_replace    |
                    \\              |    new_ver > old_ver?     |
                     \\             |    Yes -> updated         |
                      \\            |    No  -> skipped         |
                       \\           +---------------------------+
                        \\            /
                         v           v
              +--------------------------------+
              | ETS :set                       |
              | {key, value, version, updates} |
              +--------------------------------+
                            |
                            v
              +--------------------------------------+
              | processor(batch)                     |
              | batch = [{k, v, ver,  updates}, ...] |
              +--------------------------------------+
  ```

  Entries are routed to partitions via `phash2(key)` and stored in
  `:set` ETS tables. Regular `put/4` uses simple `ets:insert`
  (last-write-wins). Versioned `put_newer/5` uses a two-step
  atomic approach: `ets:insert_new` for new keys, then
  `ets:select_replace` for conditional "newer version wins"
  updates.

  ## Examples

  ### Standalone Usage

      # Start a map buffer with a custom processor
      iex> {:ok, _sup_pid} =
      ...>   PartitionedBuffer.Map.start_link(
      ...>     name: :my_map_buffer,
      ...>     processor: fn batch -> IO.inspect(batch) end
      ...>   )

      # Put a single entry
      iex> PartitionedBuffer.Map.put(:my_map_buffer, :key1, "value1")
      :ok

      # Put multiple entries at once
      iex> PartitionedBuffer.Map.put_all(:my_map_buffer, %{key2: "value2", key3: "value3"})
      :ok

      # Delete an entry
      iex> PartitionedBuffer.Map.delete(:my_map_buffer, :key1)
      :ok

      # Versioned put (newer version wins)
      iex> PartitionedBuffer.Map.put_newer(:my_map_buffer, :key4, "v1", 100)
      :ok
      iex> PartitionedBuffer.Map.put_newer(:my_map_buffer, :key4, "v2", 200)
      :ok
      iex> PartitionedBuffer.Map.get(:my_map_buffer, :key4)
      "v2"

      # Check buffer size
      iex> PartitionedBuffer.Map.size(:my_map_buffer)
      3

      # Stop the buffer gracefully (processes remaining items)
      iex> PartitionedBuffer.Map.stop(:my_map_buffer)
      :ok

  ### Adding to a Supervision Tree

      children = [
        {PartitionedBuffer.Map,
         name: :my_map_buffer,
         processor: &MyApp.EventProcessor.process_batch/1}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  ## Processor

  The processor function receives a list of `{key, value, version, updates}`
  tuples, where `version` is the entry version (set via `put_newer/5` and
  `put_all_newer/3`; `0` for regular `put/4` entries), and `updates` is the
  number of times an existing key was updated (only tracked for versioned
  updates; regular `put/4` entries always have `updates` set to `0`):

      fn batch ->
        # batch is [{key1, value1, version1, updates1}, {key2, value2, version2, updates2}, ...]
        Enum.each(batch, fn {k, v, _version, _updates} -> process(k, v) end)
      end

  ## Options

  See `PartitionedBuffer` for
  [start](`m:PartitionedBuffer#module-start-options`) and
  [runtime](`m:PartitionedBuffer#module-runtime-options`) options.

  """

  @behaviour PartitionedBuffer

  alias PartitionedBuffer.{Options, Partition}

  @typedoc "Proxy type for a buffer"
  @type buffer() :: PartitionedBuffer.buffer()

  ## API

  @doc """
  Starts a new map buffer.

  ## Options

  See `PartitionedBuffer` for
  [start](`m:PartitionedBuffer#module-start-options`) options.

  ## Examples

      PartitionedBuffer.Map.start_link(name: :my_map_buffer)

  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    opts
    |> Keyword.put(:module, __MODULE__)
    |> PartitionedBuffer.start_link()
  end

  @doc """
  Stops a map buffer gracefully.

  ## Examples

      PartitionedBuffer.Map.stop(:my_map_buffer)

  """
  @spec stop(buffer() | pid(), reason :: any(), timeout()) :: :ok
  defdelegate stop(buffer, reason \\ :normal, timeout \\ :infinity), to: PartitionedBuffer

  @doc """
  Returns the map buffer child spec.
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
  Puts a single key-value entry into the buffer.

  ## Parameters

    * `buffer` - The buffer name (atom).
    * `key` - The key for the entry.
    * `value` - The value for the entry.
    * `opts` - Optional runtime options.

  ## Options

  See `PartitionedBuffer` for
  [runtime](`m:PartitionedBuffer#module-runtime-options`) options.

  ## Examples

      # Simple put
      put(:my_buffer, :key1, "val1")

      # With custom partition routing
      put(:my_buffer, :key1, "val1", partition_key: fn {k, _v} -> k end)

  """
  @spec put(buffer(), any(), any(), keyword()) :: :ok
  def put(buffer, key, value, opts \\ []) do
    put_all(buffer, [{key, value}], opts)
  end

  @doc """
  Puts multiple key-value entries into the buffer.

  Accepts either a map or a list of `{key, value}` tuples.

  ## Parameters

    * `buffer` - The buffer name (atom).
    * `entries` - A map or list of `{key, value}` tuples.
    * `opts` - Optional runtime options.

  ## Options

  See `PartitionedBuffer` for
  [runtime](`m:PartitionedBuffer#module-runtime-options`) options.

  ## Examples

      # Using a map
      put_all(:my_buffer, %{key1: "val1", key2: "val2"})

      # Using a list of tuples
      put_all(:my_buffer, [{:key1, "val1"}, {:key2, "val2"}])

      # With custom partition routing
      put_all(:my_buffer, %{key1: "val1"}, partition_key: fn {k, _v} -> k end)

  """
  @spec put_all(buffer(), map() | [{any(), any()}], keyword()) :: :ok
  def put_all(buffer, entries, opts \\ []) when is_map(entries) or is_list(entries) do
    opts = Options.validate_runtime_options!(opts)
    partition_key = Keyword.fetch!(opts, :partition_key)

    entries
    |> Enum.group_by(fn {key, _value} ->
      PartitionedBuffer.get_partition(buffer, partition_key, key)
    end)
    |> Enum.each(fn {partition, kv_pairs} ->
      entries = Enum.map(kv_pairs, fn {key, value} -> Partition.new_entry(key, value) end)

      Partition.put(partition, entries)
    end)
  end

  @doc """
  Puts a single versioned key-value entry into the buffer.

  Uses "newer version wins" semantics: the entry is only written if:
  - The key doesn't exist, or
  - The new version is greater than the existing version

  This is useful for scenarios where you want to ensure only the latest
  version of data is stored, such as event sourcing or state synchronization.

  ## Parameters

    * `buffer` - The buffer name (atom).
    * `key` - The key for the entry.
    * `value` - The value for the entry.
    * `version` - The version (must be comparable with `>`).
    * `opts` - Optional runtime options.

  ## Options

  See `PartitionedBuffer` for
  [runtime](`m:PartitionedBuffer#module-runtime-options`) options.

  ## Examples

      # Using timestamps as versions
      put_newer(:my_buffer, :user_123, %{name: "Alice"}, System.monotonic_time())

      # Using sequence numbers
      put_newer(:my_buffer, :counter, 42, 5)

  """
  @spec put_newer(buffer(), any(), any(), integer(), keyword()) :: :ok
  def put_newer(buffer, key, value, version, opts \\ []) do
    put_all_newer(buffer, [{key, value, version}], opts)
  end

  @doc """
  Puts multiple versioned key-value entries into the buffer.

  Uses "newer version wins" semantics for each entry.

  ## Parameters

    * `buffer` - The buffer name (atom).
    * `entries` - A list of `{key, value, version}` tuples.
    * `opts` - Optional runtime options.

  ## Options

  See `PartitionedBuffer` for
  [runtime](`m:PartitionedBuffer#module-runtime-options`) options.

  ## Examples

      entries = [
        {:user_1, %{name: "Alice"}, 100},
        {:user_2, %{name: "Bob"}, 200}
      ]
      put_all_newer(:my_buffer, entries)

  """
  @spec put_all_newer(buffer(), [{any(), any(), integer()}], keyword()) :: :ok
  def put_all_newer(buffer, entries, opts \\ []) when is_list(entries) do
    opts = Options.validate_runtime_options!(opts)
    partition_key = Keyword.fetch!(opts, :partition_key)

    entries
    |> Enum.group_by(fn {key, _value, _version} ->
      PartitionedBuffer.get_partition(buffer, partition_key, key)
    end)
    |> Enum.each(fn {partition, entries} ->
      entries =
        Enum.map(entries, fn
          {key, value, version} when is_integer(version) ->
            Partition.new_entry(key, value, version)

          other ->
            raise ArgumentError, "invalid entry: #{inspect(other)}"
        end)

      Partition.put_newer(partition, entries)
    end)
  end

  @doc """
  Gets the value for the given `key` from the buffer's current write table.

  Returns `default` if the key is not found.

  Note: This reads from the current write table only. Entries already handed
  off for processing will not be visible.

  ## Parameters

    * `buffer` - The buffer name (atom).
    * `key` - The key to look up.
    * `default` - The default value if key is not found (defaults to `nil`).
    * `opts` - Optional runtime options.

  ## Options

  See `PartitionedBuffer` for
  [runtime](`m:PartitionedBuffer#module-runtime-options`) options.

  ## Examples

      # Simple get
      get(:my_buffer, :key1)

      # With custom partition routing
      get(:my_buffer, :key1, partition_key: fn {k, _v} -> k end)

  """
  @spec get(buffer(), any(), any(), keyword()) :: any()
  def get(buffer, key, default \\ nil, opts \\ []) do
    opts = Options.validate_runtime_options!(opts)
    partition_key = Keyword.fetch!(opts, :partition_key)
    partition = PartitionedBuffer.get_partition(buffer, partition_key, key)

    Partition.get(partition, key, default)
  end

  @doc """
  Deletes a key from the buffer's current write table.

  Note: If the entry has already been handed off for processing (via
  double-buffering), this delete will not affect the in-flight batch.

  ## Parameters

    * `buffer` - The buffer name (atom).
    * `key` - The key to delete.
    * `opts` - Optional runtime options.

  ## Options

  See `PartitionedBuffer` for
  [runtime](`m:PartitionedBuffer#module-runtime-options`) options.

  ## Examples

      # Simple delete
      delete(:my_buffer, :key1)

      # With custom partition routing
      delete(:my_buffer, :key1, partition_key: fn {k, _v} -> k end)

  """
  @spec delete(buffer(), any(), keyword()) :: :ok
  def delete(buffer, key, opts \\ []) do
    opts = Options.validate_runtime_options!(opts)
    partition_key = Keyword.fetch!(opts, :partition_key)
    partition = PartitionedBuffer.get_partition(buffer, partition_key, key)

    Partition.delete(partition, key)
  end

  @doc """
  Returns the map buffer size (total number of entries across all partitions).

  ## Examples

      size(:my_buffer)

  """
  @spec size(buffer()) :: non_neg_integer()
  defdelegate size(buffer), to: PartitionedBuffer, as: :buffer_size

  @doc """
  Updates the options for the map buffer.

  ## Options

  See `PartitionedBuffer.update_options/2` for the updatable options.

  ## Examples

      # Update the processing interval to 100ms
      update_options(:my_buffer, processing_interval_ms: 100)

  """
  @spec update_options(buffer(), keyword()) :: :ok
  defdelegate update_options(buffer, opts), to: PartitionedBuffer

  ## Callbacks

  @impl true
  def ets_type, do: :set
end
