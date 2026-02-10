defmodule PartitionedBuffer.Partition do
  @moduledoc """
  Buffer partition.

  The implementation is based on
  [OpenTelemetry Batch Processor][otel_batch_processor].
  The use case is very similar. The **"OTel batch processor"** buffers spans
  (large/massive amounts of them) and then exports them to an external source
  after some time (via OTLP). It is designed and implemented for efficiently
  handling large workloads. The partitioned buffer takes inspiration from the
  **"OTel batch processor"** to leverage all these properties.

  [otel_batch_processor]: https://github.com/open-telemetry/opentelemetry-erlang/blob/main/apps/opentelemetry/src/otel_batch_processor.erl
  """

  use GenServer

  import Record, only: [defrecordp: 2]

  # Internal state
  defstruct buffer: nil,
            partition: nil,
            partition_index: nil,
            table_1: nil,
            table_2: nil,
            ets_type: nil,
            processor: nil,
            processing_interval_ms: nil,
            processing_timeout_ms: nil,
            processing_batch_size: nil,
            task_supervisor_name: nil,
            runner_task: nil,
            handed_off_table: nil,
            timer_ref: nil,
            processing?: false,
            start_time: nil

  # Table record (generic container for any key/value entry)
  defrecordp(:entry, key: nil, value: nil, version: 0)

  @typedoc "Entry record"
  @type entry() :: record(:entry, key: any(), value: any(), version: integer())

  # Telemetry prefix
  @telemetry_prefix [:partitioned_buffer, :partition]

  # Inline instructions
  @compile [inline: [new_entry: 2, new_entry: 3]]

  ## API

  @doc """
  Starts a queue partition.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Puts a batch of pre-built entries into the given `partition`.

  Each entry must be created with `new_entry/3`.
  """
  @spec put(atom(), [entry()]) :: :ok
  def put(partition, entries) when is_list(entries) do
    true =
      partition
      |> get_current_table()
      |> :ets.insert(entries)

    :ok
  end

  @doc """
  Puts a batch of versioned entries into the given `partition`.

  Uses "newer version wins" semantics: an entry is only written if:
  - The key doesn't exist (insert), or
  - The new version is greater than the existing version (conditional update)

  Each entry must be created with `new_entry/3`.
  """
  @spec put_newer(atom(), [entry()]) :: :ok
  def put_newer(partition, entries) when is_list(entries) do
    table = get_current_table(partition)

    Enum.each(entries, fn entry(key: key, value: value, version: version) = entry ->
      # Try to insert as new entry first
      case :ets.insert_new(table, entry) do
        true ->
          # Entry was new, we're done
          :ok

        false ->
          # Entry exists, try conditional update with select_replace
          match_spec = replace_match_spec(key, value, version)

          :ets.select_replace(table, match_spec)
      end
    end)
  end

  @doc """
  Gets the value for the given `key` from the `partition`'s current ETS table.

  Returns `default` if the key is not found.
  """
  @spec get(atom(), any(), any()) :: any()
  def get(partition, key, default \\ nil) do
    case partition |> get_current_table() |> :ets.lookup(key) do
      [entry(value: value)] -> value
      [] -> default
    end
  end

  @doc """
  Deletes an entry by key from the given `partition`.
  """
  @spec delete(atom(), any()) :: :ok
  def delete(partition, key) do
    true =
      partition
      |> get_current_table()
      |> :ets.delete(key)

    :ok
  end

  @doc """
  Returns the partition's buffer size.
  """
  @spec buffer_size(atom()) :: non_neg_integer()
  def buffer_size(partition) do
    partition
    |> get_current_table()
    |> :ets.info(:size)
  end

  @doc """
  Creates a new entry.
  """
  @spec new_entry(any(), any(), integer()) :: entry()
  def new_entry(key, value, version \\ 0) when is_integer(version) do
    entry(key: key, value: value, version: version)
  end

  ## GenServer callbacks

  @impl true
  def init(opts) do
    # Trap exit signals (make sure dying gracefully)
    Process.flag(:trap_exit, true)

    # Get options
    buffer = Keyword.fetch!(opts, :name)
    module = Keyword.fetch!(opts, :module)
    partition_index = Keyword.fetch!(opts, :partition_index)
    processing_interval = Keyword.fetch!(opts, :processing_interval_ms)
    processing_timeout = Keyword.fetch!(opts, :processing_timeout_ms)
    processing_batch_size = Keyword.fetch!(opts, :processing_batch_size)
    processor = Keyword.fetch!(opts, :processor)

    # Get the ETS table type from the implementation module
    ets_type = module.ets_type()

    # Generate the partition name
    partition = Module.concat([buffer, to_string(partition_index)])

    table1 =
      [partition, Table1]
      |> Module.concat()
      |> new_processing_table(ets_type)

    table2 =
      [partition, Table2]
      |> Module.concat()
      |> new_processing_table(ets_type)

    :ok = put_current_table(partition, table1)

    # Register the partition
    with {:ok, _} <- Registry.register(buffer, buffer, partition) do
      start_time = System.monotonic_time()

      # Emit start event
      :telemetry.execute(
        @telemetry_prefix ++ [:start],
        %{system_time: System.system_time()},
        %{buffer: buffer, partition: partition}
      )

      # Build the state
      state = %__MODULE__{
        buffer: buffer,
        partition: partition,
        partition_index: partition_index,
        table_1: table1,
        table_2: table2,
        ets_type: ets_type,
        processor: processor,
        processing_interval_ms: processing_interval,
        processing_timeout_ms: processing_timeout,
        processing_batch_size: processing_batch_size,
        task_supervisor_name: Module.concat([buffer, TaskSupervisor]),
        start_time: start_time
      }

      {:ok, state, {:continue, :start_processing_timer}}
    end
  end

  @impl true
  def handle_continue(:start_processing_timer, state) do
    {:noreply, refresh_timer(state)}
  end

  @impl true
  def handle_info(message, state)

  # It's time to process the messages, but there is a process in progress already
  def handle_info(:processing, %__MODULE__{processing?: true} = state) do
    # Postpone the next processing
    {:noreply, refresh_timer(state)}
  end

  # It's time to process the messages
  def handle_info(:processing, %__MODULE__{processing?: false} = state) do
    # Process messages and reset the processing interval
    state =
      state
      |> process_messages()
      |> refresh_timer()

    {:noreply, state}
  end

  # Process task completed successfully
  def handle_info(
        {ref, :processing_completed},
        %__MODULE__{runner_task: %Task{ref: ref}} = state
      ) do
    # We don't care about the DOWN message now, so let's demonitor and flush it
    Process.demonitor(ref, [:flush])

    # Make sure to complete the processing properly
    {:noreply, complete_processing(state)}
  end

  # Process task failed
  def handle_info(
        {:DOWN, ref, :process, _from, reason},
        %__MODULE__{runner_task: %Task{ref: ref}, buffer: buffer, partition: partition} =
          state
      ) do
    # Emit processing task failed event
    :telemetry.execute(
      @telemetry_prefix ++ [:processing_failed],
      %{system_time: System.system_time()},
      %{buffer: buffer, partition: partition, reason: reason}
    )

    {:noreply, complete_processing(state)}
  end

  @impl true
  def terminate(
        reason,
        %__MODULE__{
          buffer: buffer,
          partition: partition,
          start_time: start_time,
          ets_type: ets_type,
          processing_batch_size: batch_size,
          processor: processor
        }
      ) do
    # Emit stop event
    :telemetry.execute(
      @telemetry_prefix ++ [:stop],
      %{duration: System.monotonic_time() - start_time},
      %{buffer: buffer, partition: partition, reason: reason}
    )

    # Process messages before dying
    # `process_batch' is used to perform a blocking process
    partition
    |> get_current_table()
    |> process_batch(ets_type, batch_size, processor)
  end

  ## Private functions

  # The processing is performed asynchronously to better handle the
  # read-and-write concurrency.
  defp process_messages(
         %__MODULE__{
           buffer: buffer,
           partition: partition,
           table_1: table1,
           table_2: table2,
           ets_type: ets_type,
           task_supervisor_name: task_supervisor_name,
           processor: processor,
           processing_timeout_ms: processing_timeout,
           processing_batch_size: batch_size
         } = state
       ) do
    # Get the current writing ETS table.
    current_table = get_current_table(partition)

    # Get the current table size
    size = :ets.info(current_table, :size)

    # Check if the current table has data to process
    if size > 0 do
      # Resolve what of the two available ETS tables should be the NEW
      # current table to continue handling the writes.
      new_current_table =
        case current_table do
          ^table1 -> table2
          ^table2 -> table1
        end

      # Point the "current table" to the other available ETS table.
      # The NEW table will continue handling the writes while the
      # previous table is isolated for processing.
      :ok = put_current_table(partition, new_current_table)

      # Get the current process so the task can send the result back to it
      self = self()

      # Spawn a separate task to run the processing on the previous table
      task =
        Task.Supervisor.async_nolink(
          task_supervisor_name,
          fn -> send_messages(self, buffer, partition, size, ets_type, batch_size, processor) end,
          shutdown: processing_timeout
        )

      # Give away the previous current table to the processing task to isolate
      # the process operation. Since the processing task is the new table owner,
      # the table can be deleted after completing the process; we don't need to
      # delete the keys one by one, which could be expensive.
      true = :ets.give_away(current_table, task.pid, :process)

      # Update the state acknowledging the process is in the "processing" state.
      %{state | processing?: true, runner_task: task, handed_off_table: current_table}
    else
      # Nothing to do if the table is empty
      state
    end
  end

  # Function for handling the processing asynchronously
  defp send_messages(from, buffer, partition, size, ets_type, batch_size, processor) do
    # Trap exits for the `:shutdown` timeout to have an effect
    # See `Task.Supervisor.async_nolink/3` for more info
    Process.flag(:trap_exit, true)

    # Telemetry metadata for the span
    metadata = %{
      buffer: buffer,
      partition: partition
    }

    # Emit a Telemetry span to keep track of the processing duration
    :telemetry.span(@telemetry_prefix ++ [:processing], metadata, fn ->
      # Receive the table transfer message
      receive do
        {:"ETS-TRANSFER", table, ^from, :process} ->
          # Process the table data in batches to optimize the memory footprint
          # (avoid loading the entire table into the memory)
          :ok = process_batch(table, ets_type, batch_size, processor)

          # We can safely delete the table since the data is already processed
          # and the current buffer points to another table
          true = :ets.delete(table)

          # Acknowledge the process is completed
          {:processing_completed, %{size: size}, metadata}
      end
    end)
  end

  # Complete the processing
  defp complete_processing(
         %__MODULE__{handed_off_table: processing_table, ets_type: ets_type} = state
       ) do
    # Since the processing task is completed, we can safely create
    # a new processing table reusing its name
    ^processing_table = new_processing_table(processing_table, ets_type)

    # Reset the state
    %{state | processing?: false, runner_task: nil, handed_off_table: nil}
  end

  # We're starting!
  defp process_batch(table, ets_type, batch_size, processor) do
    table
    |> :ets.select(ets_match_spec(ets_type), batch_size)
    |> process_batch(processor)
  end

  # We're finished!
  defp process_batch(:"$end_of_table", _processor) do
    :ok
  end

  # We're continuing!
  defp process_batch({results, continuation}, processor) do
    # Invoke the processor function
    processor.(results)

    # Continue processing the next batch
    continuation
    |> :ets.select()
    |> process_batch(processor)
  end

  # ETS match-spec based on the buffer type:
  # - :ordered_set (Queue): return only the value
  # - :set (Map): return {key, value} tuple
  defp ets_match_spec(type)

  defp ets_match_spec(:ordered_set) do
    [
      {
        entry(key: :"$1", value: :"$2", version: :_),
        [true],
        [:"$2"]
      }
    ]
  end

  defp ets_match_spec(:set) do
    [
      {
        entry(key: :"$1", value: :"$2", version: :_),
        [true],
        [{{:"$1", :"$2"}}]
      }
    ]
  end

  defp replace_match_spec(key, value, version) do
    # Performance note: The key in the match head is a literal (bound value),
    # not a pattern variable. This allows ETS to use its hash index for O(1)
    # lookup rather than scanning the entire table.
    [
      {
        # Match: {entry, key, value, existing_version} where key is literal
        entry(key: key, value: :_, version: :"$1"),
        # Guard (update only if): new_version > existing_version
        [{:>, version, :"$1"}],
        # Result: the new entry
        [{entry(key: key, value: value, version: version)}]
      }
    ]
  end

  defp refresh_timer(%__MODULE__{timer_ref: timer_ref, processing_interval_ms: interval} = state) do
    if timer_ref, do: Process.cancel_timer(timer_ref)

    timer_ref = Process.send_after(self(), :processing, interval)

    %{state | timer_ref: timer_ref}
  end

  defp new_processing_table(name, ets_type) do
    :ets.new(name, [
      ets_type,
      :named_table,
      :public,
      keypos: entry(:key) + 1,
      write_concurrency: true,
      decentralized_counters: true
    ])
  end

  @compile {:inline, current_table_key: 1}
  defp current_table_key(partition) do
    {__MODULE__, :current_table, partition}
  end

  @compile {:inline, get_current_table: 1}
  defp get_current_table(partition) do
    partition
    |> current_table_key()
    |> :persistent_term.get()
  end

  defp put_current_table(partition, tab_name) do
    # An atom is a single word so this does not trigger a global GC
    partition
    |> current_table_key()
    |> :persistent_term.put(tab_name)
  end
end
