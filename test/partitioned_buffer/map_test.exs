defmodule PartitionedBuffer.MapTest do
  use ExUnit.Case, async: false

  alias PartitionedBuffer.Map, as: M

  # Omit logs during the tests
  @moduletag capture_log: true

  # Default waiting time when receiving a message (e.g., assert_receive ...)
  @default_timeout :timer.seconds(5)

  # Telemetry events
  @partition_stop_event [:partitioned_buffer, :partition, :stop]
  @processing_start_event [:partitioned_buffer, :partition, :processing, :start]
  @processing_stop_event [:partitioned_buffer, :partition, :processing, :stop]
  @processing_failed_event [:partitioned_buffer, :partition, :processing_failed]

  @telemetry_events [
    @partition_stop_event,
    @processing_start_event,
    @processing_stop_event,
    @processing_failed_event
  ]

  setup do
    handler_id = attach_telemetry_handler(@telemetry_events)

    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok
  end

  describe "stop/3" do
    test "ok: stops the buffer" do
      {:ok, _} =
        M.start_link(
          name: :map_stop_buff,
          processor: &__MODULE__.test_processor(self(), &1),
          partitions: 1
        )

      assert M.stop(:map_stop_buff) == :ok
    end
  end

  describe "put/4 and put_all/3" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {M,
           name: __MODULE__,
           processing_interval_ms: 500,
           processing_batch_size: 5,
           processor: &__MODULE__.test_processor(self, &1),
           partitions: 1}
        )

      {:ok, buffer: __MODULE__, pid: pid}
    end

    test "ok: single put is processed", %{buffer: buff} do
      assert M.size(buff) == 0

      assert M.put(buff, :key1, "value1") == :ok

      assert M.size(buff) == 1
      assert M.get(buff, :key1) == "value1"

      # Wait for processing to complete
      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, [{:key1, "value1", 0, 0}]}, @default_timeout
    end

    test "ok: batch put with list of tuples", %{buffer: buff} do
      entries = [{:a, 1}, {:b, 2}, {:c, 3}]

      assert M.put_all(buff, entries) == :ok

      assert M.size(buff) == 3

      # Wait for processing to complete
      assert_receive {@processing_stop_event, %{duration: _, size: 3},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, batch}, @default_timeout
      assert Enum.sort(batch) == [{:a, 1, 0, 0}, {:b, 2, 0, 0}, {:c, 3, 0, 0}]
    end

    test "ok: batch put with map", %{buffer: buff} do
      entries = %{x: "hello", y: "world"}

      assert M.put_all(buff, entries) == :ok

      assert M.size(buff) == 2

      # Wait for processing to complete
      assert_receive {@processing_stop_event, %{duration: _, size: 2},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, batch}, @default_timeout
      assert Enum.sort(batch) == [{:x, "hello", 0, 0}, {:y, "world", 0, 0}]
    end

    test "ok: last-write-wins for duplicate keys", %{buffer: buff} do
      assert M.put(buff, :dup, "first") == :ok
      assert M.get(buff, :dup) == "first"

      assert M.put(buff, :dup, "second") == :ok
      assert M.get(buff, :dup) == "second"

      # Only one entry should remain (last write wins)
      assert M.size(buff) == 1

      # Wait for processing to complete
      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, [{:dup, "second", 0, 0}]}, @default_timeout
    end

    test "ok: entries are processed in batches", %{buffer: buff} do
      entries = Enum.map(1..10, fn i -> {:"key_#{i}", i} end)

      assert M.put_all(buff, entries) == :ok

      # Wait for processing to complete
      assert_receive {@processing_stop_event, %{duration: _, size: 10},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      # Should receive two batches of 5 (processing_batch_size is 5)
      assert_receive {:process_completed, batch1}, @default_timeout
      assert_receive {:process_completed, batch2}, @default_timeout
      assert length(batch1) == 5
      assert length(batch2) == 5

      # Extract values from {key, value, version, updates} tuples and sort
      values = Enum.map(batch1 ++ batch2, fn {_k, v, _version, _u} -> v end)
      assert Enum.sort(values) == Enum.to_list(1..10)
    end

    test "ok: entries are partitioned using a function", %{buffer: buff} do
      assert M.put(buff, :key1, "value1", partition_key: &__MODULE__.partition_key_fun/1) == :ok

      assert M.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout
    end

    test "ok: entries are partitioned using an MFA tuple", %{buffer: buff} do
      assert M.put(buff, :key1, "value1", partition_key: {__MODULE__, :partition_key_fun, []}) ==
               :ok

      assert M.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout
    end

    test "ok: entries are partitioned using a static key", %{buffer: buff} do
      assert M.put(buff, :key1, "value1", partition_key: :static) == :ok

      assert M.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout
    end

    test "error: no partitions available for the given buffer" do
      {:ok, buff} =
        M.start_link(
          name: :map_error_buff,
          processor: &__MODULE__.test_processor(self(), &1),
          partitions: 0
        )

      assert_raise RuntimeError, ~r"no partitions available for buffer :map_error_buff", fn ->
        M.put(:map_error_buff, :key, "value")
      end

      assert M.stop(buff) == :ok
    end
  end

  describe "get/3" do
    setup do
      self = self()

      pid =
        start_supervised!({M, name: __MODULE__, processor: &__MODULE__.test_processor(self, &1)})

      {:ok, buffer: __MODULE__, pid: pid}
    end

    test "ok: returns value for existing key", %{buffer: buff} do
      :ok = M.put(buff, :key1, "value1")

      assert M.get(buff, :key1) == "value1"
    end

    test "ok: returns nil for missing key", %{buffer: buff} do
      assert M.get(buff, :missing) == nil
    end

    test "ok: returns custom default for missing key", %{buffer: buff} do
      assert M.get(buff, :missing, :not_found) == :not_found
    end

    test "ok: reflects latest value after overwrite", %{buffer: buff} do
      :ok = M.put(buff, :key1, "v1")
      assert M.get(buff, :key1) == "v1"

      :ok = M.put(buff, :key1, "v2")
      assert M.get(buff, :key1) == "v2"
    end
  end

  describe "delete/3" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {M,
           name: __MODULE__,
           processing_interval_ms: 1_000,
           processing_batch_size: 10,
           processor: &__MODULE__.test_processor(self, &1),
           partitions: 1}
        )

      {:ok, buffer: __MODULE__, pid: pid}
    end

    test "ok: put then delete before processing", %{buffer: buff} do
      assert M.put(buff, :to_delete, "gone") == :ok
      assert M.put(buff, :to_keep, "stays") == :ok
      assert M.size(buff) == 2

      # Verify both exist before delete
      assert M.get(buff, :to_delete) == "gone"
      assert M.get(buff, :to_keep) == "stays"

      assert M.delete(buff, :to_delete) == :ok
      assert M.size(buff) == 1

      # Verify deleted key is gone and kept key remains
      assert M.get(buff, :to_delete) == nil
      assert M.get(buff, :to_keep) == "stays"

      # Wait for processing to complete
      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      # Only the kept entry should be processed
      assert_receive {:process_completed, [{:to_keep, "stays", 0, 0}]}, @default_timeout
    end
  end

  describe "put_newer/5 and put_all_newer/3" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {M,
           name: __MODULE__,
           processing_interval_ms: 500,
           processing_batch_size: 10,
           processor: &__MODULE__.test_processor(self, &1),
           partitions: 1}
        )

      {:ok, buffer: __MODULE__, pid: pid}
    end

    test "ok: inserts new entry", %{buffer: buff} do
      assert M.put_newer(buff, :key1, "value1", 100) == :ok

      assert M.size(buff) == 1
      assert M.get(buff, :key1) == "value1"

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, [{:key1, "value1", 100, 0}]}, @default_timeout
    end

    test "ok: newer version wins", %{buffer: buff} do
      assert M.put_newer(buff, :key1, "v1", 100) == :ok
      assert M.get(buff, :key1) == "v1"

      # Newer version should overwrite
      assert M.put_newer(buff, :key1, "v2", 200) == :ok
      assert M.get(buff, :key1) == "v2"

      assert M.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, [{:key1, "v2", 200, 1}]}, @default_timeout
    end

    test "ok: older version is ignored", %{buffer: buff} do
      assert M.put_newer(buff, :key1, "v1", 200) == :ok
      assert M.get(buff, :key1) == "v1"

      # Older version should be ignored
      assert M.put_newer(buff, :key1, "v2", 100) == :ok
      assert M.get(buff, :key1) == "v1"

      assert M.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, [{:key1, "v1", 200, 0}]}, @default_timeout
    end

    test "ok: same version is ignored", %{buffer: buff} do
      assert M.put_newer(buff, :key1, "v1", 100) == :ok
      assert M.get(buff, :key1) == "v1"

      # Same version should be ignored
      assert M.put_newer(buff, :key1, "v2", 100) == :ok
      assert M.get(buff, :key1) == "v1"

      assert M.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, [{:key1, "v1", 100, 0}]}, @default_timeout
    end

    test "ok: put_all_newer with multiple entries", %{buffer: buff} do
      entries = [
        {:a, "val_a", 100},
        {:b, "val_b", 200},
        {:c, "val_c", 300}
      ]

      assert M.put_all_newer(buff, entries) == :ok

      assert M.size(buff) == 3
      assert M.get(buff, :a) == "val_a"
      assert M.get(buff, :b) == "val_b"
      assert M.get(buff, :c) == "val_c"

      assert_receive {@processing_stop_event, %{duration: _, size: 3},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, batch}, @default_timeout

      assert Enum.sort(batch) == [
               {:a, "val_a", 100, 0},
               {:b, "val_b", 200, 0},
               {:c, "val_c", 300, 0}
             ]
    end

    test "ok: put_all_newer respects version ordering", %{buffer: buff} do
      # Insert initial values
      initial = [
        {:a, "a1", 100},
        {:b, "b1", 200}
      ]

      assert M.put_all_newer(buff, initial) == :ok

      # Update with mixed versions
      updates = [
        {:a, "a2", 200},
        {:b, "b2", 100}
      ]

      assert M.put_all_newer(buff, updates) == :ok

      # :a should be updated (200 > 100), :b should not (100 < 200)
      assert M.get(buff, :a) == "a2"
      assert M.get(buff, :b) == "b1"

      assert_receive {@processing_stop_event, %{duration: _, size: 2},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, batch}, @default_timeout
      assert Enum.sort(batch) == [{:a, "a2", 200, 1}, {:b, "b1", 200, 0}]
    end

    test "ok: works with timestamp versions", %{buffer: buff} do
      t1 = System.monotonic_time()
      assert M.put_newer(buff, :event, %{data: "first"}, t1) == :ok

      t2 = System.monotonic_time()
      assert M.put_newer(buff, :event, %{data: "second"}, t2) == :ok

      # t2 > t1, so second should win
      assert M.get(buff, :event) == %{data: "second"}

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout
    end

    test "ok: updates existing entry with tuple key and higher version", %{buffer: buff} do
      assert M.put_newer(buff, {1, "key1"}, {"value0"}, 100) == :ok
      assert M.put_newer(buff, {1, "key1"}, {"value1"}, 200) == :ok

      assert M.size(buff) == 1
      assert M.get(buff, {1, "key1"}) == {"value1"}

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, [{{1, "key1"}, {"value1"}, 200, 1}]}, @default_timeout
    end

    test "ok: updates existing entry with list key and list value containing tuples",
         %{buffer: buff} do
      key = ["account1", "user1", "event"]
      value0 = [{1, "old"}, {2, "data"}]
      value1 = [{1, "new"}, {3, "updated"}]

      assert M.put_newer(buff, key, value0, 100) == :ok
      assert M.put_newer(buff, key, value1, 200) == :ok

      assert M.size(buff) == 1
      assert M.get(buff, key) == value1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, [{^key, ^value1, 200, 1}]}, @default_timeout
    end

    test "error: put_newer raises ArgumentError for non-integer version", %{buffer: buff} do
      assert_raise ArgumentError, ~r/invalid entry/, fn ->
        M.put_newer(buff, :key1, "value1", "not_an_integer")
      end
    end

    test "error: put_all_newer raises ArgumentError for invalid entry", %{buffer: buff} do
      entries = [
        {:a, "val_a", 100},
        {:b, "val_b", "not_an_integer"}
      ]

      assert_raise ArgumentError, ~r/invalid entry/, fn ->
        M.put_all_newer(buff, entries)
      end
    end
  end

  describe "processing" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {M,
           name: __MODULE__,
           processing_interval_ms: 10,
           processing_batch_size: 5,
           partitions: 1,
           processor: &__MODULE__.test_processor(self, &1)}
        )

      {:ok, buffer: __MODULE__, pid: pid}
    end

    test "error: task fails", %{buffer: buff} do
      :ok = M.put(buff, :err, %{error: true})

      # Wait for the processing task to fail and emit the event
      assert_receive {@processing_failed_event, %{system_time: _},
                      %{buffer: ^buff, partition: _, reason: {%RuntimeError{}, _stacktrace}}},
                     @default_timeout
    end

    test "error: task exits", %{buffer: buff} do
      Process.flag(:trap_exit, true)

      :ok = M.put(buff, :exit_key, %{exit: :exit})

      # Wait for the processing task to exit and emit the event
      assert_receive {@processing_failed_event, %{system_time: _},
                      %{buffer: ^buff, partition: _, reason: :exit}},
                     @default_timeout
    end

    test "shutdown: partition handles supervisor kill", %{pid: pid} do
      Process.flag(:trap_exit, true)

      assert Supervisor.stop(pid, :kill) == :ok

      # Verify the partition was stopped and emitted the telemetry event
      assert_receive {@partition_stop_event, %{duration: _},
                      %{buffer: _, partition: _, reason: :shutdown}},
                     @default_timeout
    end

    test "shutdown: entries are processed before dying", %{buffer: buff, pid: pid} do
      # Wait for the processing interval (no entries were written yet)
      :ok = Process.sleep(100)

      # Put an entry with custom sleep time
      :ok = M.put(buff, :slow, %{sleep_ms: 500, data: "first"})

      # Make sure the processing started
      assert_receive {@processing_start_event, %{}, %{buffer: ^buff, partition: _}},
                     @default_timeout

      # Wait for the next processing interval
      :ok = Process.sleep(100)

      # Put another entry while processing is running
      :ok = M.put(buff, :fast, %{id: 2, data: "second"})

      # Stop the buffer normally
      assert Supervisor.stop(pid) == :ok

      # Verify the partition was stopped and emitted the telemetry event
      assert_receive {@partition_stop_event, %{duration: _},
                      %{buffer: ^buff, partition: _, reason: :shutdown}},
                     @default_timeout

      # Verify the entries were processed (Map returns {key, value, version, updates} tuples)
      assert_receive {:process_completed, [{:slow, %{sleep_ms: 500, data: "first"}, 0, 0}]},
                     @default_timeout

      assert_receive {:process_completed, [{:fast, %{id: 2, data: "second"}, 0, 0}]},
                     @default_timeout
    end
  end

  describe "processor as MFA" do
    setup do
      self = self()

      start_supervised!(
        {M,
         name: :map_mfa_test,
         processing_interval_ms: 500,
         processing_batch_size: 5,
         processor: {__MODULE__, :mfa_processor, [self]},
         partitions: 1}
      )

      {:ok, buffer: :map_mfa_test}
    end

    test "ok: processes entries using MFA processor", %{buffer: buff} do
      assert M.put(buff, :key1, "value1") == :ok

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, [{:key1, "value1", 0, 0}]}, @default_timeout
    end
  end

  describe "update_options/2" do
    setup do
      self = self()

      start_supervised!(
        {M, name: __MODULE__, processor: &__MODULE__.test_processor(self, &1), partitions: 1}
      )

      {:ok, buffer: __MODULE__}
    end

    test "ok: updates the given options", %{buffer: buff} do
      # Lower the interval so processing triggers
      assert M.update_options(buff, processing_interval_ms: 200, processing_batch_size: 2) == :ok

      # Insert 4 entries
      assert M.put_all(buff, [{:a, 1}, {:b, 2}, {:c, 3}, {:d, 4}]) == :ok

      # Wait for processing to complete (after 200ms)
      assert_receive {@processing_stop_event, %{duration: _, size: 4},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      # Should receive 2 batches of 2 (batch_size is now 2)
      assert_receive {:process_completed, batch1}, @default_timeout
      assert_receive {:process_completed, batch2}, @default_timeout
      assert length(batch1) == 2
      assert length(batch2) == 2
    end

    test "error: raises on invalid options", %{buffer: buff} do
      assert_raise NimbleOptions.ValidationError, fn ->
        M.update_options(buff, processing_interval_ms: -1)
      end
    end
  end

  describe "processing_batch_size: :table" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {M,
           name: :map_table_test,
           processing_interval_ms: 500,
           processing_batch_size: :table,
           processor: &__MODULE__.table_processor(self, &1),
           partitions: 1}
        )

      {:ok, buffer: :map_table_test, pid: pid}
    end

    test "ok: processor receives the ETS table", %{buffer: buff} do
      assert M.put_all(buff, [{:a, 1}, {:b, 2}, {:c, 3}]) == :ok

      assert_receive {@processing_stop_event, %{duration: _, size: 3},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:table_processed, table_name, 3}, @default_timeout
      assert is_atom(table_name)
    end

    test "ok: processor can rename and give away the table", %{buffer: buff} do
      assert M.put_all(buff, [{:x, 10}, {:y, 20}]) == :ok

      assert_receive {@processing_stop_event, %{duration: _, size: 2},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:table_kept, kept_table}, @default_timeout

      # The renamed table should still exist and be accessible
      assert :ets.info(kept_table, :size) == 2

      # Clean up
      :ets.delete(kept_table)
    end
  end

  describe "partitioning" do
    setup do
      self = self()

      start_supervised!(
        {M,
         name: :map_partitioning_test,
         processing_interval_ms: 500,
         processing_batch_size: 10,
         partitions: 2,
         processor: &__MODULE__.test_processor(self, &1)}
      )

      {:ok, buffer: :map_partitioning_test}
    end

    test "ok: all entries are processed across partitions", %{buffer: buff} do
      entries = Enum.map(1..10, fn i -> {:"key_#{i}", %{id: i, data: "val#{i}"}} end)

      # Put entries
      :ok = M.put_all(buff, entries)

      # Make sure processing completed on both partitions
      assert_receive {@processing_stop_event, %{duration: _, size: s1},
                      %{buffer: :map_partitioning_test, partition: p1}},
                     @default_timeout

      assert_receive {@processing_stop_event, %{duration: _, size: s2},
                      %{buffer: :map_partitioning_test, partition: p2}},
                     @default_timeout

      # Check the total processed entries
      assert s1 + s2 == 10

      # Entries should have been routed to different partitions
      assert p1 != p2
    end
  end

  ## Helpers

  # Map processor receives {key, value, version, updates} tuples
  def test_processor(_pid, [{_key, %{error: true}, _version, _updates} | _]) do
    raise "task error"
  end

  def test_processor(_pid, [{_key, %{exit: reason}, _version, _updates} | _]) do
    exit(reason)
  end

  def test_processor(pid, [{_key, %{sleep_ms: time_ms}, _version, _updates} | _] = chunk) do
    :ok = Process.sleep(time_ms)

    send(pid, {:process_completed, chunk})
  end

  def test_processor(pid, chunk) do
    # Simulate processing time
    :ok = Process.sleep(200)

    send(pid, {:process_completed, chunk})
  end

  # MFA-compatible processor (batch is prepended to args)
  def mfa_processor(chunk, pid) do
    :ok = Process.sleep(200)

    send(pid, {:process_completed, chunk})
  end

  # Table mode processor: reads the table and sends results back
  def table_processor(pid, table) when is_atom(table) do
    size = :ets.info(table, :size)

    # On first call, just report the table; on second call, rename and keep it
    if size > 2 do
      send(pid, {:table_processed, table, size})
    else
      # Rename to free the original name, give away to the test process
      # so the table survives the task exit, then notify
      new_name = Module.concat([table, Copy])
      :ets.rename(table, new_name)

      if Process.alive?(pid) do
        true = :ets.give_away(new_name, pid, :kept)

        send(pid, {:table_kept, new_name})
      end
    end
  end

  def partition_key_fun(key) do
    key
  end

  defp attach_telemetry_handler(handler_id \\ self(), events) do
    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        &__MODULE__.handle_event/4,
        %{pid: self()}
      )

    handler_id
  end

  def handle_event(event, measurements, metadata, %{pid: pid}) do
    send(pid, {event, measurements, metadata})
  end
end
