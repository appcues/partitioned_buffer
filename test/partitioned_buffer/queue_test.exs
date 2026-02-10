defmodule PartitionedBuffer.QueueTest do
  use ExUnit.Case, async: false

  alias PartitionedBuffer.Queue, as: Q

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

  describe "push/3" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {Q,
           name: __MODULE__,
           processing_interval_ms: 500,
           processing_batch_size: 5,
           processor: &__MODULE__.test_processor(self, &1),
           partitions: 1}
        )

      {:ok, buffer: __MODULE__, pid: pid}
    end

    test "ok: messages are processed in ordered batches", %{buffer: buff} do
      assert Q.size(buff) == 0

      {expected_batch1, expected_batch2} =
        Enum.map(1..10, fn i ->
          %{id: i, data: "message#{i}"}
        end)
        |> Enum.split(5)

      # Push messages
      assert Q.push(buff, expected_batch1 ++ expected_batch2) == :ok

      # Make sure the processing completed
      assert_receive {@processing_stop_event, %{duration: _, size: 10},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      # Push more messages while processing
      assert Q.push(buff, expected_batch1) == :ok

      # Check the processed messages
      assert_receive {:process_completed, ^expected_batch1}, @default_timeout
      assert_receive {:process_completed, ^expected_batch2}, @default_timeout
      assert_receive {:process_completed, ^expected_batch1}, @default_timeout

      # Push more after processing
      assert Q.push(buff, expected_batch2) == :ok

      # Make sure the processing completed
      assert_receive {@processing_stop_event, %{duration: _, size: 5},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      # Check the processed messages
      assert_receive {:process_completed, ^expected_batch2}, @default_timeout

      assert Q.size(buff) == 0
    end

    test "ok: handles single message pushes", %{buffer: buff} do
      msg = %{id: 1, data: "single"}

      assert Q.push(buff, msg) == :ok

      assert Q.size(buff) == 1

      # Wait for processing to complete
      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, [^msg]}, @default_timeout
    end

    test "ok: messages are partitioned using a function", %{buffer: buff} do
      assert Q.push(buff, %{id: 1, data: "message1"},
               partition_key: &__MODULE__.partition_key_fun/1
             ) == :ok

      assert Q.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout
    end

    test "ok: messages are partitioned using an MFA tuple", %{buffer: buff} do
      assert Q.push(buff, %{id: 1, data: "message1"},
               partition_key: {__MODULE__, :partition_key_fun, []}
             ) == :ok

      assert Q.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout
    end

    test "ok: messages are partitioned using a custom key", %{buffer: buff} do
      assert Q.push(buff, %{id: 1, data: "message1"}, partition_key: 1) ==
               :ok

      assert Q.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout
    end

    test "error: no partitions available for the given buffer" do
      {:ok, buff} =
        Q.start_link(
          name: :error_buff,
          processor: &__MODULE__.test_processor(self(), &1),
          partitions: 0
        )

      assert_raise RuntimeError, ~r"no partitions available for buffer :error_buff", fn ->
        Q.push(:error_buff, "message")
      end

      assert Q.stop(buff) == :ok
    end
  end

  describe "stop/3" do
    test "ok: stops the buffer" do
      {:ok, _} =
        Q.start_link(
          name: :stop_buff,
          processor: &__MODULE__.test_processor(self(), &1),
          partitions: 1
        )

      assert Q.stop(:stop_buff) == :ok
    end
  end

  describe "processing" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {Q,
           name: __MODULE__,
           processing_interval_ms: 10,
           processing_batch_size: 5,
           partitions: 1,
           processor: &__MODULE__.test_processor(self, &1)}
        )

      {:ok, buffer: __MODULE__, pid: pid}
    end

    test "error: task fails", %{buffer: buff} do
      :ok = Q.push(buff, %{error: true})

      # Wait for the processing task to fail and emit the event
      assert_receive {@processing_failed_event, %{system_time: _},
                      %{buffer: ^buff, partition: _, reason: {%RuntimeError{}, _stacktrace}}},
                     @default_timeout
    end

    test "error: task exits", %{buffer: buff} do
      Process.flag(:trap_exit, true)

      :ok = Q.push(buff, %{exit: :exit})

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

    test "shutdown: messages are processed before dying", %{buffer: buff, pid: pid} do
      # Wait for the processing interval (no messages were written yet)
      :ok = Process.sleep(100)

      # Send a message with custom sleep time
      msg1 = %{sleep_ms: 500, data: "first"}
      :ok = Q.push(buff, msg1)

      # Make sure the processing started
      assert_receive {@processing_start_event, %{}, %{buffer: ^buff, partition: _}},
                     @default_timeout

      # Wait for the next processing interval
      :ok = Process.sleep(100)

      # Send another message while the processing is running
      msg2 = %{id: 2, data: "second"}
      :ok = Q.push(buff, msg2)

      # Stop the buffer normally
      assert Supervisor.stop(pid) == :ok

      # Verify the partition was stopped and emitted the telemetry event
      assert_receive {@partition_stop_event, %{duration: _},
                      %{buffer: ^buff, partition: _, reason: :shutdown}},
                     @default_timeout

      # Verify the messages were processed
      assert_receive {:process_completed, [^msg1]}, @default_timeout
      assert_receive {:process_completed, [^msg2]}, @default_timeout
    end
  end

  describe "partitioning" do
    setup do
      self = self()

      _pid =
        start_supervised!(
          {Q,
           name: :partitioning_test,
           processing_interval_ms: 500,
           processing_batch_size: 10,
           partitions: 2,
           processor: &__MODULE__.test_processor(self, &1)}
        )

      {:ok, buffer: :partitioning_test}
    end

    test "ok: all messages are processed across partitions", %{buffer: buff} do
      batch = Enum.map(1..10, fn i -> %{id: i, data: "msg#{i}"} end)

      # Push messages
      :ok = Q.push(buff, batch)

      # Make sure processing completed on both partitions
      assert_receive {@processing_stop_event, %{duration: _, size: s1},
                      %{buffer: :partitioning_test, partition: p1}},
                     @default_timeout

      assert_receive {@processing_stop_event, %{duration: _, size: s2},
                      %{buffer: :partitioning_test, partition: p2}},
                     @default_timeout

      # Check the total processed messages
      assert s1 + s2 == 10

      # Messages should have been routed to different partitions
      assert p1 != p2
    end
  end

  ## Helpers

  def test_processor(_pid, [%{error: true} | _]) do
    raise "task error"
  end

  def test_processor(_pid, [%{exit: reason} | _]) do
    exit(reason)
  end

  def test_processor(pid, [%{sleep_ms: time_ms} | _] = chunk) do
    :ok = Process.sleep(time_ms)

    send(pid, {:process_completed, chunk})
  end

  def test_processor(pid, chunk) do
    # Simulate processing time
    :ok = Process.sleep(200)

    send(pid, {:process_completed, chunk})
  end

  def partition_key_fun(%{id: id}) do
    id
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
