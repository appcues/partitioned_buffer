defmodule PartitionedBuffer.Options do
  @moduledoc false

  # Start options
  start_opts = [
    name: [
      type: :atom,
      required: true,
      doc: """
      The buffer name (used to identify the buffer).
      """
    ],
    processor: [
      type: {:fun, 1},
      required: true,
      doc: """
      A callback function that processes batches of messages. Called with a list
      of accumulated messages and should handle the processing logic (e.g., send
      to external service, persist to database, etc.).
      """
    ],
    processing_interval_ms: [
      type: :pos_integer,
      required: false,
      default: :timer.seconds(5),
      doc: """
      How often (in milliseconds) each partition checks its buffer and initiates
      processing. Messages are processed at this interval if any are buffered.
      Lower values mean faster processing but more frequent task spawning.
      """
    ],
    processing_timeout_ms: [
      type: :timeout,
      required: false,
      default: :timer.minutes(1),
      doc: """
      Maximum time (in milliseconds) for a processing task to complete before
      being forcefully terminated. Used during graceful shutdown to wait for
      in-flight processing to complete. See `Task.Supervisor.async_nolink/3`
      for more information.
      """
    ],
    processing_batch_size: [
      type: :pos_integer,
      required: false,
      default: 10,
      doc: """
      Maximum number of messages to include in a single batch sent to the
      processor callback. Messages are processed in batches of up to this size
      to optimize memory usage and processor performance.
      """
    ],
    partitions: [
      type: :non_neg_integer,
      required: false,
      doc: """
      Number of partitions to create. Each partition has its own buffer and
      processing cycle. Defaults to `System.schedulers_online()` to match the
      number of available schedulers. More partitions reduce lock contention
      but increase per-partition overhead.
      """
    ]
  ]

  auto_opts = [
    module: [
      type: :atom,
      required: true,
      doc: """
      The buffer implementation module (e.g., `PartitionedBuffer.Queue`).

      This option is automatically set when starting a buffer through a
      specific implementation like `PartitionedBuffer.Queue.start_link/1`.
      It must be provided explicitly when calling
      `PartitionedBuffer.start_link/1` directly.
      """
    ]
  ]

  runtime_opts = [
    partition_key: [
      type: :any,
      required: false,
      default: nil,
      doc: """
      Determines what value is used as the routing key for partitioning
      messages.

      Can be one of four values:

        * `nil` (default): The message itself is used as the routing key.
          Messages with the same content are routed to the same partition.

        * A function of arity 1: Applied to each message to return the routing
          key. Allows grouping related messages together (e.g., by user ID or
          account ID) to keep them in the same partition.

        * An MFA tuple `{Module, Function, Args}`: The function is applied with
          the message prepended to the arguments. Useful for delegating routing
          logic to a module function while keeping configuration declarative.

        * Any static term: Used as the routing key for all messages, giving
          explicit control over which partition receives them (e.g., `:logs`,
          `:events`, or an identifier).

      Fundamentally, this option determines how messages are distributed across
      partitions. Use it to keep related messages together (for ordering or
      state locality) or spread unrelated messages across partitions
      (for parallelism).
      """
    ]
  ]

  # Start options schema
  @start_opts_schema NimbleOptions.new!(start_opts ++ auto_opts)

  # Start options schema only for docs
  @start_opts_docs_schema NimbleOptions.new!(start_opts)

  # Runtime options schema
  @runtime_opts_schema NimbleOptions.new!(runtime_opts)

  ## API

  @spec start_options_docs() :: binary()
  def start_options_docs do
    NimbleOptions.docs(@start_opts_docs_schema)
  end

  @spec runtime_options_docs() :: binary()
  def runtime_options_docs do
    NimbleOptions.docs(@runtime_opts_schema)
  end

  @spec validate_start_options!(keyword()) :: keyword()
  def validate_start_options!(opts) do
    opts
    |> Keyword.take(Keyword.keys(@start_opts_schema.schema))
    |> NimbleOptions.validate!(@start_opts_schema)
  end

  @spec validate_runtime_options!(keyword()) :: keyword()
  def validate_runtime_options!(opts) do
    NimbleOptions.validate!(opts, @runtime_opts_schema)
  end
end
