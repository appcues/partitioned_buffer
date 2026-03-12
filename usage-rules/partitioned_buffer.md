# PartitionedBuffer Project-Specific Usage Rules

## Project Overview

PartitionedBuffer is an ETS-based partitioned buffer library for
high-throughput data processing in Elixir. Inspired by the
[OpenTelemetry Batch Processor](https://github.com/open-telemetry/opentelemetry-erlang/blob/main/apps/opentelemetry/src/otel_batch_processor.erl),
it efficiently buffers arbitrary data and periodically processes it
using a configurable processor function.

It ships with two concrete buffer implementations:

- **`PartitionedBuffer.Queue`** — Ordered queue buffer
  (insertion-time ordered, backed by `:ordered_set` ETS tables).
- **`PartitionedBuffer.Map`** — Key-value map buffer
  (last-write-wins semantics, backed by `:set` ETS tables).

Both use partitioning to reduce lock contention and double-buffering
for zero-downtime processing.

## Key Files

| Path | Purpose |
|------|---------|
| `lib/partitioned_buffer.ex` | Main behaviour, routing, shared API |
| `lib/partitioned_buffer/queue.ex` | Queue buffer implementation |
| `lib/partitioned_buffer/map.ex` | Map buffer implementation |
| `lib/partitioned_buffer/partition.ex` | Partition GenServer (core engine) |
| `lib/partitioned_buffer/partition_supervisor.ex` | Supervises N partition workers |
| `lib/partitioned_buffer/supervisor.ex` | Top-level supervisor (Registry + TaskSupervisor + PartitionSupervisor) |
| `lib/partitioned_buffer/options.ex` | NimbleOptions schemas for start, runtime, and updatable options |
| `mix.exs` | Dependencies and project config |
| `CHANGELOG.md` | Release history and breaking changes |
| `test/` | Test suite |

## Architecture

```
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

### Key Design Decisions

- **Double-buffering**: Each partition owns two ETS tables. One
  accepts writes while the other is isolated for processing. This
  enables zero-downtime table swaps.
- **`persistent_term` for table lookup**: The current write table
  is stored in `persistent_term` keyed by partition name. Because
  the value is always an atom, this avoids global GC triggers.
- **Async processing via `Task.Supervisor`**: Processing runs in a
  separate task using `Task.Supervisor.async_nolink/3`. The
  previous write table is transferred to the task via
  `:ets.give_away/3`, fully isolating reads from writes.
- **Batch processing with continuations**: `ets:select/3` with
  continuations controls memory by processing in configurable
  batch sizes rather than loading entire tables.
- **Partition routing**: Uses `:erlang.phash2/2` to distribute
  entries across partitions. Configurable via `partition_key`
  runtime option (function, MFA, static value, or `nil` for
  message-hash default).

### Adding a New Buffer Type

To add a new buffer implementation:

1. Create a new module implementing the `PartitionedBuffer`
   behaviour (implement `c:ets_type/0`).
2. Provide `start_link/1`, `stop/3`, `child_spec/1`, and
   domain-specific write/read functions.
3. Delegate shared operations (`size`, `update_options`, `stop`)
   to `PartitionedBuffer`.
4. If the new type needs a custom match spec for processing,
   add a clause to `ets_match_spec/1` in `Partition`.
5. Add tests mirroring the existing `Queue` and `Map` test
   structure.

## Processor Conventions

### Queue Processor

The processor receives a flat list of values:

```elixir
fn batch ->
  # batch is [value1, value2, ...]
  Enum.each(batch, &process/1)
end
```

### Map Processor

The processor receives a list of 4-element tuples:

```elixir
fn batch ->
  # batch is [{key, value, version, updates}, ...]
  Enum.each(batch, fn {k, v, _version, _updates} ->
    process(k, v)
  end)
end
```

- `version` — Set via `put_newer/5` / `put_all_newer/3`;
  `0` for regular `put/4` entries.
- `updates` — Number of times an existing key was conditionally
  updated; `0` for regular `put/4` entries.

### MFA Processors

The processor also accepts `{Module, Function, Args}` tuples.
The batch is prepended to the arguments:

```elixir
processor: {MyModule, :process, [extra_arg]}
# calls MyModule.process(batch, extra_arg)
```

## Options and Validation

- All options are validated via `NimbleOptions` schemas in
  `PartitionedBuffer.Options`.
- Start options: `:name`, `:processor`, `:partitions`,
  `:processing_interval_ms`, `:processing_timeout_ms`,
  `:processing_batch_size`, `:module` (auto-set by
  implementations).
- Runtime options: `:partition_key`.
- Updatable options (via `update_options/2`):
  `:processing_interval_ms`, `:processing_timeout_ms`,
  `:processing_batch_size`.
- Options documented via `NimbleOptions` should provide functions
  to insert that documentation into module docs. E.g.,
  `#{PartitionedBuffer.Options.start_options_docs()}`.

## ETS Match Spec Safety

When building ETS match specs (especially for `select_replace`):

- Bare tuples in match spec bodies are interpreted as operations,
  **not** literal data. Use the `ms_literal/1` helper in
  `Partition` to wrap tuple/list values with the `{{...}}`
  constructor form.
- Always use literal (bound) keys in match heads for O(1) lookup
  instead of pattern variables that cause full-table scans.
- Map keys/values with embedded tuples are a known ETS
  `select_replace` limitation.

## Telemetry

All telemetry events use the prefix
`[:partitioned_buffer, :partition]`. Events:

| Event | When |
|-------|------|
| `...:start` | Partition starts |
| `...:stop` | Partition terminates |
| `...:processing:start` | Batch processing begins |
| `...:processing:stop` | Batch processing completes |
| `...:processing:exception` | Exception during processing |
| `...:processing_failed` | Processing task fails (DOWN) |

When adding new telemetry events, document them in the
`PartitionedBuffer` module `@moduledoc` following the existing
format (measurements + metadata).

## Testing Patterns

- Test both `Queue` and `Map` implementations.
- Use short `processing_interval_ms` values in tests (e.g., 100ms)
  to avoid slow test suites.
- Test processor invocation by using a processor that sends
  messages to the test process (e.g., `fn batch -> send(pid, batch) end`).
- Test edge cases: empty buffers, single-partition setups,
  concurrent writes, graceful shutdown processing.
- For versioned updates (`put_newer`), test ordering guarantees:
  newer overwrites older, older is ignored.
- Test with tuple and list keys/values to exercise `ms_literal/1`.

## Documentation Standards

- Start with a clear `@moduledoc` explaining purpose and usage.
- Include usage examples in module documentation.
- Use `@doc` for all public functions with examples.
- Options documented using `NimbleOptions` should be inserted into
  docs via helper functions (e.g.,
  `#{PartitionedBuffer.Options.start_options_docs()}`).
- Maximum text length is 80 characters. Exceptions: links and
  code snippets that exceed by only 1-2 characters.
- When changing documentation, validate with `mix docs`.

## Code Style

### Module Organization

Organize module sections in this order:

1. `@moduledoc`
2. `@behaviour` / `use` / `import` / `alias`
3. Module attributes and records
4. Type definitions
5. `## API` — Public functions
6. `## Callbacks` — Behaviour implementations
7. `## Private functions`

### Comments

- Use `##` for section separators (e.g., `## API`,
  `## Private functions`).
- Use `#` for inline code comments. Keep them concise.
- Avoid obvious comments; code should be self-explanatory.
- Use `@compile inline: [...]` for hot-path functions.

### Naming

- Buffer implementations: `PartitionedBuffer.<Type>`
  (e.g., `Queue`, `Map`).
- Partition tables: `<Buffer>.<Index>.Table1` /
  `<Buffer>.<Index>.Table2`.
- Use `buffer` for the buffer name variable, `partition` for
  partition names, `entry` for ETS records.
- Options: always `opts`.

## Common Pitfalls to Avoid

- **Do NOT** read from a partition's ETS table without going
  through `get_current_table/1` — the active table can swap at
  any time.
- **Do NOT** use bare tuples in ETS match spec bodies without
  wrapping them via `ms_literal/1`.
- **Do NOT** forget to handle the `:processing?` flag — if a
  processing cycle is already running, the timer postpones.
- **Do NOT** delete ETS tables manually outside the processing
  task — the task owns the handed-off table and deletes it after
  processing.
- **Do NOT** store non-atom values in `persistent_term` for table
  lookup — this would trigger global GC on every update.
- **Do NOT** forget to update `CHANGELOG.md` for user-visible
  changes.
