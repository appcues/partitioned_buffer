# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## Release 0.4.1

### Bug Fixes

- Fixed `select_replace` match spec in `put_newer/5` and `put_all_newer/3` when
  values contain maps (including maps nested inside tuples or lists). Maps in
  match spec bodies are now wrapped with `{:const, map}` via `ms_literal/1`,
  which tells ETS to treat them as opaque literals.

## Release 0.4.0

### Enhancements

- `:table` mode for `:processing_batch_size` option. When set to `:table`,
  the ETS table name is passed directly to the processor instead of reading
  and batching the data. This gives the processor full control over how it
  reads and processes the table. The processor can optionally keep the table
  for later processing by calling `:ets.rename/2` to free the original name
  and `:ets.give_away/3` to transfer it to another process.
  [#14](https://github.com/appcues/partitioned_buffer/issues/14).
- Added ASCII data flow diagrams to `PartitionedBuffer.Queue`,
  `PartitionedBuffer.Map`, and `PartitionedBuffer.Partition` module docs.
- Minor documentation and comment fixes (grammar, typos, consistency).
- Migrated CI from CircleCI to GitHub Actions.

## Release 0.3.0

### Breaking Changes

- `PartitionedBuffer.Map` processor now receives `{key, value, version, updates}`
  tuples instead of `{key, {value, updates}}`. The `version` field is the entry
  version set via `put_newer/5` and `put_all_newer/3` (`0` for regular `put/4`
  entries). Existing processor functions must update their pattern matching
  accordingly.

### Bug Fixes

- Fixed `select_replace` match spec in `put_newer/5` and `put_all_newer/3` when
  keys or values are tuples or lists. Bare tuples in match spec bodies are
  interpreted as operations by ETS, not as literal data. Added `ms_literal/1` to
  wrap tuple/list values using the `{{...}}` constructor form.

## Release 0.2.2

### Added

- `update_options/2` for `PartitionedBuffer.Queue` and `PartitionedBuffer.Map`.
  Allows updating `processing_interval_ms`, `processing_timeout_ms`, and
  `processing_batch_size` at runtime across all partitions.

### Enhancements

- The `:processor` option now accepts an MFA tuple `{Module, Function, Args}`
  in addition to anonymous functions. The batch is prepended to the arguments.

## Release 0.2.1

### Enhancements

- `PartitionedBuffer.Map` processor now receives `{key, {value, updates}}` tuples
  instead of `{key, value}`. The `updates` field tracks the number of times an
  existing key was updated (only for versioned updates via `put_newer/5` and
  `put_all_newer/3`; regular `put/4` entries always have `updates` set to `0`).

## Release 0.2.0

### Added

- `PartitionedBuffer.Map` — New key-value map buffer using `:set` ETS tables
  with last-write-wins semantics. Exposes `put/4`, `put_all/3`, `get/3`,
  `delete/3`, `size/1`, `stop/3`, `start_link/1`, etc.
- `PartitionedBuffer.Map` — `put_newer/5` and `put_all_newer/3` for versioned
  conditional updates with "newer version wins" semantics. Uses ETS
  `insert_new` + `select_replace` with a literal key for O(1) atomic updates.

### Changed

- Refactored the previous monolithic implementation into
  `PartitionedBuffer.Queue`, extracting it as a concrete buffer implementation
  behind the `PartitionedBuffer` behaviour. This sets the foundation for
  multiple buffer types (e.g., `Map`).

## Release 0.1.1

- Moved `size` from metadata to measurements in the processing Telemetry span.
  This change prevents high cardinality tags in Datadog, reducing monitoring
  costs while still allowing size to be tracked as a metric.

## Release 0.1.0

- [sc-83498] Initial implementation for the partitioned buffer.
