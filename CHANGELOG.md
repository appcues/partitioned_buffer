# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

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
- `PartitionedBuffer.Map` – put_newer/5` and `put_all_newer/3` for versioned
  conditional updates with "newer version wins" semantics. Uses ETS
  `insert_new` + `select_replace` with a literal key for O(1) atomic updates.

### Changed

- Refactored the previous monolithic implementation into
  `PartitionedBuffer.Queue`, extracting it as a concrete buffer implementation
  behind the `PartitionedBuffer` behaviour. This sets the foundation for
  multiple buffer types (e.g., `Map`).

## Release 0.1.1

- Move `size` from metadata to measurements in the processing Telemetry span.
  This change prevents high cardinality tags in Datadog, reducing monitoring
  costs while still allowing size to be tracked as a metric.

## Release 0.1.0

- [sc-83498] Initial implementation for the partitioned buffer.
