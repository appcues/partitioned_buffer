# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## Release 0.1.1

- Move `size` from metadata to measurements in the processing Telemetry span.
  This change prevents high cardinality tags in Datadog, reducing monitoring
  costs while still allowing size to be tracked as a metric.

## Release 0.1.0

- [sc-83498] Initial implementation for the partitioned buffer.
