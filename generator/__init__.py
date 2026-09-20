"""Shared synthetic e-commerce data generator -- not a component itself.

Both `benchmark/` (a clean DuckDB snapshot, then deliberately-injected
data-quality issues on top) and `cdc/` (a replayable INSERT/UPDATE/DELETE
event stream over the same entities, for CDC testing) build off this one
deterministic generator (`config.SEED`) rather than each inventing their
own dataset -- see the top-level README's architecture diagram.

This package has no imports from `agent/`, `benchmark/`, `cdc/`, or
`grader/` -- it's a leaf dependency the rest of the repo builds on, never
the other way around.

Public API (everything else in this package -- entities, events, schema,
inject_issues -- is an internal implementation detail of `generate` and
`eventlog`, not meant to be imported directly):

    from generator import config          # SEED, date range, table sizes
    from generator import build_database  # generator.generate's entrypoint
    from generator import eventlog        # Event, build_event_log, read/write_jsonl
"""

from . import config, eventlog
from .eventlog import Event, build_event_log, read_jsonl, write_jsonl
from .generate import build_database

__all__ = [
    "config",
    "eventlog",
    "Event",
    "build_event_log",
    "read_jsonl",
    "write_jsonl",
    "build_database",
]
