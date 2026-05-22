"""Profiler-CSV iteration helpers, parameterised by staging dir.

These are the multi-host versions of the helpers that used to live as
module-level functions in run-grid-search.py (lines 122-218).  Everything
reads from ``staging_paths.merged_profiler`` -- a symlink farm refreshed by
RemoteCollector after each rsync.

CSV row format (see ProfilerReport.java:111):
    timestamp,component,taskId,type,name,total,sampled,avgMs,minMs,maxMs
"""

from .paths import StagingPaths


def _iter_profiler_csvs(staging_paths):
    # type: (StagingPaths) -> iter
    """Yield rows from every profiler CSV (across all slaves) as split lists."""
    profiler_dir = staging_paths.merged_profiler
    if not profiler_dir.is_dir():
        return
    for csv_path in sorted(profiler_dir.glob("*.csv")):
        try:
            with open(str(csv_path)) as f:
                for line in f:
                    yield line.strip().split(",")
        except OSError:
            continue


def count_lifecycle_event(staging_paths, component, event_name):
    # type: (StagingPaths, str, str) -> int
    """Count distinct taskIds that emitted a given lifecycle event for a component."""
    task_ids = set()
    for cols in _iter_profiler_csvs(staging_paths):
        if (len(cols) >= 5
                and cols[1] == component
                and cols[3] == "lifecycle"
                and cols[4] == event_name):
            task_ids.add(cols[2])
    return len(task_ids)


def latest_epoch(staging_paths, component):
    # type: (StagingPaths, str) -> int
    """Highest epoch reported in EPOCH_ADVANCED rows for a component."""
    best = 0
    for cols in _iter_profiler_csvs(staging_paths):
        if (len(cols) >= 6
                and cols[1] == component
                and cols[3] == "lifecycle"
                and cols[4] == "EPOCH_ADVANCED"):
            try:
                best = max(best, int(cols[5]))
            except ValueError:
                pass
    return best


def read_lifecycle_config(staging_paths, component, event_name):
    # type: (StagingPaths, str, str) -> set[int]
    """Set of distinct values reported for a lifecycle config event across all tasks."""
    values = set()
    for cols in _iter_profiler_csvs(staging_paths):
        if (len(cols) >= 6
                and cols[1] == component
                and cols[3] == "lifecycle"
                and cols[4] == event_name):
            try:
                values.add(int(cols[5]))
            except ValueError:
                pass
    return values


def sum_counter(staging_paths, component, counter_name):
    # type: (StagingPaths, str, str) -> int
    """Sum a profiler counter across all tasks (take the latest cumulative value per task)."""
    per_task = {}  # type: dict[str, int]
    for cols in _iter_profiler_csvs(staging_paths):
        if (len(cols) >= 6
                and cols[1] == component
                and cols[3] == "counter"
                and cols[4] == counter_name):
            try:
                per_task[cols[2]] = max(per_task.get(cols[2], 0), int(cols[5]))
            except ValueError:
                pass
    return sum(per_task.values())
