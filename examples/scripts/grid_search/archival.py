"""Per-slave-grouped archival of run artifacts.

Archive layout (one run_dir per topology run):

    <run_dir>/
      profiler/<slave>/profiler-<component>-task<id>.csv ...
      worker-logs/<slave>/<worker_dir>_<port>_worker.log ...
      worker-artifacts/<slave>/<worker_dir>_<port>_<artifact> ...
      synthetic-report-run<id>.txt          (from whichever slave hosted the agg bolt)
      params.txt
"""

import re
import shutil
from pathlib import Path

from .hosts import ClusterTopology
from .paths import StagingPaths


_ERROR_LINE_RE = re.compile(r"^\S+ \S+ .* \[ERROR\]")


class FatalErrors(object):
    def __init__(self):
        self.oom = False
        self.fatal = False
        self.error_count = 0


def snapshot_worker_log_sizes(staging_paths, topo_name):
    # type: (StagingPaths, str) -> dict[Path, int]
    """Record current byte size of every worker.log in the merged view.

    Call immediately before issuing the kill signal so the subsequent
    error scan ignores bytes written during/after shutdown.
    """
    sizes = {}  # type: dict[Path, int]
    root = staging_paths.merged_workers_artifacts
    if not root.is_dir():
        return sizes
    for worker_dir in sorted(root.glob("{}-*".format(topo_name))):
        if not worker_dir.is_dir():
            continue
        for log_name in ("worker.log", "worker.log.err"):
            for log_file in sorted(worker_dir.glob("*/{}".format(log_name))):
                if log_file.is_file():
                    try:
                        sizes[log_file] = log_file.stat().st_size
                    except OSError:
                        sizes[log_file] = 0
    return sizes


def check_worker_fatal_errors(staging_paths, topo_name, run_id, log_sizes=None):
    # type: (StagingPaths, str, int, dict[Path, int]) -> FatalErrors
    """Scan worker logs for OOM and SGX/native fatal errors."""
    result = FatalErrors()
    root = staging_paths.merged_workers_artifacts
    if not root.is_dir():
        return result
    for worker_dir in sorted(root.glob("{}-*".format(topo_name))):
        if not worker_dir.is_dir():
            continue
        for log_name in ("worker.log", "worker.log.err"):
            for log_file in sorted(worker_dir.glob("*/{}".format(log_name))):
                if not log_file.is_file():
                    continue
                short_path = "{}/{}/{}".format(
                    worker_dir.name, log_file.parent.name, log_name)
                try:
                    limit = log_sizes.get(log_file) if log_sizes is not None else None
                    with open(str(log_file), encoding="utf-8", errors="replace") as f:
                        content = f.read(limit) if limit is not None else f.read()
                except OSError:
                    continue

                if "java.lang.OutOfMemoryError" in content:
                    result.oom = True
                    print("[run {}] WARNING: OutOfMemoryError detected in {}".format(
                        run_id, short_path))

                for line in content.splitlines():
                    if "STDERR" in line and "[INFO] Fatal error:" in line:
                        result.fatal = True
                        print("[run {}] WARNING: SGX/native fatal error detected in {}".format(
                            run_id, short_path))
                        print("[run {}]   {}".format(run_id, line))
                        break

                if "Halting process: Worker died" in content:
                    result.fatal = True
                    print("[run {}] WARNING: Worker died detected in {}".format(
                        run_id, short_path))
    return result


def archive_profiler_csvs(topology, staging_paths, run_dir):
    # type: (ClusterTopology, StagingPaths, Path) -> int
    """Copy profiler CSVs from each slave's staging dir into run_dir/profiler/<slave>/."""
    total = 0
    profiler_dir = run_dir / "profiler"
    for slave in topology.slaves:
        src_dir = staging_paths.slave_profiler(slave.hostname)
        if not src_dir.is_dir():
            continue
        dest_dir = profiler_dir / slave.hostname
        files = list(src_dir.glob("*.csv"))
        if not files:
            continue
        dest_dir.mkdir(parents=True, exist_ok=True)
        for f in files:
            shutil.copy2(str(f), str(dest_dir / f.name))
            total += 1
    return total


def archive_topology_report(topology, staging_paths, run_dir, run_id):
    # type: (ClusterTopology, StagingPaths, Path, int) -> str
    """Find synthetic-report-run<id>.txt on whichever slave hosted the agg bolt.

    Returns the slave hostname that hosted the report (or empty string if not found).
    """
    report_name = "synthetic-report-run{}.txt".format(run_id)
    found = []
    for slave in topology.slaves:
        candidate = staging_paths.slave_profiler(slave.hostname) / report_name
        if candidate.is_file():
            found.append((slave.hostname, candidate))

    if not found:
        print("[run {}] WARNING: topology report not found on any slave".format(run_id))
        return ""
    if len(found) > 1:
        # Should never happen (aggregation bolt parallelism=1).
        print("[run {}] WARNING: report found on multiple slaves: {}".format(
            run_id, [h for h, _ in found]))
    src_host, src_path = found[0]
    shutil.copy2(str(src_path), str(run_dir / report_name))
    return src_host


def archive_worker_logs_and_artifacts(topology, staging_paths, run_dir,
                                       topo_name, run_id, log_sizes, errors):
    # type: (ClusterTopology, StagingPaths, Path, str, int, dict[Path, int], FatalErrors) -> tuple[int, int]
    """Copy worker.log{,.err} and other artifacts, grouped by slave.

    Side effect: appends to ``errors.error_count`` and toggles ``errors.oom``
    if any log shows OutOfMemoryError on a real (pre-kill) line.

    Returns (worker_log_count, worker_artifact_count).
    """
    worker_log_count = 0
    worker_artifact_count = 0
    logs_root = run_dir / "worker-logs"
    artifacts_root = run_dir / "worker-artifacts"

    for slave in topology.slaves:
        per_slave_artifacts_dir = staging_paths.slave_workers_artifacts(slave.hostname)
        if not per_slave_artifacts_dir.is_dir():
            continue

        slave_logs_dir = logs_root / slave.hostname
        slave_artifacts_dir = artifacts_root / slave.hostname

        for worker_dir in sorted(per_slave_artifacts_dir.glob("{}-*".format(topo_name))):
            if not worker_dir.is_dir():
                continue
            # Logs
            for log_name in ("worker.log", "worker.log.err"):
                for log_file in sorted(worker_dir.glob("*/{}".format(log_name))):
                    if not log_file.is_file():
                        continue
                    port_dir = log_file.parent.name
                    dest_name = "{}_{}_{}".format(worker_dir.name, port_dir, log_name)
                    slave_logs_dir.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(log_file), str(slave_logs_dir / dest_name))
                    worker_log_count += 1

                    try:
                        # The merged-view symlink points back to per-slave staging,
                        # so use the same file for size-limited reads.
                        merged_path = staging_paths.merged_workers_artifacts / worker_dir.name / port_dir / log_name
                        limit = log_sizes.get(merged_path) if log_sizes else None
                        with open(str(log_file), encoding="utf-8", errors="replace") as f:
                            content = f.read(limit) if limit is not None else f.read()
                        lines = content.splitlines()
                    except OSError:
                        continue

                    if any("java.lang.OutOfMemoryError" in line for line in lines):
                        errors.oom = True
                        print("[run {}] WARNING: OutOfMemoryError detected in {}".format(
                            run_id, dest_name))

                    matched_errors = [l for l in lines if _ERROR_LINE_RE.match(l)]
                    if matched_errors:
                        errors.error_count += 1
                        print("[run {}] WARNING: ERROR log entries detected in {}".format(
                            run_id, dest_name))
                        for line in matched_errors[:3]:
                            print("[run {}]   {}".format(run_id, line))

            # Other artifacts (heap dumps, gc logs)
            for port_dir in sorted(worker_dir.iterdir()):
                if not port_dir.is_dir():
                    continue
                for artifact in sorted(port_dir.iterdir()):
                    if artifact.name in ("worker.log", "worker.log.err"):
                        continue
                    if not artifact.is_file():
                        continue
                    slave_artifacts_dir.mkdir(parents=True, exist_ok=True)
                    dest_name = "{}_{}_{}".format(
                        worker_dir.name, port_dir.name, artifact.name)
                    shutil.copy2(str(artifact), str(slave_artifacts_dir / dest_name))
                    worker_artifact_count += 1

    return worker_log_count, worker_artifact_count
