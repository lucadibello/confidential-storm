#!/usr/bin/env python3
"""
run-grid-search.py - grid-search benchmark for both the Baseline (no SGX)
and Enclave (SGX) Synthetic DP Histogram topologies.

Both topologies emit identical profiler CSV lifecycle events (COMPONENT_STARTED,
EPOCH_ADVANCED, MAX_EPOCH_REACHED), so startup and completion detection works the
same way for both.

Prerequisites:
  - Storm cluster must be running (make cluster-up in the relevant example dir)
  - JAR(s) must be built with profiling (make build-profiled)

Usage:
  # Enclave only (default)
  python3 run-grid-search.py --mode enclave

  # Baseline only
  python3 run-grid-search.py --mode baseline

  # A/B comparison (baseline first, then enclave, per combo)
  python3 run-grid-search.py --mode comparison

  # Change grid parameters (example: multiple tick intervals and parallelisms)
  python3 run-grid-search.py --mode enclave --tick-intervals 60 90
  TICK_INTERVALS="60 90" python3 run-grid-search.py --mode enclave
"""

import argparse
import math
import os
import re
import shutil
import subprocess
import sys
import time
from itertools import product
from pathlib import Path
from typing import List


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
# examples/scripts/ -> examples/
EXAMPLES_DIR = SCRIPT_DIR.parent
FRAMEWORK_ROOT = EXAMPLES_DIR.parent

BASELINE_PROJECT = EXAMPLES_DIR / "synthetic-benchmark-baseline"
ENCLAVE_PROJECT = EXAMPLES_DIR / "synthetic-dp-histogram"

BASELINE_JAR = BASELINE_PROJECT / "target" / "synthetic-benchmark-baseline-1.0-SNAPSHOT.jar"
BASELINE_CLASS = "ch.usi.inf.examples.synthetic_baseline.SyntheticBaselineTopology"

ENCLAVE_JAR = ENCLAVE_PROJECT / "host" / "target" / "synthetic-dp-histogram-host-1.0-SNAPSHOT.jar"
ENCLAVE_CLASS = "ch.usi.inf.examples.synthetic_dp.host.SyntheticTopology"

PROFILER_HOST_DIR = FRAMEWORK_ROOT / "data" / "storm-logs" / "supervisor" / "profiler"
WORKERS_HOST_DIR = FRAMEWORK_ROOT / "data" / "storm-logs" / "supervisor" / "workers-artifacts"

# Component names (shared by both topologies)
DP_COMPONENT = "bolt-data-perturbation"
AGG_COMPONENT = "bolt-histogram-aggregation"
BOUNDING_COMPONENT = "bolt-user-contribution-bounding"
SPOUT_COMPONENT = "spout"


# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

def env_int_list(name, default):
    # type: (str, str) -> List[int]
    return [int(x) for x in os.environ.get(name, default).split()]

def env_float(name, default):
    # type: (str, float) -> float
    return float(os.environ.get(name, str(default)))

def env_int(name, default):
    # type: (str, int) -> int
    return int(os.environ.get(name, str(default)))

def env_str(name, default):
    # type: (str, str) -> str
    return os.environ.get(name, default)

def env_bool(name, default):
    # type: (str, bool) -> bool
    return os.environ.get(name, str(default).lower()).lower() in ("true", "1", "yes")


# ---------------------------------------------------------------------------
# File helpers (3.6 compat)
# ---------------------------------------------------------------------------

def _read_file_lossy(path):
    """Read a file replacing decode errors."""
    with open(str(path), encoding="utf-8", errors="replace") as f:
        return f.read()


def _safe_unlink(path):
    """Unlink a file, ignoring errors if it doesn't exist."""
    try:
        path.unlink()
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Profiler CSV helpers
# ---------------------------------------------------------------------------

def _iter_profiler_csvs():
    """Yield rows from all profiler CSVs as split lists."""
    for csv_path in sorted(PROFILER_HOST_DIR.glob("*.csv")):
        with open(str(csv_path)) as f:
            for line in f:
                yield line.strip().split(",")


def count_lifecycle_event(component, event_name):
    """Count distinct taskIds that emitted a given lifecycle event for a component."""
    # CSV columns: timestamp, component, taskId, type, name, total, ...
    task_ids = set()
    for cols in _iter_profiler_csvs():
        if len(cols) >= 5 and cols[1] == component and cols[3] == "lifecycle" and cols[4] == event_name:
            task_ids.add(cols[2])
    return len(task_ids)


def latest_epoch(component):
    """Return the highest epoch from EPOCH_ADVANCED CSV rows for a component."""
    best = 0
    for cols in _iter_profiler_csvs():
        if len(cols) >= 6 and cols[1] == component and cols[3] == "lifecycle" and cols[4] == "EPOCH_ADVANCED":
            try:
                best = max(best, int(cols[5]))
            except ValueError:
                pass
    return best


def read_lifecycle_config(component, event_name):
    """Return the set of distinct values reported for a lifecycle config event across all tasks.

    Used to verify that components started with the expected configuration
    (e.g., MAX_EPOCHS_CONFIGURED, TICK_INTERVAL_SECS).
    CSV columns: timestamp, component, taskId, type, name, total, ...
    """
    values = set()
    for cols in _iter_profiler_csvs():
        if len(cols) >= 6 and cols[1] == component and cols[3] == "lifecycle" and cols[4] == event_name:
            try:
                values.add(int(cols[5]))
            except ValueError:
                pass
    return values


def verify_topology_config(p):
    """Check that DP and agg bolts reported the expected MAX_EPOCHS_CONFIGURED and TICK_INTERVAL_SECS.

    Returns True if all reported values match expectations, False otherwise.
    Mismatches are printed as warnings so the run can be aborted early.
    """
    ok = True
    for component, label in [(DP_COMPONENT, "dp"), (AGG_COMPONENT, "agg")]:
        reported_epochs = read_lifecycle_config(component, "MAX_EPOCHS_CONFIGURED")
        reported_ticks  = read_lifecycle_config(component, "TICK_INTERVAL_SECS")

        if not reported_epochs:
            print("[run {}] WARNING: {} did not report MAX_EPOCHS_CONFIGURED".format(
                p.run_id, label))
            ok = False
        elif reported_epochs != {p.max_time_steps}:
            print("[run {}] CONFIG MISMATCH: {} MAX_EPOCHS_CONFIGURED={} expected={}".format(
                p.run_id, label, sorted(reported_epochs), p.max_time_steps))
            ok = False

        if not reported_ticks:
            print("[run {}] WARNING: {} did not report TICK_INTERVAL_SECS".format(
                p.run_id, label))
            ok = False
        elif reported_ticks != {p.tick_interval}:
            print("[run {}] CONFIG MISMATCH: {} TICK_INTERVAL_SECS={} expected={}".format(
                p.run_id, label, sorted(reported_ticks), p.tick_interval))
            ok = False

    if ok:
        print("[run {}] Config verified: max_epochs={} tick_interval={}s".format(
            p.run_id, p.max_time_steps, p.tick_interval))
    return ok


def sum_counter(component, counter_name):
    """Sum a profiler counter across all tasks and CSV snapshots for a component.

    Counter rows have the format: timestamp,component,taskId,counter,<name>,<total>,...
    The 'total' field is cumulative per task, so we take the *latest* (highest)
    value per taskId and sum across tasks.
    """
    per_task = {}  # type: dict[str, int]
    for cols in _iter_profiler_csvs():
        if len(cols) >= 6 and cols[1] == component and cols[3] == "counter" and cols[4] == counter_name:
            try:
                per_task[cols[2]] = max(per_task.get(cols[2], 0), int(cols[5]))
            except ValueError:
                pass
    return sum(per_task.values())


# ---------------------------------------------------------------------------
# Worker log scanning
# ---------------------------------------------------------------------------

class FatalErrors(object):
    def __init__(self, oom=False, fatal=False):
        self.oom = oom
        self.fatal = fatal


def snapshot_worker_log_sizes(topo_name):
    """Record the current byte size of every worker.log for a topology.

    Call this immediately before sending the kill signal.  The returned dict
    maps each log path to its size at that moment, so subsequent scans can
    ignore bytes written during/after shutdown.
    """
    sizes = {}  # type: dict[Path, int]
    for worker_dir in sorted(WORKERS_HOST_DIR.glob("{}-*/".format(topo_name))):
        if not worker_dir.is_dir():
            continue
        for log_file in sorted(worker_dir.glob("*/worker.log")):
            if log_file.is_file():
                try:
                    sizes[log_file] = log_file.stat().st_size
                except OSError:
                    sizes[log_file] = 0
    return sizes


def check_worker_fatal_errors(topo_name, run_id, log_sizes=None):
    """Scan worker logs for OOM and SGX/native fatal errors.

    If log_sizes is provided (from snapshot_worker_log_sizes), only the bytes
    present before the kill signal are scanned, so shutdown-induced OOM errors
    are not counted as real problems.
    """
    result = FatalErrors()
    for worker_dir in sorted(WORKERS_HOST_DIR.glob("{}-*/".format(topo_name))):
        if not worker_dir.is_dir():
            continue
        for log_file in sorted(worker_dir.glob("*/worker.log")):
            if not log_file.is_file():
                continue
            short_path = "{}/{}/worker.log".format(worker_dir.name, log_file.parent.name)
            try:
                limit = log_sizes.get(log_file) if log_sizes is not None else None
                with open(str(log_file), encoding="utf-8", errors="replace") as f:
                    content = f.read(limit) if limit is not None else f.read()
            except OSError:
                continue

            for line in content.splitlines():
                if "STDERR" in line and "[INFO] Fatal error:" in line:
                    result.fatal = True
                    print("[run {}] WARNING: SGX/native fatal error detected in {}".format(run_id, short_path))
                    print("[run {}]   {}".format(run_id, line))
                    break

            if "Halting process: Worker died" in content:
                result.fatal = True
                print("[run {}] WARNING: Worker died detected in {}".format(run_id, short_path))
    return result


# ---------------------------------------------------------------------------
# Topology type abstraction
# ---------------------------------------------------------------------------

class TopologyType(object):
    def __init__(self, name, jar, topology_class, topo_prefix, project_dir,
                 startup_timeout, build_target):
        self.name = name                       # "baseline" or "enclave"
        self.jar = jar                         # Path to JAR
        self.topology_class = topology_class   # Fully qualified class
        self.topo_prefix = topo_prefix         # e.g. "SyntheticDPBaseline" or "SyntheticDP"
        self.project_dir = project_dir         # Where reports land and build runs
        self.startup_timeout = startup_timeout # Default startup timeout
        self.build_target = build_target       # Make target name


BASELINE = TopologyType(
    name="baseline",
    jar=BASELINE_JAR,
    topology_class=BASELINE_CLASS,
    topo_prefix="SyntheticDPBaseline",
    project_dir=BASELINE_PROJECT,
    startup_timeout=120,
    build_target="build-profiled",
)

ENCLAVE = TopologyType(
    name="enclave",
    jar=ENCLAVE_JAR,
    topology_class=ENCLAVE_CLASS,
    topo_prefix="SyntheticDP",
    project_dir=ENCLAVE_PROJECT,
    startup_timeout=800,
    build_target="build-profiled",
)


# ---------------------------------------------------------------------------
# Single run execution
# ---------------------------------------------------------------------------

class RunParams(object):
    def __init__(self, topo_type, run_id, tick_interval, max_time_steps, parallelism,
                 mu, num_users, num_keys, seed, ground_truth, poll_interval,
                 wait_safety_factor, startup_timeout, total_runs, run_dir):
        self.topo_type = topo_type             # TopologyType instance
        self.run_id = run_id
        self.tick_interval = tick_interval
        self.max_time_steps = max_time_steps
        self.parallelism = parallelism
        self.mu = mu
        self.num_users = num_users
        self.num_keys = num_keys
        self.seed = seed
        self.ground_truth = ground_truth
        self.poll_interval = poll_interval
        self.wait_safety_factor = wait_safety_factor
        self.startup_timeout = startup_timeout
        self.total_runs = total_runs
        self.run_dir = run_dir


def run_storm(*args):
    """Run a storm CLI command, returning the exit code."""
    return subprocess.call(["storm"] + list(args))


def wait_for_startup(p):
    """Wait for all components to emit COMPONENT_STARTED and DP bolts to pass BARRIER_RELEASED."""
    expected_started = 2 * p.parallelism + 2  # dp + agg + bounding + spout
    startup_elapsed = 0
    all_started = False

    while startup_elapsed < p.startup_timeout:
        time.sleep(p.poll_interval)
        startup_elapsed += p.poll_interval

        dp_started = count_lifecycle_event(DP_COMPONENT, "COMPONENT_STARTED")
        agg_started = count_lifecycle_event(AGG_COMPONENT, "COMPONENT_STARTED")
        bounding_started = count_lifecycle_event(BOUNDING_COMPONENT, "COMPONENT_STARTED")
        spout_started = count_lifecycle_event(SPOUT_COMPONENT, "COMPONENT_STARTED")
        total_started = dp_started + agg_started + bounding_started + spout_started

        dp_barrier = count_lifecycle_event(DP_COMPONENT, "BARRIER_RELEASED")

        print("[run {}] startup {}s: started {}/{} (dp={}/{}, agg={}/1, bounding={}/{}, spout={}/1)"
              " barrier={}/{}".format(
            p.run_id, startup_elapsed, total_started, expected_started,
            dp_started, p.parallelism, agg_started,
            bounding_started, p.parallelism, spout_started,
            dp_barrier, p.parallelism))

        if total_started >= expected_started and dp_barrier >= p.parallelism:
            print("[run {}] All components started and DP barrier released after {}s.".format(
                p.run_id, startup_elapsed))
            all_started = True
            break

    if not all_started:
        print("[run {}] WARNING: Startup timed out after {}s.".format(p.run_id, p.startup_timeout))
    return all_started, startup_elapsed


def poll_completion(p, topo_name, max_wait):
    """Poll profiler CSVs for MAX_EPOCH_REACHED lifecycle events."""
    expected_dp = p.parallelism
    expected_agg = 1
    elapsed = 0
    completed = False
    errors = FatalErrors()

    while elapsed < max_wait:
        time.sleep(p.poll_interval)
        elapsed += p.poll_interval

        dp_done = count_lifecycle_event(DP_COMPONENT, "MAX_EPOCH_REACHED")
        agg_done = count_lifecycle_event(AGG_COMPONENT, "MAX_EPOCH_REACHED")
        dp_epoch = latest_epoch(DP_COMPONENT)
        agg_epoch = latest_epoch(AGG_COMPONENT)

        agg_dummies = sum_counter(AGG_COMPONENT, "dummies_received")
        agg_reals = sum_counter(AGG_COMPONENT, "real_partials_received")

        print("[run {}] {}s: dp={}/{} (epoch {}/{}), agg={}/{} (epoch {}/{})"
              " | agg partials: real={} dummy={}".format(
            p.run_id, elapsed,
            dp_done, expected_dp, dp_epoch, p.max_time_steps,
            agg_done, expected_agg, agg_epoch, p.max_time_steps,
            agg_reals, agg_dummies))

        if dp_done >= expected_dp and agg_done >= expected_agg:
            print("[run {}] All {} tasks reached max epoch after {}s.".format(
                p.run_id, expected_dp + expected_agg, elapsed))
            completed = True
            break

        # errors = check_worker_fatal_errors(topo_name, p.run_id)
        # if errors.oom or errors.fatal:
        #     print("[run {}] Fatal error detected at {}s. Aborting wait.".format(p.run_id, elapsed))
        #     break

    if not completed:
        dp_done = count_lifecycle_event(DP_COMPONENT, "MAX_EPOCH_REACHED")
        agg_done = count_lifecycle_event(AGG_COMPONENT, "MAX_EPOCH_REACHED")
        print("[run {}] WARNING: Timed out after {}s. Final: dp={}/{}, agg={}/{}".format(
            p.run_id, max_wait, dp_done, expected_dp, agg_done, expected_agg))

    return completed, elapsed, errors


def execute_run(p):
    """Execute a single grid-search run (works identically for baseline and enclave)."""
    tt = p.topo_type
    topo_name = "{}{}".format(tt.topo_prefix, p.run_id)
    run_dir = p.run_dir
    max_wait = int(math.ceil(p.tick_interval * p.max_time_steps * p.wait_safety_factor))

    print("=" * 60)
    print(" [{}] Run {}/{}: tick={}s epochs={} p={} mu={}".format(
        tt.name, p.run_id, p.total_runs, p.tick_interval, p.max_time_steps,
        p.parallelism, p.mu))
    print(" Topology: {}  Startup timeout: {}s  Run timeout: {}s".format(
        topo_name, p.startup_timeout, max_wait))
    print(" Archive:  {}".format(run_dir))
    print("=" * 60)

    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "profiler").mkdir(exist_ok=True)

    # ---- 1. Clean profiler CSVs and stale worker artifacts before submission ----
    existing_csvs = list(PROFILER_HOST_DIR.glob("*.csv"))
    if existing_csvs:
        print("[run {}] Cleaning profiler dir before submission...".format(p.run_id))
        for f in existing_csvs:
            f.unlink()

    existing_worker_dirs = list(WORKERS_HOST_DIR.glob("{}-*/".format(topo_name)))
    if existing_worker_dirs:
        print("[run {}] WARNING: Found existing worker artifacts for {}. Cleaning up...".format(
            p.run_id, topo_name))
        for d in existing_worker_dirs:
            shutil.rmtree(str(d), ignore_errors=True)

    # ---- 2. Submit topology ----
    submit_time = int(time.time())
    print("[run {}] Submitting topology...".format(p.run_id))
    run_storm(
        "jar", str(tt.jar), tt.topology_class,
        "--run-id",         str(p.run_id),
        "--tick-interval",  str(p.tick_interval),
        "--max-time-steps", str(p.max_time_steps),
        "--parallelism",    str(p.parallelism),
        "--mu",             str(p.mu),
        "--num-users",      str(p.num_users),
        "--num-keys",       str(p.num_keys),
        "--seed",           str(p.seed),
        "--ground-truth",   p.ground_truth,
    )

    # ---- 3. Wait for startup (all components emit COMPONENT_STARTED) ----
    print("[run {}] Topology submitted. Waiting for startup (timeout: {}s)...".format(
        p.run_id, p.startup_timeout))
    all_started, startup_elapsed = wait_for_startup(p)

    # ---- 4. Verify topology started with correct configuration ----
    config_ok = False
    if all_started:
        config_ok = verify_topology_config(p)
        if not config_ok:
            print("[run {}] ABORTING: topology misconfigured, killing and skipping.".format(p.run_id))
            run_storm("kill", topo_name, "-w", "5")
            time.sleep(10)
            for f in PROFILER_HOST_DIR.glob("*.csv"):
                _safe_unlink(f)
            run_dir.mkdir(parents=True, exist_ok=True)
            with open(str(run_dir / "params.txt"), "w") as fh:
                fh.write("run_id={}\ncompleted=false\nrun_error=true\nrun_error_message=config_mismatch\n".format(
                    p.run_id))
            return

    # ---- 5. Poll for completion (MAX_EPOCH_REACHED lifecycle events) ----
    completed, elapsed, errors = poll_completion(p, topo_name, max_wait)

    # Brief pause to let in-flight CSV writes flush
    time.sleep(3)

    # ---- 6. Kill topology ----
    # Snapshot log sizes before kill -- bytes written after this point are shutdown noise.
    log_sizes = snapshot_worker_log_sizes(topo_name)
    errors = check_worker_fatal_errors(topo_name, p.run_id, log_sizes)

    print("[run {}] Killing topology {}...".format(p.run_id, topo_name))
    rc = run_storm("kill", topo_name, "-w", "5")
    if rc != 0:
        print("[run {}] WARNING: storm kill returned non-zero (topology may have already exited).".format(p.run_id))

    print("[run {}] Waiting 10s for Storm/ZooKeeper cleanup...".format(p.run_id))
    time.sleep(10)

    # ---- 7. Archive profiler CSVs + clean profiler dir ----
    csv_files = list(PROFILER_HOST_DIR.glob("*.csv"))
    csv_count = len(csv_files)
    if csv_files:
        for f in csv_files:
            shutil.copy2(str(f), str(run_dir / "profiler"))
            f.unlink()
        print("[run {}] Archived and cleaned {} profiler CSV(s).".format(p.run_id, csv_count))
    else:
        print("[run {}] WARNING: No profiler CSVs found.".format(p.run_id))

    # ---- 8. Archive topology report ----
    report_file = tt.project_dir / "data" / "synthetic-report-run{}.txt".format(p.run_id)
    if report_file.is_file():
        shutil.copy2(str(report_file), str(run_dir))
        print("[run {}] Archived topology report.".format(p.run_id))
    else:
        print("[run {}] WARNING: Topology report not found: {}".format(p.run_id, report_file))

    # ---- 9. Archive worker logs + detect crashes ----
    worker_logs_dir = run_dir / "worker-logs"
    worker_logs_dir.mkdir(exist_ok=True)
    worker_log_count = 0
    worker_crash_count = 0
    error_line_re = re.compile(r"^\S+ \S+ .* \[ERROR\]")

    for worker_dir in sorted(WORKERS_HOST_DIR.glob("{}-*/".format(topo_name))):
        if not worker_dir.is_dir():
            continue
        for log_file in sorted(worker_dir.glob("*/worker.log")):
            if not log_file.is_file():
                continue
            port_dir = log_file.parent.name
            dest_name = "{}_{}_worker.log".format(worker_dir.name, port_dir)
            shutil.copy2(str(log_file), str(worker_logs_dir / dest_name))
            worker_log_count += 1

            try:
                limit = log_sizes.get(log_file)
                with open(str(log_file), encoding="utf-8", errors="replace") as f:
                    content = f.read(limit) if limit is not None else f.read()
                lines = content.splitlines()
            except OSError:
                continue

            if any("java.lang.OutOfMemoryError" in line for line in lines):
                errors.oom = True
                print("[run {}] WARNING: OutOfMemoryError detected in {}".format(p.run_id, dest_name))

            matched_errors = [l for l in lines if error_line_re.match(l)]
            if matched_errors:
                worker_crash_count += 1
                print("[run {}] WARNING: ERROR log entries detected in {}".format(p.run_id, dest_name))
                for line in matched_errors[:3]:
                    print("[run {}]   {}".format(p.run_id, line))

    print("[run {}] Archived {} worker log(s) (OOM: {}, fatal: {}, ERROR: {}).".format(
        p.run_id, worker_log_count, errors.oom, errors.fatal, worker_crash_count))

    # ---- 10. Write run metadata ----
    end_time = int(time.time())
    duration = end_time - submit_time

    meta_lines = [
        "run_id={}".format(p.run_id),
        "topology_name={}".format(topo_name),
        "topology_type={}".format(tt.name),
        "tick_interval_secs={}".format(p.tick_interval),
        "max_time_steps={}".format(p.max_time_steps),
        "parallelism={}".format(p.parallelism),
        "mu={}".format(p.mu),
        "num_users={}".format(p.num_users),
        "num_keys={}".format(p.num_keys),
        "seed={}".format(p.seed),
        "ground_truth={}".format(p.ground_truth),
        "submit_time_unix={}".format(submit_time),
        "end_time_unix={}".format(end_time),
        "duration_secs={}".format(duration),
        "all_started={}".format(str(all_started).lower()),
        "startup_elapsed_secs={}".format(startup_elapsed),
        "startup_timeout_secs={}".format(p.startup_timeout),
        "completed={}".format(str(completed).lower()),
        "wait_used_secs={}".format(elapsed),
        "max_wait_secs={}".format(max_wait),
        "profiler_csvs_archived={}".format(csv_count),
        "worker_logs_archived={}".format(worker_log_count),
        "worker_oom_detected={}".format(str(errors.oom).lower()),
        "worker_fatal_detected={}".format(str(errors.fatal).lower()),
        "worker_error_count={}".format(worker_crash_count),
        "agg_dummies_received={}".format(sum_counter(AGG_COMPONENT, "dummies_received")),
        "agg_real_partials_received={}".format(sum_counter(AGG_COMPONENT, "real_partials_received")),
        "config_verified={}".format(str(config_ok).lower()),
    ]

    with open(str(run_dir / "params.txt"), "w") as f:
        f.write("\n".join(meta_lines) + "\n")
    print("[run {}] Metadata written.".format(p.run_id))


def cleanup_failed_run(topo_type, run_id, run_dir, tick, epochs, par, mu,
                       num_users, num_keys, seed, ground_truth, exc):
    """Best-effort cleanup after a failed run."""
    topo_name = "{}{}".format(topo_type.topo_prefix, run_id)
    print("[run {}] ERROR: Run failed: {}. Attempting cleanup...".format(run_id, exc))

    subprocess.call(["storm", "kill", topo_name, "-w", "5"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(10)

    for f in PROFILER_HOST_DIR.glob("*.csv"):
        _safe_unlink(f)

    run_dir.mkdir(parents=True, exist_ok=True)
    with open(str(run_dir / "params.txt"), "w") as fh:
        fh.write("\n".join([
            "run_id={}".format(run_id),
            "topology_name={}".format(topo_name),
            "topology_type={}".format(topo_type.name),
            "tick_interval_secs={}".format(tick),
            "max_time_steps={}".format(epochs),
            "parallelism={}".format(par),
            "mu={}".format(mu),
            "num_users={}".format(num_users),
            "num_keys={}".format(num_keys),
            "seed={}".format(seed),
            "ground_truth={}".format(ground_truth),
            "completed=false",
            "run_error=true",
            "run_error_message={}".format(exc),
        ]) + "\n")
    print("[run {}] Cleanup done. Continuing with next run.".format(run_id))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Unified grid-search benchmark for Baseline and Enclave DP Histogram topologies.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="All parameters can also be set via environment variables (see module docstring).")

    p.add_argument("--mode", choices=["baseline", "enclave", "comparison"],
                   default=env_str("MODE", "enclave"),
                   help="Which topology to benchmark (default: enclave)")

    # Grid parameters (CLI overrides env vars)
    p.add_argument("--tick-intervals", type=int, nargs="+",
                   default=env_int_list("TICK_INTERVALS", "90"),
                   help="Tick intervals in seconds")
    p.add_argument("--max-time-steps", type=int, nargs="+",
                   default=env_int_list("MAX_TIME_STEPS", "50"),
                   help="Max epoch counts")
    p.add_argument("--parallelisms", type=int, nargs="+",
                   default=env_int_list("PARALLELISMS", "8"),
                   help="Parallelism hints")
    p.add_argument("--mus", type=int, nargs="+",
                   default=env_int_list("MUS", "100"),
                   help="Mu (key threshold) values")
    p.add_argument("--num-users", type=int, nargs="+",
                   default=env_int_list("NUM_USERS_LIST", "1000000"),
                   help="User counts")
    p.add_argument("--num-keys", type=int, nargs="+",
                   default=env_int_list("NUM_KEYS_LIST", "1000000"),
                   help="Key counts")

    # Fixed
    p.add_argument("--seed", type=int, default=env_int("SEED", 42))
    p.add_argument("--ground-truth", default=env_str("GROUND_TRUTH", "false"))

    # Timing
    p.add_argument("--wait-safety-factor", type=float,
                   default=env_float("WAIT_SAFETY_FACTOR", 2.0))
    p.add_argument("--poll-interval", type=int,
                   default=env_int("POLL_INTERVAL_SECS", 10))
    p.add_argument("--startup-timeout-baseline", type=int,
                   default=env_int("STARTUP_TIMEOUT_BASELINE", 120))
    p.add_argument("--startup-timeout-enclave", type=int,
                   default=env_int("STARTUP_TIMEOUT_ENCLAVE", 800))

    # Other
    p.add_argument("--no-build", action="store_true",
                   default=not env_bool("BUILD", True),
                   help="Skip the build step")
    p.add_argument("--start-run-id", type=int,
                   default=env_int("START_RUN_ID", 1))
    p.add_argument("--resume-from-run", type=int,
                   default=env_int("RESUME_FROM_RUN", 0),
                   help="Skip runs with run_id < this value (resume a stopped grid search)")
    return p.parse_args()


def main():
    args = parse_args()

    # Check prerequisites
    if shutil.which("storm") is None:
        print("ERROR: 'storm' command not found. Is Storm installed and on PATH?")
        sys.exit(1)

    # Determine which topology types to run
    if args.mode == "baseline":
        types_to_run = [BASELINE]
    elif args.mode == "enclave":
        types_to_run = [ENCLAVE]
    else:
        types_to_run = [BASELINE, ENCLAVE]

    # Apply startup timeouts
    BASELINE.startup_timeout = args.startup_timeout_baseline
    ENCLAVE.startup_timeout = args.startup_timeout_enclave

    # Compute grid
    grid = list(product(
        args.tick_intervals, args.max_time_steps, args.parallelisms,
        args.mus, args.num_users, args.num_keys))
    total_combos = len(grid)
    total_runs = total_combos * len(types_to_run)

    # Estimate worst-case runtime
    max_tick = max(args.tick_intervals)
    max_epochs = max(args.max_time_steps)
    max_startup = max(tt.startup_timeout for tt in types_to_run)
    estimated_secs = (max_tick * max_epochs * args.wait_safety_factor + max_startup) * total_runs
    estimated_hrs = estimated_secs / 3600

    grid_timestamp = time.strftime("%Y%m%d_%H%M%S")
    if args.mode == "comparison":
        runs_dir = BASELINE_PROJECT / "data" / "comparison-runs" / grid_timestamp
    elif args.mode == "baseline":
        runs_dir = BASELINE_PROJECT / "data" / "runs" / grid_timestamp
    else:
        runs_dir = ENCLAVE_PROJECT / "data" / "runs" / grid_timestamp

    mode_label = {
        "baseline": "Baseline DP Histogram (No SGX)",
        "enclave": "Enclave DP Histogram (SGX)",
        "comparison": "Baseline vs Enclave -- Comparison",
    }[args.mode]

    print("=" * 60)
    print(" {} -- Grid Search Benchmark".format(mode_label))
    print("=" * 60)
    print(" Tick intervals (s):  {}".format(" ".join(str(v) for v in args.tick_intervals)))
    print(" Max time steps:      {}".format(" ".join(str(v) for v in args.max_time_steps)))
    print(" Parallelisms:        {}".format(" ".join(str(v) for v in args.parallelisms)))
    print(" Mu values:           {}".format(" ".join(str(v) for v in args.mus)))
    print(" Num users:           {}".format(" ".join(str(v) for v in args.num_users)))
    print(" Num keys:            {}".format(" ".join(str(v) for v in args.num_keys)))
    print(" Fixed: seed={}".format(args.seed))
    print(" Timeout factor:      {}x  poll: {}s".format(args.wait_safety_factor, args.poll_interval))
    print(" Total combos:        {}".format(total_combos))
    print(" Total runs:          {}".format(total_runs))
    print(" Estimated runtime:   {:.2f} hrs (worst case)".format(estimated_hrs))
    print(" Archive dir:         {}".format(runs_dir))
    print(" Start run ID:        {}".format(args.start_run_id))
    if args.resume_from_run > 0:
        print(" Resume from run:     {} (skipping earlier runs)".format(args.resume_from_run))
    print("=" * 60)
    print()

    # Build JAR(s)
    if not args.no_build:
        for tt in types_to_run:
            print("[grid-search] Building {} JAR ({})...".format(tt.name, tt.build_target))
            subprocess.check_call(["make", "-C", str(tt.project_dir), tt.build_target])
            print("[grid-search] {} build complete.".format(tt.name))
        print()

    # Validate JARs
    for tt in types_to_run:
        if not tt.jar.is_file():
            print("ERROR: JAR not found at {}".format(tt.jar))
            print("Run 'make {}' in {} first.".format(tt.build_target, tt.project_dir))
            sys.exit(1)

    runs_dir.mkdir(parents=True, exist_ok=True)
    PROFILER_HOST_DIR.mkdir(parents=True, exist_ok=True)

    # Run the grid
    run_id = args.start_run_id
    for tick, epochs, par, mu, num_users, num_keys in grid:
        if args.mode == "comparison":
            combo_dir = runs_dir / "tick{}_epochs{}_p{}_mu{}_u{}_k{}".format(
                tick, epochs, par, mu, num_users, num_keys)
        else:
            combo_dir = None

        if args.resume_from_run > 0 and run_id < args.resume_from_run:
            print("[grid-search] Skipping run {} (resuming from run {})".format(
                run_id, args.resume_from_run))
            run_id += 1
            continue

        for tt in types_to_run:
            if combo_dir is not None:
                run_dir = combo_dir / tt.name
            else:
                run_dir = runs_dir / "tick{}_epochs{}_p{}_mu{}_u{}_k{}_run{}".format(
                    tick, epochs, par, mu, num_users, num_keys, run_id)

            params = RunParams(
                topo_type=tt,
                run_id=run_id,
                tick_interval=tick,
                max_time_steps=epochs,
                parallelism=par,
                mu=mu,
                num_users=num_users,
                num_keys=num_keys,
                seed=args.seed,
                ground_truth=args.ground_truth,
                poll_interval=args.poll_interval,
                wait_safety_factor=args.wait_safety_factor,
                startup_timeout=tt.startup_timeout,
                total_runs=total_runs,
                run_dir=run_dir,
            )

            try:
                execute_run(params)
            except Exception as exc:
                cleanup_failed_run(
                    tt, run_id, run_dir, tick, epochs, par, mu,
                    num_users, num_keys, args.seed, args.ground_truth, exc)

            print()

        run_id += 1

    total_completed = run_id - args.start_run_id
    print("=" * 60)
    print(" Grid search complete. Combos processed: {}".format(total_completed))
    print(" Results archived in: {}".format(runs_dir))
    print("=" * 60)


if __name__ == "__main__":
    main()
