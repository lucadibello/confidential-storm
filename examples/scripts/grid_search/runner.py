"""Per-run driver: submit topology, wait for startup, poll, archive.
"""

import math
import os
import subprocess
import time
from pathlib import Path

from . import profiler_io as pio
from .archival import (
    FatalErrors,
    archive_profiler_csvs,
    archive_topology_report,
    archive_worker_logs_and_artifacts,
    check_worker_fatal_errors,
    snapshot_worker_log_sizes,
)
from .hosts import ClusterTopology
from .paths import StagingPaths
from .remote import RemoteCollector


# Component names (shared by both topologies, see ComponentConstants.java).
DP_COMPONENT = "bolt-data-perturbation"
AGG_COMPONENT = "bolt-histogram-aggregation"
BOUNDING_COMPONENT = "bolt-user-contribution-bounding"
SPOUT_COMPONENT = "spout"


# ---------------------------------------------------------------------------
# Topology type
# ---------------------------------------------------------------------------

class TopologyType(object):
    def __init__(self, name, jar, topology_class, topo_prefix, project_dir,
                 startup_timeout, build_target):
        self.name = name
        self.jar = jar
        self.topology_class = topology_class
        self.topo_prefix = topo_prefix
        self.project_dir = project_dir
        self.startup_timeout = startup_timeout
        self.build_target = build_target


# ---------------------------------------------------------------------------
# Per-run parameters
# ---------------------------------------------------------------------------

class RunParams(object):
    def __init__(self, topo_type, run_id, tick_interval, max_time_steps, parallelism,
                 mu, num_users, num_keys, seed, ground_truth, poll_interval,
                 wait_safety_factor, startup_timeout, total_runs, run_dir,
                 topology, staging_paths, remote_collector,
                 storm_conf_dir, post_kill_sleep, max_rsync_failures, label=""):
        # type: (TopologyType, int, int, int, int, int, int, int, int, str, int, float, int, int, Path, ClusterTopology, StagingPaths, RemoteCollector, Path, int, int, str) -> None
        self.topo_type = topo_type
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
        self.topology = topology
        self.staging_paths = staging_paths
        self.remote_collector = remote_collector
        self.storm_conf_dir = storm_conf_dir
        self.post_kill_sleep = post_kill_sleep
        self.max_rsync_failures = max_rsync_failures
        self.label = label


# ---------------------------------------------------------------------------
# Storm CLI wrapper
# ---------------------------------------------------------------------------

def run_storm(storm_conf_dir, *args):
    # type: (Path, *str) -> int
    """Invoke `storm` with STORM_CONF_DIR set so it talks to the right Nimbus."""
    env = os.environ.copy()
    env["STORM_CONF_DIR"] = str(storm_conf_dir)
    return subprocess.call(["storm"] + list(args), env=env)


# ---------------------------------------------------------------------------
# Lifecycle config verification
# ---------------------------------------------------------------------------

def verify_topology_config(p):
    # type: (RunParams) -> bool
    ok = True
    for component, label in [(DP_COMPONENT, "dp"), (AGG_COMPONENT, "agg")]:
        reported_epochs = pio.read_lifecycle_config(p.staging_paths, component, "MAX_EPOCHS_CONFIGURED")
        reported_ticks = pio.read_lifecycle_config(p.staging_paths, component, "TICK_INTERVAL_SECS")

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


# ---------------------------------------------------------------------------
# Poll loops
# ---------------------------------------------------------------------------

def _bump_failures(consec, stats):
    """Update per-host consecutive-failure counts based on a SyncStats."""
    failed = set(stats.failed_hosts)
    for host in list(consec.keys()):
        if host not in failed:
            consec[host] = 0
    for host in failed:
        consec[host] = consec.get(host, 0) + 1


def wait_for_startup(p):
    # type: (RunParams) -> tuple[bool, int]
    expected_started = 2 * p.parallelism + 2  # dp + agg + bounding + spout
    startup_elapsed = 0
    all_started = False
    consec_failures = {}  # type: dict[str, int]

    while startup_elapsed < p.startup_timeout:
        time.sleep(p.poll_interval)
        startup_elapsed += p.poll_interval

        stats = p.remote_collector.sync_profiler()
        _bump_failures(consec_failures, stats)
        if stats.failed_hosts:
            print("[run {}] WARNING: rsync failed for: {}".format(
                p.run_id, stats.failed_hosts))

        dp_started = pio.count_lifecycle_event(p.staging_paths, DP_COMPONENT, "COMPONENT_STARTED")
        agg_started = pio.count_lifecycle_event(p.staging_paths, AGG_COMPONENT, "COMPONENT_STARTED")
        bounding_started = pio.count_lifecycle_event(p.staging_paths, BOUNDING_COMPONENT, "COMPONENT_STARTED")
        spout_started = pio.count_lifecycle_event(p.staging_paths, SPOUT_COMPONENT, "COMPONENT_STARTED")
        total_started = dp_started + agg_started + bounding_started + spout_started

        dp_barrier = pio.count_lifecycle_event(p.staging_paths, DP_COMPONENT, "BARRIER_RELEASED")

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

        if any(c >= p.max_rsync_failures for c in consec_failures.values()):
            bad = [h for h, c in consec_failures.items() if c >= p.max_rsync_failures]
            print("[run {}] ABORTING startup: persistent rsync failures from {}".format(
                p.run_id, bad))
            break

    if not all_started:
        print("[run {}] WARNING: Startup timed out after {}s.".format(
            p.run_id, p.startup_timeout))
    return all_started, startup_elapsed


def poll_completion(p, topo_name, max_wait):
    # type: (RunParams, str, int) -> tuple[bool, int, FatalErrors]
    expected_dp = p.parallelism
    expected_agg = 1
    elapsed = 0
    completed = False
    errors = FatalErrors()
    consec_failures = {}  # type: dict[str, int]
    rsync_aborted = False

    while elapsed < max_wait:
        time.sleep(p.poll_interval)
        elapsed += p.poll_interval

        stats = p.remote_collector.sync_all()
        _bump_failures(consec_failures, stats)
        if stats.failed_hosts:
            print("[run {}] WARNING: rsync failed for: {}".format(
                p.run_id, stats.failed_hosts))

        dp_done = pio.count_lifecycle_event(p.staging_paths, DP_COMPONENT, "MAX_EPOCH_REACHED")
        agg_done = pio.count_lifecycle_event(p.staging_paths, AGG_COMPONENT, "MAX_EPOCH_REACHED")
        dp_epoch = pio.latest_epoch(p.staging_paths, DP_COMPONENT)
        agg_epoch = pio.latest_epoch(p.staging_paths, AGG_COMPONENT)

        agg_dummies = pio.sum_counter(p.staging_paths, AGG_COMPONENT, "dummies_received")
        agg_reals = pio.sum_counter(p.staging_paths, AGG_COMPONENT, "real_partials_received")

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

        if any(c >= p.max_rsync_failures for c in consec_failures.values()):
            bad = [h for h, c in consec_failures.items() if c >= p.max_rsync_failures]
            print("[run {}] ABORTING run: persistent rsync failures from {}".format(
                p.run_id, bad))
            rsync_aborted = True
            errors.fatal = True
            break

    if not completed and not rsync_aborted:
        dp_done = pio.count_lifecycle_event(p.staging_paths, DP_COMPONENT, "MAX_EPOCH_REACHED")
        agg_done = pio.count_lifecycle_event(p.staging_paths, AGG_COMPONENT, "MAX_EPOCH_REACHED")
        print("[run {}] WARNING: Timed out after {}s. Final: dp={}/{}, agg={}/{}".format(
            p.run_id, max_wait, dp_done, expected_dp, agg_done, expected_agg))

    return completed, elapsed, errors


# ---------------------------------------------------------------------------
# execute_run
# ---------------------------------------------------------------------------

def execute_run(p):
    # type: (RunParams) -> None
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
    print(" Cluster:  master={} slaves={} (N={})".format(
        p.topology.master.hostname,
        ",".join(s.hostname for s in p.topology.slaves) or "(none)",
        p.topology.n() or 1))
    print(" Archive:  {}".format(run_dir))
    print("=" * 60)

    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "profiler").mkdir(exist_ok=True)

    # ---- 1. Clean remote artifacts + local staging before submission ----
    print("[run {}] Cleaning remote profiler dirs + local staging...".format(p.run_id))
    p.remote_collector.clean_remote_dirs(topo_name)
    p.remote_collector.clean_local_staging()

    # ---- 2. Submit topology ----
    submit_time = int(time.time())
    print("[run {}] Submitting topology...".format(p.run_id))
    run_storm(
        p.storm_conf_dir,
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

    # ---- 3. Wait for startup ----
    print("[run {}] Topology submitted. Waiting for startup (timeout: {}s)...".format(
        p.run_id, p.startup_timeout))
    all_started, startup_elapsed = wait_for_startup(p)

    # ---- 4. Verify topology config ----
    config_ok = False
    if all_started:
        config_ok = verify_topology_config(p)
        if not config_ok:
            print("[run {}] ABORTING: topology misconfigured, killing and skipping.".format(
                p.run_id))
            run_storm(p.storm_conf_dir, "kill", topo_name, "-w", "5")
            time.sleep(p.post_kill_sleep)
            p.remote_collector.clean_remote_dirs(topo_name)
            with open(str(run_dir / "params.txt"), "w") as fh:
                fh.write("run_id={}\ncompleted=false\nrun_error=true\n"
                         "run_error_message=config_mismatch\n".format(p.run_id))
            return

    # ---- 5. Poll for completion ----
    completed, elapsed, errors = poll_completion(p, topo_name, max_wait)

    # Brief pause to let in-flight CSV writes flush.
    time.sleep(3)
    p.remote_collector.sync_all()

    # ---- 6. Kill topology ----
    log_sizes = snapshot_worker_log_sizes(p.staging_paths, topo_name)
    scan_errors = check_worker_fatal_errors(p.staging_paths, topo_name, p.run_id, log_sizes)
    errors.oom = errors.oom or scan_errors.oom
    errors.fatal = errors.fatal or scan_errors.fatal

    print("[run {}] Killing topology {}...".format(p.run_id, topo_name))
    rc = run_storm(p.storm_conf_dir, "kill", topo_name, "-w", "5")
    if rc != 0:
        print("[run {}] WARNING: storm kill returned non-zero "
              "(topology may have already exited).".format(p.run_id))

    print("[run {}] Waiting {}s for Storm/ZooKeeper cleanup...".format(
        p.run_id, p.post_kill_sleep))
    time.sleep(p.post_kill_sleep)

    # Final post-kill sync to catch any last flushes.
    p.remote_collector.sync_all()

    # ---- 7. Archive profiler CSVs (per-slave grouping) ----
    csv_count = archive_profiler_csvs(p.topology, p.staging_paths, run_dir)
    if csv_count == 0:
        print("[run {}] WARNING: No profiler CSVs found across any slave.".format(p.run_id))
    else:
        print("[run {}] Archived {} profiler CSV(s) across {} slave(s).".format(
            p.run_id, csv_count, p.topology.n() or 1))

    # ---- 8. Archive topology report (from whichever slave hosted it) ----
    aggregator_host = archive_topology_report(p.topology, p.staging_paths, run_dir, p.run_id)
    if aggregator_host:
        print("[run {}] Archived topology report from {}.".format(p.run_id, aggregator_host))

    # ---- 9. Archive worker logs + crash detection ----
    worker_log_count, worker_artifact_count = archive_worker_logs_and_artifacts(
        p.topology, p.staging_paths, run_dir, topo_name, p.run_id, log_sizes, errors)
    print("[run {}] Archived {} worker log(s) (OOM: {}, fatal: {}, ERROR: {}),"
          " {} extra artifact(s).".format(
        p.run_id, worker_log_count, errors.oom, errors.fatal,
        errors.error_count, worker_artifact_count))

    # ---- 10. Write run metadata ----
    end_time = int(time.time())
    duration = end_time - submit_time

    slave_hosts_str = ",".join(s.hostname for s in p.topology.slaves)
    meta_lines = [
        "run_id={}".format(p.run_id),
        "label={}".format(p.label),
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
        "worker_error_count={}".format(errors.error_count),
        "agg_dummies_received={}".format(pio.sum_counter(p.staging_paths, AGG_COMPONENT, "dummies_received")),
        "agg_real_partials_received={}".format(pio.sum_counter(p.staging_paths, AGG_COMPONENT, "real_partials_received")),
        "config_verified={}".format(str(config_ok).lower()),
        "n_supervisors={}".format(p.topology.n() or 1),
        "supervisor_hosts={}".format(slave_hosts_str),
        "master_host={}".format(p.topology.master.hostname),
        "aggregator_host={}".format(aggregator_host),
    ]

    with open(str(run_dir / "params.txt"), "w") as fh:
        fh.write("\n".join(meta_lines) + "\n")
    print("[run {}] Metadata written.".format(p.run_id))


def cleanup_failed_run(topo_type, run_id, run_dir, tick, epochs, par, mu,
                       num_users, num_keys, seed, ground_truth,
                       storm_conf_dir, post_kill_sleep, exc):
    # type: (TopologyType, int, Path, int, int, int, int, int, int, int, str, Path, int, Exception) -> None
    topo_name = "{}{}".format(topo_type.topo_prefix, run_id)
    print("[run {}] ERROR: Run failed: {}. Attempting cleanup...".format(run_id, exc))

    env = os.environ.copy()
    env["STORM_CONF_DIR"] = str(storm_conf_dir)
    subprocess.call(["storm", "kill", topo_name, "-w", "5"],
                    env=env,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(post_kill_sleep)

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
