#!/usr/bin/env python3
"""run-grid-search.py - grid-search benchmark for the Synthetic DP Histogram
topologies, with single-host or multi-host (scaling) operation.

Both Baseline and Enclave topologies emit identical profiler CSV lifecycle
events (COMPONENT_STARTED, EPOCH_ADVANCED, MAX_EPOCH_REACHED).

Prerequisites
-------------
Single-host:
  - Storm cluster up (``make cluster-up`` in the example dir).
  - JAR(s) built with profiling (``make build-profiled``).

Multi-host:
  - Master node has the storm CLI on PATH and can SSH passwordlessly to
    every slave
  - Every slave has Docker + ``docker compose`` v2 installed, the
    ``confidential-storm:latest`` image available locally, and the SGX
    devices + aesm socket (for enclave mode).
  - Firewall ports open:
      master <- slaves: 22 (SSH)
      slaves -> master: 6627 (Nimbus thrift), 2181 (ZooKeeper)
      slaves <-> slaves: 6700-6703 (Storm inter-worker netty transport)

Examples:

  # Legacy single-host enclave
  python3 run-grid-search.py --mode enclave

  # Multi-host scaling sweep (1, 2, 4 supervisors)
  python3 run-grid-search.py \\
      --mode enclave \\
      --supervisor-hosts 10.0.0.11,10.0.0.12,10.0.0.13,10.0.0.14 \\
      --master-host 10.0.0.10 \\
      --scale-values 1,2,4 \\
      --label scaling-sweep
"""

import argparse
import atexit
import os
import shutil
import socket
import subprocess
import sys
import time
from itertools import product
from pathlib import Path

from grid_search.cluster import ClusterManager
from grid_search.config_render import ConfigRenderer
from grid_search.hosts import ClusterTopology, MasterHost, SupervisorHost
from grid_search.paths import StagingPaths
from grid_search.remote import RemoteCollector
from grid_search.runner import (
    RunParams,
    TopologyType,
    cleanup_failed_run,
    execute_run,
)

# Make the grid_search package importable when this script is invoked
# directly via "python3 run-grid-search.py" rather than as a module.
SCRIPT_DIR = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# Paths and topology types
# ---------------------------------------------------------------------------

# examples/scripts/ -> examples/
EXAMPLES_DIR = SCRIPT_DIR.parent
FRAMEWORK_ROOT = EXAMPLES_DIR.parent
TEMPLATES_DIR = SCRIPT_DIR / "templates"

BASELINE_PROJECT = EXAMPLES_DIR / "synthetic-benchmark-baseline"
ENCLAVE_PROJECT = EXAMPLES_DIR / "synthetic-benchmark-confidential"

BASELINE_JAR = BASELINE_PROJECT / "target" / "synthetic-benchmark-baseline-1.0-SNAPSHOT.jar"
BASELINE_CLASS = "ch.usi.inf.examples.synthetic_baseline.SyntheticBaselineTopology"

ENCLAVE_JAR = ENCLAVE_PROJECT / "host" / "target" / "synthetic-benchmark-confidential-host-1.0-SNAPSHOT.jar"
ENCLAVE_CLASS = "ch.usi.inf.examples.synthetic_dp.host.SyntheticTopology"

MICROBATCH_BASELINE_PROJECT = EXAMPLES_DIR / "microbatch-benchmark-baseline"
MICROBATCH_ENCLAVE_PROJECT = EXAMPLES_DIR / "microbatch-benchmark-confidential"

MICROBATCH_BASELINE_JAR = (
    MICROBATCH_BASELINE_PROJECT / "target" / "microbatch-benchmark-baseline-1.0-SNAPSHOT.jar"
)
MICROBATCH_BASELINE_CLASS = (
    "ch.usi.inf.examples.microbatch_baseline.MicroBatchBaselineTopology"
)

MICROBATCH_ENCLAVE_JAR = (
    MICROBATCH_ENCLAVE_PROJECT / "host" / "target"
    / "microbatch-benchmark-confidential-host-1.0-SNAPSHOT.jar"
)
MICROBATCH_ENCLAVE_CLASS = (
    "ch.usi.inf.examples.microbatch_dp.host.MicroBatchTopology"
)


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

MICROBATCH_BASELINE = TopologyType(
    name="microbatch-baseline",
    jar=MICROBATCH_BASELINE_JAR,
    topology_class=MICROBATCH_BASELINE_CLASS,
    topo_prefix="MicroBatchBaseline",
    project_dir=MICROBATCH_BASELINE_PROJECT,
    startup_timeout=120,
    build_target="build",
)

MICROBATCH_ENCLAVE = TopologyType(
    name="microbatch-enclave",
    jar=MICROBATCH_ENCLAVE_JAR,
    topology_class=MICROBATCH_ENCLAVE_CLASS,
    topo_prefix="MicroBatchDP",
    project_dir=MICROBATCH_ENCLAVE_PROJECT,
    startup_timeout=800,
    build_target="build",
)

# Modes that drive batches via BEGIN/END markers — they ignore mu/num_users/
# /num_keys/tick/max_time_steps and instead take batch-sizes-gb + runs-per-size.
MICROBATCH_MODES = ("microbatch-baseline", "microbatch-enclave")


# ---------------------------------------------------------------------------
# Env / arg helpers
# ---------------------------------------------------------------------------

def env_int_list(name, default):
    return [int(x) for x in os.environ.get(name, default).split()]

def env_int_csv(name, default):
    raw = os.environ.get(name, default)
    return [int(x) for x in raw.replace(",", " ").split() if x]

def env_str_csv(name, default):
    raw = os.environ.get(name, default)
    return [x for x in raw.replace(",", " ").split() if x]

def env_float(name, default):
    return float(os.environ.get(name, str(default)))

def env_int(name, default):
    return int(os.environ.get(name, str(default)))

def env_str(name, default):
    return os.environ.get(name, default)

def env_bool(name, default):
    return os.environ.get(name, str(default).lower()).lower() in ("true", "1", "yes")


def parse_args():
    p = argparse.ArgumentParser(
        description="Grid-search benchmark for Baseline + Enclave DP Histogram topologies "
                    "with single-host or multi-host scaling support.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="All parameters can also be set via environment variables "
               "(see module docstring).")

    p.add_argument("--mode", choices=[
                       "baseline", "enclave", "comparison",
                       "microbatch-baseline", "microbatch-enclave",
                   ],
                   default=env_str("MODE", "enclave"),
                   help="Which topology to benchmark (default: enclave). "
                        "microbatch-* modes ignore tick/max_time_steps/mu/num_users/"
                        "num_keys and use --batch-sizes-gb / --runs-per-size instead.")

    # ---- Micro-batch parameters (only used when --mode microbatch-*) -------
    p.add_argument("--batch-sizes-gb", type=str,
                   default=env_str("BATCH_SIZES_GB", "1,2,5"),
                   help="Comma-separated micro-batch sizes in GB. "
                        "Default: 1,2,5 (matches paper Fig.).")
    p.add_argument("--runs-per-size", type=int,
                   default=env_int("RUNS_PER_SIZE", 3),
                   help="Number of times each batch size is repeated in a single "
                        "topology run. Default: 3.")
    p.add_argument("--bytes-per-tuple", type=int,
                   default=env_int("BYTES_PER_TUPLE", 31),
                   help="Bytes used for the GB->records conversion. Default: 31 "
                        "(measured baseline Kryo footprint; pass the same value to "
                        "both pipelines for matched record-count workload).")
    p.add_argument("--completion-timeout-ms", type=int,
                   default=env_int("COMPLETION_TIMEOUT_MS", 30 * 60 * 1000),
                   help="Spout-side ZK wait timeout per batch, in ms. Default: 30 min.")

    # ---- Grid parameters ---------------------------------------------------
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
    p.add_argument("--post-kill-sleep", type=int,
                   default=env_int("POST_KILL_SLEEP", 30),
                   help="Seconds to wait after `storm kill` for cleanup "
                        "(bumped from legacy 10s for multi-host blob-store GC).")

    # Build / submission
    p.add_argument("--no-build", action="store_true",
                   default=not env_bool("BUILD", True),
                   help="Skip the build step")
    p.add_argument("--start-run-id", type=int,
                   default=env_int("START_RUN_ID", 1))
    p.add_argument("--resume-from-run", type=int,
                   default=env_int("RESUME_FROM_RUN", 0),
                   help="Skip runs with run_id < this value")
    p.add_argument("--label", type=str,
                   default=env_str("LABEL", ""),
                   help="Optional label used as a subdirectory to group related runs")

    # ---- Multi-host flags --------------------------------------------------
    p.add_argument("--supervisor-hosts", type=str,
                   default=env_str("SUPERVISOR_HOSTS", ""),
                   help="Comma-separated slave IPs/hostnames. "
                        "Empty -> single-host legacy mode.")
    p.add_argument("--master-host", type=str,
                   default=env_str("MASTER_HOST", socket.gethostname()),
                   help="Reachable hostname/IP slaves use to contact Nimbus/ZK.")
    p.add_argument("--scale-values", type=str,
                   default=env_str("SCALE_VALUES", "1"),
                   help="Comma-separated list of N (supervisor count) values "
                        "to sweep, e.g. 1,2,4,8. Ignored without --supervisor-hosts.")
    p.add_argument("--ssh-user", type=str,
                   default=env_str("SSH_USER", os.environ.get("USER", "root")),
                   help="SSH user for the outer host (to establish the tunnel).")
    p.add_argument("--ssh-key", type=str,
                   default=env_str("SSH_KEY", str(Path.home() / ".ssh" / "id_ed25519")),
                   help="SSH key used for both the outer hop and the devcontainer hop.")
    p.add_argument("--ssh-port", type=int,
                   default=env_int("SSH_PORT", 22),
                   help="SSH port on the outer host.")
    p.add_argument("--container-user", type=str,
                   default=env_str("CONTAINER_USER", "dev"),
                   help="SSH user inside the devcontainer (default: dev).")
    p.add_argument("--container-port", type=int,
                   default=env_int("CONTAINER_PORT", 2222),
                   help="SSH port the devcontainer listens on (on the outer host's "
                        "127.0.0.1, default: 2222).")
    p.add_argument("--remote-data-dir", type=str,
                   default=env_str("REMOTE_DATA_DIR", "/workspaces/confidential-storm"),
                   help="Path inside the slave devcontainer used for SCP/rsync. "
                        "The host-side path for docker volume mounts is discovered "
                        "automatically via docker inspect.")
    p.add_argument("--nimbus-thrift-port", type=int,
                   default=env_int("NIMBUS_THRIFT_PORT", 6627),
                   help="Nimbus thrift port (default: 6627).")
    p.add_argument("--zk-port", type=int,
                   default=env_int("ZK_PORT", 2181),
                   help="ZooKeeper client port (default: 2181).")
    p.add_argument("--ui-port", type=int,
                   default=env_int("UI_PORT", 8080),
                   help="Storm UI HTTP port (default: 8080). "
                        "Change if another service (e.g. cAdvisor) already holds 8080.")
    p.add_argument("--logviewer-port", type=int,
                   default=env_int("LOGVIEWER_PORT", 8000),
                   help="Storm Logviewer port (default: 8000).")
    p.add_argument("--slot-ports", type=str,
                   default=env_str("SLOT_PORTS", "6700,6701,6702,6703"))
    p.add_argument("--rsync-bwlimit", type=int,
                   default=env_int("RSYNC_BWLIMIT", 0),
                   help="rsync bandwidth limit in KB/s (0 = unlimited).")
    p.add_argument("--cluster-health-timeout", type=int,
                   default=env_int("CLUSTER_HEALTH_TIMEOUT", 180))
    p.add_argument("--max-rsync-failures", type=int,
                   default=env_int("MAX_RSYNC_FAILURES", 3),
                   help="Consecutive failures from a single slave before aborting a run.")
    p.add_argument("--no-cluster", action="store_true",
                   default=env_bool("NO_CLUSTER", False),
                   help="Don't bring up / tear down the cluster. "
                        "Assume operator manages it (legacy default).")
    p.add_argument("--no-sgx-check", action="store_true",
                   default=env_bool("NO_SGX_CHECK", False))
    p.add_argument("--storm-image", type=str,
                   default=env_str("STORM_IMAGE", "confidential-storm:latest"))
    p.add_argument("--storm-version", type=str,
                   default=env_str("STORM_VERSION", "2.8.3"))
    p.add_argument("--zookeeper-version", type=str,
                   default=env_str("ZOOKEEPER_VERSION", "3.9"))

    return p.parse_args()


# ---------------------------------------------------------------------------
# Topology / host construction
# ---------------------------------------------------------------------------

def build_master(args):
    # type: (argparse.Namespace) -> MasterHost
    return MasterHost(
        hostname=args.master_host,
        nimbus_thrift_port=args.nimbus_thrift_port,
        zk_port=args.zk_port,
        ui_port=args.ui_port,
        logviewer_port=args.logviewer_port,
        container_project_dir=args.remote_data_dir,
    )


def build_slaves(args, slot_ports):
    # type: (argparse.Namespace, list) -> list
    hostnames = [h for h in args.supervisor_hosts.replace(",", " ").split() if h]
    ssh_key = args.ssh_key if args.ssh_key else None
    slaves = []
    for h in hostnames:
        slaves.append(SupervisorHost(
            hostname=h,
            outer_user=args.ssh_user,
            outer_key=ssh_key,
            outer_port=args.ssh_port,
            container_user=args.container_user,
            container_port=args.container_port,
            remote_data_dir=args.remote_data_dir,
            slot_ports=list(slot_ports),
            rsync_bwlimit=args.rsync_bwlimit,
        ))
    return slaves


def build_local_supervisor(args, slot_ports):
    """Make a SupervisorHost that points at the master's local filesystem.

    Used for legacy single-host mode (no --supervisor-hosts).  ``remote_data_dir``
    is set to FRAMEWORK_ROOT so the rsync-from-local short-circuit lines up
    with the legacy ./data/storm-logs/supervisor/ paths the existing
    docker-compose.yml uses.
    """
    return SupervisorHost(
        hostname="localhost",
        outer_user=args.ssh_user,
        outer_key=None,
        outer_port=args.ssh_port,
        container_user=args.container_user,
        container_port=args.container_port,
        remote_data_dir=str(FRAMEWORK_ROOT),
        slot_ports=list(slot_ports),
        rsync_bwlimit=args.rsync_bwlimit,
    )


def parse_scale_values(args, n_available):
    raw = [int(x) for x in args.scale_values.replace(",", " ").split() if x]
    if not raw:
        return [1]
    if n_available > 0:
        bad = [n for n in raw if n > n_available]
        if bad:
            raise SystemExit(
                "ERROR: --scale-values {} exceeds the number of "
                "--supervisor-hosts provided ({})".format(bad, n_available))
    return raw


# ---------------------------------------------------------------------------
# Micro-batch execution
# ---------------------------------------------------------------------------

def _microbatch_csv_basename(topo_type, run_id):
    # type: (TopologyType, int) -> str
    if topo_type.name == "microbatch-baseline":
        return "microbatch-baseline-run{}.csv".format(run_id)
    return "microbatch-confidential-run{}.csv".format(run_id)


def _count_microbatch_csv_rows(csv_path):
    # type: (Path) -> int
    if not csv_path.is_file():
        return 0
    n = 0
    with open(str(csv_path), "r") as fh:
        for i, line in enumerate(fh):
            if i == 0:
                # header
                continue
            if line.strip():
                n += 1
    return n


def execute_microbatch_run(topo_type, run_id, parallelism, batch_sizes_gb,
                           runs_per_size, bytes_per_tuple, completion_timeout_ms,
                           seed, total_runs, run_dir, topology, staging_paths,
                           remote_collector, storm_conf_dir, post_kill_sleep,
                           poll_interval):
    """Submit, drive and archive a single micro-batch topology run.

    Completion signal: the aggregator writes one CSV row per batch completed
    (`microbatch-{baseline,confidential}-runN.csv`). We poll until the row
    count reaches `len(sizes) * runs_per_size`, then kill the topology and
    archive the CSV + worker logs.

    Submission flags mirror MicroBatchTopology / MicroBatchBaselineTopology's
    CLI surface; mu / num_users / num_keys / tick / max_time_steps are not
    used in micro-batch mode.
    """
    from grid_search.runner import run_storm
    from grid_search.archival import (
        FatalErrors,
        snapshot_worker_log_sizes,
        check_worker_fatal_errors,
        archive_profiler_csvs,
        archive_topology_report,
        archive_worker_logs_and_artifacts,
    )

    topo_name = "{}{}".format(topo_type.topo_prefix, run_id)
    sizes_list = [s.strip() for s in batch_sizes_gb.split(",") if s.strip()]
    expected_rows = len(sizes_list) * runs_per_size

    print("=" * 60)
    print(" [{}] Run {}/{}: parallelism={} sizes={} runs/size={} bytes/tuple={}".format(
        topo_type.name, run_id, total_runs, parallelism,
        batch_sizes_gb, runs_per_size, bytes_per_tuple))
    print(" Topology: {}  Expected batches: {}  Startup timeout: {}s".format(
        topo_name, expected_rows, topo_type.startup_timeout))
    print(" Archive:  {}".format(run_dir))
    print("=" * 60)

    run_dir.mkdir(parents=True, exist_ok=True)

    # ---- 1. Clean remote artifacts + local staging ----
    print("[microbatch run {}] Cleaning remote profiler dirs + local staging...".format(run_id))
    remote_collector.clean_remote_dirs(topo_name)
    remote_collector.clean_local_staging()

    # ---- 2. Submit ----
    print("[microbatch run {}] Submitting topology...".format(run_id))
    rc = run_storm(
        storm_conf_dir,
        "jar", str(topo_type.jar), topo_type.topology_class,
        "--run-id",                str(run_id),
        "--parallelism",           str(parallelism),
        "--batch-sizes-gb",        batch_sizes_gb,
        "--runs-per-size",         str(runs_per_size),
        "--bytes-per-tuple",       str(bytes_per_tuple),
        "--completion-timeout-ms", str(completion_timeout_ms),
        "--seed",                  str(seed),
    )
    if rc != 0:
        raise RuntimeError(
            "storm jar exited with code {} for topology {}".format(rc, topo_name))

    # ---- 3. Poll CSV row count ----
    # Hard cap on total wall time = startup + N batches * conservative per-batch budget
    # (we trust completion_timeout_ms per batch already, plus a 60s pad per batch).
    per_batch_pad_secs = max(60, completion_timeout_ms // 1000)
    max_wait = topo_type.startup_timeout + expected_rows * per_batch_pad_secs

    errors = FatalErrors()
    elapsed = 0
    last_rows = 0
    completed = False
    csv_name = _microbatch_csv_basename(topo_type, run_id)

    while elapsed < max_wait:
        time.sleep(poll_interval)
        elapsed += poll_interval

        remote_collector.sync_all()
        # The aggregator writes to /logs/storm/profiler/<csv>; the remote
        # collector rsyncs that into the merged profiler view.
        candidates = list(staging_paths.merged_profiler.glob(csv_name))
        if not candidates:
            print("[microbatch run {}] {}s: CSV not yet visible ({})".format(
                run_id, elapsed, csv_name))
            continue

        rows = max(_count_microbatch_csv_rows(c) for c in candidates)
        if rows != last_rows:
            print("[microbatch run {}] {}s: {} / {} batches complete".format(
                run_id, elapsed, rows, expected_rows))
            last_rows = rows

        if rows >= expected_rows:
            print("[microbatch run {}] All {} batches reported after {}s.".format(
                run_id, expected_rows, elapsed))
            completed = True
            break

    if not completed:
        print("[microbatch run {}] WARNING: timed out after {}s ({} / {} batches).".format(
            run_id, max_wait, last_rows, expected_rows))

    # ---- 4. Snapshot logs, kill, final sync ----
    log_sizes = snapshot_worker_log_sizes(staging_paths, topo_name)
    scan_errors = check_worker_fatal_errors(staging_paths, topo_name, run_id, log_sizes)
    errors.oom = errors.oom or scan_errors.oom
    errors.fatal = errors.fatal or scan_errors.fatal

    print("[microbatch run {}] Killing topology {}...".format(run_id, topo_name))
    run_storm(storm_conf_dir, "kill", topo_name, "-w", "5")
    time.sleep(post_kill_sleep)
    remote_collector.sync_all()

    # ---- 5. Archive ----
    csv_count = archive_profiler_csvs(topology, staging_paths, run_dir)
    aggregator_host = archive_topology_report(topology, staging_paths, run_dir, run_id)
    worker_log_count, worker_artifact_count = archive_worker_logs_and_artifacts(
        topology, staging_paths, run_dir, topo_name, run_id, log_sizes, errors)
    print("[microbatch run {}] Archived: profiler-csvs={} worker-logs={} "
          "worker-artifacts={} aggregator-host={}".format(
              run_id, csv_count, worker_log_count, worker_artifact_count, aggregator_host))

    # ---- 6. Params file ----
    with open(str(run_dir / "params.txt"), "w") as fh:
        fh.write("run_id={}\nparallelism={}\nbatch_sizes_gb={}\nruns_per_size={}\n"
                 "bytes_per_tuple={}\ncompletion_timeout_ms={}\n"
                 "completed={}\nbatches_reported={}\nexpected_batches={}\n"
                 "oom={}\nfatal={}\n".format(
                     run_id, parallelism, batch_sizes_gb, runs_per_size,
                     bytes_per_tuple, completion_timeout_ms,
                     str(completed).lower(), last_rows, expected_rows,
                     str(errors.oom).lower(), str(errors.fatal).lower()))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    if shutil.which("storm") is None:
        print("ERROR: 'storm' command not found. Is Storm installed and on PATH?")
        sys.exit(1)

    # Topology types in play
    if args.mode == "baseline":
        types_to_run = [BASELINE]
    elif args.mode == "enclave":
        types_to_run = [ENCLAVE]
    elif args.mode == "microbatch-baseline":
        types_to_run = [MICROBATCH_BASELINE]
    elif args.mode == "microbatch-enclave":
        types_to_run = [MICROBATCH_ENCLAVE]
    else:
        types_to_run = [BASELINE, ENCLAVE]

    BASELINE.startup_timeout = args.startup_timeout_baseline
    ENCLAVE.startup_timeout = args.startup_timeout_enclave
    MICROBATCH_BASELINE.startup_timeout = args.startup_timeout_baseline
    MICROBATCH_ENCLAVE.startup_timeout = args.startup_timeout_enclave

    is_microbatch = args.mode in MICROBATCH_MODES

    # Build the grid. In micro-batch mode only parallelism is swept — batch
    # sizes and runs-per-size are handled inside one topology submission.
    if is_microbatch:
        grid = [(0, par, 0, 0, 0, 0) for par in args.parallelisms]
    else:
        grid = list(product(
            args.max_time_steps, args.parallelisms,
            args.mus, args.num_users, args.num_keys, args.tick_intervals))
    total_combos = len(grid)

    # Resolve cluster shape
    slot_ports = [int(x) for x in args.slot_ports.replace(",", " ").split() if x]
    master = build_master(args)
    remote_slaves = build_slaves(args, slot_ports)
    is_multi_host = len(remote_slaves) > 0
    scale_values = parse_scale_values(args, len(remote_slaves)) if is_multi_host else [1]

    total_runs = total_combos * len(types_to_run) * len(scale_values)

    # Estimate runtime
    max_tick = max(args.tick_intervals)
    max_epochs = max(args.max_time_steps)
    max_startup = max(tt.startup_timeout for tt in types_to_run)
    estimated_secs = (max_tick * max_epochs * args.wait_safety_factor + max_startup) * total_runs
    estimated_hrs = estimated_secs / 3600

    grid_timestamp = time.strftime("%Y%m%d_%H%M%S")
    label = args.label.strip()

    if args.mode == "comparison":
        base = BASELINE_PROJECT / "data" / "comparison-runs"
    elif args.mode == "baseline":
        base = BASELINE_PROJECT / "data" / "runs"
    elif args.mode == "microbatch-baseline":
        base = MICROBATCH_BASELINE_PROJECT / "data" / "runs"
    elif args.mode == "microbatch-enclave":
        base = MICROBATCH_ENCLAVE_PROJECT / "data" / "runs"
    else:
        base = ENCLAVE_PROJECT / "data" / "runs"
    runs_root = (base / label / grid_timestamp) if label else (base / grid_timestamp)
    cluster_root = FRAMEWORK_ROOT / "data" / "cluster" / grid_timestamp
    staging_root = FRAMEWORK_ROOT / "data" / "staging" / grid_timestamp

    mode_label = {
        "baseline": "Baseline DP Histogram (No SGX)",
        "enclave": "Enclave DP Histogram (SGX)",
        "comparison": "Baseline vs Enclave -- Comparison",
        "microbatch-baseline": "Micro-batch DP Histogram -- Baseline (No SGX)",
        "microbatch-enclave": "Micro-batch DP Histogram -- Enclave (SGX)",
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
    print(" Archive dir:         {}".format(runs_root))
    if label:
        print(" Label:               {}".format(label))
    print(" Start run ID:        {}".format(args.start_run_id))
    if args.resume_from_run > 0:
        print(" Resume from run:     {} (skipping earlier runs)".format(args.resume_from_run))
    print(" Master host:         {}  nimbus={} zk={} ui={} logviewer={}".format(
        master.hostname, master.nimbus_thrift_port, master.zk_port,
        master.ui_port, master.logviewer_port))
    if is_multi_host:
        print(" Supervisor hosts:    {}".format(", ".join(s.hostname for s in remote_slaves)))
        print(" Scale values:        {}".format(", ".join(str(n) for n in scale_values)))
        print(" Manage cluster:      {}".format(not args.no_cluster))
    else:
        print(" Mode:                single-host (legacy)")
        print(" Manage cluster:      {}".format(not args.no_cluster))
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

    runs_root.mkdir(parents=True, exist_ok=True)
    cluster_root.mkdir(parents=True, exist_ok=True)
    staging_root.mkdir(parents=True, exist_ok=True)

    # Storm logs dir — devcontainer-side path used for reading/archiving.
    local_logs_dir = FRAMEWORK_ROOT / "data" / "storm-logs"
    for sub in ("nimbus", "ui", "supervisor", "supervisor/profiler", "logviewer"):
        (local_logs_dir / sub).mkdir(parents=True, exist_ok=True)

    # Discover the master's host-side project root so docker-compose volume
    # paths in the rendered master compose use the path the host daemon sees,
    # not the /workspaces/... path that only exists inside the devcontainer.
    try:
        master.discover_host_project_dir()
        print("[grid-search] Master host project dir: {}".format(master.host_project_dir))
    except RuntimeError as exc:
        print("[grid-search] WARNING: {}".format(exc))
        print("[grid-search]          Falling back to devcontainer path for "
              "master compose volumes — Storm containers may not start.")

    # host_logs_dir is the path the host Docker daemon will bind-mount for logs.
    host_logs_dir = (
        Path(master.host_project_dir) / "data" / "storm-logs"
        if master.host_project_dir
        else local_logs_dir)

    # ---- Open SSH tunnels for all remote slaves upfront ----
    if is_multi_host:
        print("[grid-search] Opening SSH tunnels to {} slave(s)...".format(len(remote_slaves)))
        try:
            ClusterTopology(master=master, slaves=remote_slaves).open_tunnels()
        except RuntimeError as exc:
            print("[grid-search] ERROR: {}\n"
                  "             Check outer-host SSH access and that the "
                  "devcontainer is running on each slave.".format(exc))
            sys.exit(1)
        for s in remote_slaves:
            print("[grid-search]   {} -> localhost:{} (host dir: {})".format(
                s.hostname, s._local_forward_port, s.host_project_dir))
        print()

    # ---- Scale sweep ----
    run_id = args.start_run_id
    cluster_managers = []  # track for atexit cleanup

    def _shutdown_all():
        for cm in cluster_managers:
            try:
                cm.tear_down(remove_volumes=False)
            except Exception:
                pass
        if is_multi_host:
            for s in remote_slaves:
                try:
                    s.close_tunnel()
                except Exception:
                    pass

    atexit.register(_shutdown_all)

    for n in scale_values:
        print("=" * 60)
        print(" Scale step: N = {}".format(n))
        print("=" * 60)

        if is_multi_host:
            active_slaves = remote_slaves[:n]
            combined_mode = False
        else:
            # Legacy single-host: synthesize a local "slave".
            active_slaves = [build_local_supervisor(args, slot_ports)]
            combined_mode = True

        topology = ClusterTopology(master=master, slaves=active_slaves)

        # Render configs into a per-N subdir
        n_cluster_dir = cluster_root / "n={}".format(n)
        n_cluster_dir.mkdir(parents=True, exist_ok=True)
        renderer = ConfigRenderer(
            templates_dir=TEMPLATES_DIR,
            output_dir=n_cluster_dir,
            storm_image=args.storm_image,
            storm_version=args.storm_version,
            zookeeper_version=args.zookeeper_version,
        )
        if combined_mode:
            render = renderer.render_combined(topology, host_logs_dir, active_slaves[0])
            storm_conf_dir = render.combined_storm_yaml.local_path.parent
        else:
            render = renderer.render_multi_host(topology, host_logs_dir)
            storm_conf_dir = render.master_storm_yaml.local_path.parent

        # Per-N staging
        n_staging_root = staging_root / "n={}".format(n)
        staging_paths = StagingPaths(n_staging_root)

        # Cluster lifecycle
        manager = ClusterManager(topology, render, combined_mode=combined_mode)
        if not args.no_cluster:
            print("[grid-search] Bringing up cluster (N={})...".format(n))
            try:
                manager.bring_up(
                    fresh=True,
                    health_timeout=args.cluster_health_timeout,
                    sgx_check=(not args.no_sgx_check)
                              and (args.mode not in ("baseline", "microbatch-baseline")),
                )
                cluster_managers.append(manager)
            except Exception as exc:
                print("[grid-search] ERROR: cluster bring-up failed for N={}: {}".format(n, exc))
                fail_dir = runs_root / "n={}".format(n)
                fail_dir.mkdir(parents=True, exist_ok=True)
                (fail_dir / "cluster-failure.txt").write_text(str(exc))
                # Skip this N, move on.
                continue

        remote_collector = RemoteCollector(topology, staging_paths)

        # ---- Grid loop for this N ----
        for epochs, par, mu, num_users, num_keys, tick in grid:
            if args.mode == "comparison":
                combo_dir = runs_root / "n={}".format(n) / "tick{}_epochs{}_p{}_mu{}_u{}_k{}".format(
                    tick, epochs, par, mu, num_users, num_keys)
            else:
                combo_dir = None

            if args.resume_from_run > 0 and run_id < args.resume_from_run:
                print("[grid-search] Skipping run {} (resuming from run {})".format(
                    run_id, args.resume_from_run))
                run_id += 1
                continue

            for tt in types_to_run:
                if is_microbatch:
                    run_dir = (runs_root / "n={}".format(n)
                               / "p{}_sizes{}_runs{}_run{}".format(
                                   par, args.batch_sizes_gb.replace(",", "-"),
                                   args.runs_per_size, run_id))
                elif combo_dir is not None:
                    run_dir = combo_dir / tt.name
                else:
                    run_dir = (runs_root / "n={}".format(n)
                               / "tick{}_epochs{}_p{}_mu{}_u{}_k{}_run{}".format(
                                   tick, epochs, par, mu, num_users, num_keys, run_id))

                if is_microbatch:
                    try:
                        execute_microbatch_run(
                            topo_type=tt,
                            run_id=run_id,
                            parallelism=par,
                            batch_sizes_gb=args.batch_sizes_gb,
                            runs_per_size=args.runs_per_size,
                            bytes_per_tuple=args.bytes_per_tuple,
                            completion_timeout_ms=args.completion_timeout_ms,
                            seed=args.seed,
                            total_runs=total_runs,
                            run_dir=run_dir,
                            topology=topology,
                            staging_paths=staging_paths,
                            remote_collector=remote_collector,
                            storm_conf_dir=storm_conf_dir,
                            post_kill_sleep=args.post_kill_sleep,
                            poll_interval=args.poll_interval,
                        )
                    except Exception as exc:
                        print("[microbatch run {}] FAILED: {}".format(run_id, exc))
                        run_dir.mkdir(parents=True, exist_ok=True)
                        (run_dir / "run-error.txt").write_text(str(exc))
                    print()
                    continue

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
                    label=label,
                    poll_interval=args.poll_interval,
                    wait_safety_factor=args.wait_safety_factor,
                    startup_timeout=tt.startup_timeout,
                    total_runs=total_runs,
                    run_dir=run_dir,
                    topology=topology,
                    staging_paths=staging_paths,
                    remote_collector=remote_collector,
                    storm_conf_dir=storm_conf_dir,
                    post_kill_sleep=args.post_kill_sleep,
                    max_rsync_failures=args.max_rsync_failures,
                )

                try:
                    execute_run(params)
                except Exception as exc:
                    cleanup_failed_run(
                        tt, run_id, run_dir, tick, epochs, par, mu,
                        num_users, num_keys, args.seed, args.ground_truth,
                        storm_conf_dir, args.post_kill_sleep, exc)

                print()

            run_id += 1

        # Tear down before moving to next N
        if not args.no_cluster:
            print("[grid-search] Tearing down cluster (end of N={}).".format(n))
            try:
                manager.tear_down(remove_volumes=True)
                if manager in cluster_managers:
                    cluster_managers.remove(manager)
            except Exception as exc:
                print("[grid-search] WARNING: tear-down error: {}".format(exc))

    total_completed = run_id - args.start_run_id
    print("=" * 60)
    print(" Grid search complete. Combos processed: {}".format(total_completed))
    print(" Results archived in: {}".format(runs_root))
    print("=" * 60)


if __name__ == "__main__":
    main()
