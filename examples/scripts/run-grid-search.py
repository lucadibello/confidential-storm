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

    p.add_argument("--mode", choices=["baseline", "enclave", "comparison"],
                   default=env_str("MODE", "enclave"),
                   help="Which topology to benchmark (default: enclave)")

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
                   default=env_str("REMOTE_DATA_DIR", "/opt/confidential-storm"))
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
        nimbus_thrift_port=6627,
        zk_port=2181,
        ui_port=8080,
        logviewer_port=8000,
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
    else:
        types_to_run = [BASELINE, ENCLAVE]

    BASELINE.startup_timeout = args.startup_timeout_baseline
    ENCLAVE.startup_timeout = args.startup_timeout_enclave

    # Build the grid
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
    else:
        base = ENCLAVE_PROJECT / "data" / "runs"
    runs_root = (base / label / grid_timestamp) if label else (base / grid_timestamp)
    cluster_root = FRAMEWORK_ROOT / "data" / "cluster" / grid_timestamp
    staging_root = FRAMEWORK_ROOT / "data" / "staging" / grid_timestamp

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
    print(" Archive dir:         {}".format(runs_root))
    if label:
        print(" Label:               {}".format(label))
    print(" Start run ID:        {}".format(args.start_run_id))
    if args.resume_from_run > 0:
        print(" Resume from run:     {} (skipping earlier runs)".format(args.resume_from_run))
    print(" Master host:         {}".format(master.hostname))
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

    # Storm logs dir on master (the legacy ./data/storm-logs/{nimbus,ui,supervisor}/)
    local_logs_dir = FRAMEWORK_ROOT / "data" / "storm-logs"
    for sub in ("nimbus", "ui", "supervisor", "supervisor/profiler", "logviewer"):
        (local_logs_dir / sub).mkdir(parents=True, exist_ok=True)

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
            print("[grid-search]   {} -> localhost:{}".format(
                s.hostname, s._local_forward_port))
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
            zookeeper_version=args.zookeeper_version,
        )
        if combined_mode:
            render = renderer.render_combined(topology, local_logs_dir, active_slaves[0])
            storm_conf_dir = render.combined_storm_yaml.local_path.parent
        else:
            render = renderer.render_multi_host(topology, local_logs_dir)
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
                    sgx_check=(not args.no_sgx_check) and (args.mode != "baseline"),
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
                if combo_dir is not None:
                    run_dir = combo_dir / tt.name
                else:
                    run_dir = (runs_root / "n={}".format(n)
                               / "tick{}_epochs{}_p{}_mu{}_u{}_k{}_run{}".format(
                                   tick, epochs, par, mu, num_users, num_keys, run_id))

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
