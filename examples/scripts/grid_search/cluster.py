"""Bring up / tear down the Storm cluster across master + N slaves.

Bring-up order:
  1. (fresh-per-N) tear everything down with ``-v``.
  2. On master: docker compose up -d (zk + nimbus + ui + logviewer; or combined
     for N=1 single-host mode).
  3. Wait for Nimbus thrift port to accept TCP connections.
  4. In parallel on each slave: scp rendered compose + storm.yaml to remote
     data dir, then docker compose up -d.
  5. Poll Storm UI REST until ``supervisor_count >= N``.
  6. Optional SGX preflight per slave.

Tear-down is idempotent: every step uses ``check=False`` so a half-dead slave
does not block the rest.
"""

import json
import socket
import subprocess
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .config_render import ClusterRender
from .hosts import ClusterTopology


class ClusterManager(object):

    def __init__(self, topology, render, combined_mode=False):
        # type: (ClusterTopology, ClusterRender, bool) -> None
        self.topology = topology
        self.render = render
        self.combined_mode = combined_mode

    # ---- High-level lifecycle ---------------------------------------------

    def bring_up(self, fresh=True, health_timeout=180, sgx_check=True):
        # type: (bool, int, bool) -> None
        if fresh:
            self.tear_down(remove_volumes=True)

        if self.combined_mode:
            self._compose_up_local(self.render.combined_compose.local_path)
        else:
            self._compose_up_local(self.render.master_compose.local_path)
            if not self._wait_thrift_up(timeout=120):
                raise RuntimeError("Nimbus thrift port {} on {} did not open in time".format(
                    self.topology.master.nimbus_thrift_port,
                    self.topology.master.hostname))
            self._push_and_start_slaves()

        if not self._wait_supervisor_count(self.topology.n() or 1, timeout=health_timeout):
            raise RuntimeError("Cluster never reported {} supervisor(s) within {}s".format(
                self.topology.n() or 1, health_timeout))

        if sgx_check and not self.combined_mode:
            self._preflight_sgx()

    def tear_down(self, remove_volumes=True):
        # type: (bool) -> None
        flags = ["down"] + (["-v"] if remove_volumes else [])

        if not self.combined_mode:
            for slave in self.topology.slaves:
                compose_remote = "{}/docker-compose.yml".format(slave.remote_data_dir)
                slave.run("docker", "compose", "-f", compose_remote, *flags, check=False)

        compose_local = (self.render.combined_compose.local_path if self.combined_mode
                         else self.render.master_compose.local_path)
        subprocess.run(["docker", "compose", "-f", str(compose_local), *flags], check=False)

    # ---- Bring-up internals -----------------------------------------------

    def _compose_up_local(self, compose_path):
        # type: (Path) -> None
        subprocess.run(["docker", "compose", "-f", str(compose_path), "up", "-d"],
                       check=True)

    def _push_and_start_slaves(self):
        def push_one(slave):
            yaml_file = self.render.slave_storm_yamls[slave.hostname]
            compose_file = self.render.slave_composes[slave.hostname]
            # Create remote data dirs.
            slave.run("mkdir", "-p",
                      "{}/conf".format(slave.remote_data_dir),
                      "{}/data/storm-logs/supervisor/profiler".format(slave.remote_data_dir),
                      "{}/data/storm-logs/supervisor/workers-artifacts".format(slave.remote_data_dir),
                      check=True)
            slave.scp_to(yaml_file.local_path, yaml_file.remote_path)
            slave.scp_to(compose_file.local_path, compose_file.remote_path)
            slave.run("docker", "compose", "-f",
                      "{}/docker-compose.yml".format(slave.remote_data_dir),
                      "up", "-d", check=True)

        if not self.topology.slaves:
            return
        with ThreadPoolExecutor(max_workers=len(self.topology.slaves)) as ex:
            list(ex.map(push_one, self.topology.slaves))

    # ---- Health checks -----------------------------------------------------

    def _wait_thrift_up(self, timeout):
        # type: (int) -> bool
        deadline = time.time() + timeout
        addr = (self.topology.master.hostname, self.topology.master.nimbus_thrift_port)
        while time.time() < deadline:
            try:
                with socket.create_connection(addr, timeout=3):
                    return True
            except (OSError, socket.timeout):
                time.sleep(2)
        return False

    def _wait_supervisor_count(self, n, timeout):
        # type: (int, int) -> bool
        url = "http://{}:{}/api/v1/cluster/summary".format(
            self.topology.master.hostname, self.topology.master.ui_port)
        deadline = time.time() + timeout
        last_seen = -1
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=5) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    count = int(data.get("supervisors", 0))
                    if count != last_seen:
                        print("[cluster] supervisor count: {}/{}".format(count, n))
                        last_seen = count
                    if count >= n:
                        return True
            except (urllib.error.URLError, ValueError, KeyError, OSError):
                pass
            time.sleep(3)
        return False

    def _preflight_sgx(self):
        for slave in self.topology.slaves:
            proc = slave.run("test", "-S", "/var/run/aesmd/aesm.socket",
                             check=False)
            if proc.returncode != 0:
                print("[cluster] WARNING: aesm socket missing on {}".format(slave.hostname))
            proc = slave.run("test", "-c", "/dev/sgx_enclave", check=False)
            if proc.returncode != 0:
                print("[cluster] WARNING: /dev/sgx_enclave missing on {}".format(slave.hostname))

    # ---- Query helpers -----------------------------------------------------

    def supervisor_count(self):
        # type: () -> int
        url = "http://{}:{}/api/v1/cluster/summary".format(
            self.topology.master.hostname, self.topology.master.ui_port)
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return int(data.get("supervisors", 0))
        except (urllib.error.URLError, ValueError, KeyError, OSError):
            return -1
