"""Host abstractions for the multi-node Storm cluster.

Each remote slave runs a devcontainer that exposes SSH on port 2222 bound to
127.0.0.1 on the outer host.  The outer host itself is reachable over a normal
SSH port (default 22).  Because the firewall only forwards port 22, reaching
the devcontainer requires a two-hop setup:

  1. ``open_tunnel()`` opens ``ssh -N -L <local_port>:127.0.0.1:<container_port>
     outer_user@outer_host`` as a background process.
  2. All subsequent ``run()`` / ``scp_to()`` / ``rsync_from()`` calls go to
     ``127.0.0.1:<local_port>`` as ``container_user`` (default: ``dev``).

For the degenerate single-host case (hostname resolves to localhost) the tunnel
is skipped and commands run directly in the local shell.

Architecture diagram (per slave):

  master devcontainer
     |  ssh -L 15xxx:127.0.0.1:2222  outer_user@slave_real_ip (port 22)
     ↓
  slave outer host (port 22 open, 2222 firewall-blocked from outside)
     |  forwards 127.0.0.1:2222 → devcontainer sshd
     ↓
  slave devcontainer  (ssh dev@127.0.0.1 -p 15xxx from master)
     |  uses /var/run/docker.sock (DooD mount)
     ↓
  host docker daemon  → Storm containers (supervisor etc.)
"""

import os
import shlex
import socket as _socket
import subprocess
import time
from pathlib import Path


_LOCAL_NAMES = {"localhost", "127.0.0.1", "::1"}


def _is_local(hostname):
    # type: (str) -> bool
    if not hostname:
        return True
    if hostname in _LOCAL_NAMES:
        return True
    try:
        if hostname == _socket.gethostname():
            return True
        if hostname == _socket.getfqdn():
            return True
    except OSError:
        pass
    return False


def _find_free_port():
    # type: () -> int
    """Ask the OS for a free ephemeral port number."""
    with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class SyncStats(object):
    """Result of one rsync invocation against a single slave."""

    def __init__(self):
        self.bytes_transferred = 0
        self.files_transferred = 0
        self.failed_hosts = []  # type: list[str]

    def merge(self, other):
        # type: (SyncStats) -> None
        self.bytes_transferred += other.bytes_transferred
        self.files_transferred += other.files_transferred
        self.failed_hosts.extend(other.failed_hosts)


class SupervisorHost(object):
    """A remote (or local) Storm Supervisor machine reached via SSH tunnel.

    For remote slaves the tunnel must be opened with ``open_tunnel()`` before
    any ``run()`` / ``scp_to()`` / ``rsync_from()`` call.  Tunnels are closed
    with ``close_tunnel()`` (or automatically when the process exits if the
    caller registers ``close_tunnel()`` in an atexit handler).
    """

    def __init__(self,
                 hostname,
                 outer_user,
                 outer_key=None,
                 outer_port=22,
                 container_user="dev",
                 container_port=2222,
                 remote_data_dir="/opt/confidential-storm",
                 slot_ports=None,
                 rsync_bwlimit=0):
        # type: (str, str, object, int, str, int, str, list, int) -> None
        self.hostname = hostname
        self.outer_user = outer_user
        self.outer_key = Path(outer_key) if outer_key else None
        self.outer_port = outer_port
        self.container_user = container_user
        self.container_port = container_port
        self.remote_data_dir = remote_data_dir
        self.slot_ports = list(slot_ports) if slot_ports else [6700, 6701, 6702, 6703]
        self.rsync_bwlimit = rsync_bwlimit
        self.is_local = _is_local(hostname)

        # Populated by discover_host_project_dir() after the tunnel opens.
        # Used for docker-compose volume paths (host daemon resolves these as
        # HOST filesystem paths, not devcontainer paths).
        self.host_project_dir = None  # type: str

        # Tunnel state — populated by open_tunnel()
        self._local_forward_port = None  # type: int
        self._tunnel_proc = None  # type: subprocess.Popen

    # ---- Host path discovery -----------------------------------------------

    def discover_host_project_dir(self):
        # type: () -> str
        """Return the host-side project root by inspecting the devcontainer's mounts.

        Queries the bind-mount source for ``/workspaces/confidential-storm``
        inside the running devcontainer via the DooD socket.  This is more
        reliable than reading the ``devcontainer.local_folder`` label, which is
        only set when VS Code starts the container.

        Sets ``self.host_project_dir`` as a side-effect and returns the path.
        Raises RuntimeError on failure.
        """
        fmt = (
            "{{ range .Mounts }}"
            "{{ if eq .Destination \"/workspaces/confidential-storm\" }}"
            "{{ .Source }}"
            "{{ end }}"
            "{{ end }}"
        )
        proc = self.run(
            "docker", "inspect", "confidential-storm-devcontainer",
            "--format", fmt,
            capture_output=True,
        )
        path = (proc.stdout or "").strip()
        if not path:
            detail = (proc.stderr or "").strip()
            raise RuntimeError(
                "Could not discover host project dir on {} "
                "(no bind-mount for /workspaces/confidential-storm found; "
                "is confidential-storm-devcontainer running with the workspace "
                "mount and the docker socket accessible?){}"
                .format(self.hostname,
                        "\n  docker stderr: " + detail if detail else ""))
        self.host_project_dir = path
        return path

    # ---- Tunnel lifecycle -------------------------------------------------

    def open_tunnel(self, wait_timeout=20):
        # type: (int) -> None
        """Establish the SSH port-forward and wait for it to accept connections.

        Raises RuntimeError if the tunnel is not ready within ``wait_timeout``
        seconds.  No-op for local hosts.
        """
        if self.is_local:
            return
        if self._tunnel_proc is not None:
            return  # already open

        port = _find_free_port()
        cmd = [
            "ssh",
            "-N",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "BatchMode=yes",
            "-o", "ExitOnForwardFailure=yes",
            "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=3",
            "-p", str(self.outer_port),
            "-L", "127.0.0.1:{}:127.0.0.1:{}".format(port, self.container_port),
        ]
        if self.outer_key:
            cmd.extend(["-i", str(self.outer_key)])
        cmd.append("{}@{}".format(self.outer_user, self.hostname))

        self._tunnel_proc = subprocess.Popen(cmd,
                                             stdout=subprocess.DEVNULL,
                                             stderr=subprocess.DEVNULL)
        self._local_forward_port = port

        # Poll until the forwarded port accepts connections.
        deadline = time.time() + wait_timeout
        while time.time() < deadline:
            if self._tunnel_proc.poll() is not None:
                raise RuntimeError(
                    "SSH tunnel process to {} exited early (rc={})".format(
                        self.hostname, self._tunnel_proc.returncode))
            try:
                with _socket.create_connection(("127.0.0.1", port), timeout=1):
                    return
            except (OSError, _socket.timeout):
                time.sleep(0.5)

        self.close_tunnel()
        raise RuntimeError(
            "SSH tunnel to {} (port {}) did not open within {}s".format(
                self.hostname, self.container_port, wait_timeout))

    def close_tunnel(self):
        # type: () -> None
        """Terminate the SSH tunnel process.  No-op for local hosts or if not open."""
        if self._tunnel_proc is None:
            return
        self._tunnel_proc.terminate()
        try:
            self._tunnel_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._tunnel_proc.kill()
        self._tunnel_proc = None
        self._local_forward_port = None

    # ---- SSH ---------------------------------------------------------------

    def _ssh_opts(self):
        # type: () -> list[str]
        """Common SSH options (no -p / -i / destination)."""
        return [
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=10",
        ]

    def _container_ssh_base(self):
        # type: () -> list[str]
        """SSH argv prefix that reaches the devcontainer through the open tunnel."""
        if self._local_forward_port is None:
            raise RuntimeError(
                "Tunnel to {} is not open. Call open_tunnel() first.".format(self.hostname))
        cmd = ["ssh"] + self._ssh_opts() + ["-p", str(self._local_forward_port)]
        if self.outer_key:
            cmd.extend(["-i", str(self.outer_key)])
        cmd.append("{}@127.0.0.1".format(self.container_user))
        return cmd

    def ssh_cmd(self, *cmd):
        # type: (*str) -> list[str]
        """Build an argv that runs ``cmd`` on the host (or locally)."""
        if self.is_local:
            return list(cmd)
        remote = " ".join(shlex.quote(c) for c in cmd)
        return self._container_ssh_base() + [remote]

    def run(self, *cmd, **kwargs):
        # type: (*str, **object) -> subprocess.CompletedProcess
        """Run ``cmd`` on the host, returning the CompletedProcess."""
        check = bool(kwargs.pop("check", False))
        capture_output = bool(kwargs.pop("capture_output", False))
        timeout = kwargs.pop("timeout", None)
        argv = self.ssh_cmd(*cmd)
        pipe = subprocess.PIPE if capture_output else None
        return subprocess.run(argv, check=check,
                              stdout=pipe, stderr=pipe,
                              timeout=timeout, universal_newlines=True)

    # ---- File transfer -----------------------------------------------------

    def scp_to(self, local_path, remote_path):
        # type: (Path, str) -> None
        """Copy a single local file to ``remote_path`` inside the devcontainer."""
        if self.is_local:
            Path(remote_path).parent.mkdir(parents=True, exist_ok=True)
            subprocess.run(["cp", "-f", str(local_path), remote_path], check=True)
            return
        parent = os.path.dirname(remote_path) or "."
        self.run("mkdir", "-p", parent, check=True)
        cmd = ["scp"] + self._ssh_opts() + ["-P", str(self._local_forward_port)]
        if self.outer_key:
            cmd.extend(["-i", str(self.outer_key)])
        cmd.extend([str(local_path),
                    "{}@127.0.0.1:{}".format(self.container_user, remote_path)])
        subprocess.run(cmd, check=True)

    def rsync_from(self, remote_path, local_dir, includes=None,
                   excludes=None, inplace=False):
        # type: (str, Path, list, list, bool) -> SyncStats
        """Pull ``remote_path`` (directory) into ``local_dir`` via rsync.

        Returns a SyncStats; on failure records ``self.hostname`` in
        ``failed_hosts`` instead of raising.
        """
        local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)
        stats = SyncStats()

        cmd = ["rsync", "-az", "--partial", "--stats", "--out-format=%f"]
        if inplace:
            cmd.append("--inplace")
        if self.rsync_bwlimit > 0:
            cmd.append("--bwlimit={}".format(self.rsync_bwlimit))
        for pat in includes or []:
            cmd.append("--include={}".format(pat))
        for pat in excludes or []:
            cmd.append("--exclude={}".format(pat))

        if not remote_path.endswith("/"):
            remote_path = remote_path + "/"

        if self.is_local:
            src = remote_path
        else:
            if self._local_forward_port is None:
                stats.failed_hosts.append(self.hostname)
                return stats
            ssh_e = (["ssh"] + self._ssh_opts()
                     + ["-p", str(self._local_forward_port)])
            if self.outer_key:
                ssh_e.extend(["-i", str(self.outer_key)])
            cmd.extend(["-e", " ".join(shlex.quote(p) for p in ssh_e)])
            src = "{}@127.0.0.1:{}".format(self.container_user, remote_path)

        cmd.extend([src, str(local_dir) + "/"])

        try:
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  universal_newlines=True, timeout=120)
        except subprocess.TimeoutExpired:
            stats.failed_hosts.append(self.hostname)
            return stats

        if proc.returncode not in (0, 24):
            stats.failed_hosts.append(self.hostname)
            return stats

        for line in (proc.stderr or "").splitlines():
            if line.startswith("Number of regular files transferred:"):
                try:
                    stats.files_transferred = int(
                        line.split(":", 1)[1].strip().replace(",", ""))
                except ValueError:
                    pass
            elif line.startswith("Total transferred file size:"):
                try:
                    size_str = line.split(":", 1)[1].strip().split()[0].replace(",", "")
                    stats.bytes_transferred = int(size_str)
                except (ValueError, IndexError):
                    pass
        return stats

    def reachable(self):
        # type: () -> bool
        """True iff the tunnel is open, SSH into the devcontainer works, and docker responds."""
        if self.is_local:
            return subprocess.run(["docker", "version"],
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE).returncode == 0
        if self._local_forward_port is None:
            return False
        try:
            proc = self.run("docker", "version", capture_output=True, timeout=15)
        except subprocess.TimeoutExpired:
            return False
        return proc.returncode == 0


class MasterHost(object):
    """The orchestrator node.  Runs Nimbus + ZooKeeper + Storm UI."""

    def __init__(self, hostname, nimbus_thrift_port=6627, zk_port=2181,
                 ui_port=8080, logviewer_port=8000):
        # type: (str, int, int, int, int) -> None
        self.hostname = hostname
        self.nimbus_thrift_port = nimbus_thrift_port
        self.zk_port = zk_port
        self.ui_port = ui_port
        self.logviewer_port = logviewer_port
        self.host_project_dir = None  # type: str

    def discover_host_project_dir(self):
        # type: () -> str
        """Return the host-side project root by inspecting the devcontainer's mounts."""
        fmt = (
            "{{ range .Mounts }}"
            "{{ if eq .Destination \"/workspaces/confidential-storm\" }}"
            "{{ .Source }}"
            "{{ end }}"
            "{{ end }}"
        )
        proc = subprocess.run(
            ["docker", "inspect", "confidential-storm-devcontainer",
             "--format", fmt],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True)
        path = (proc.stdout or "").strip()
        if not path:
            detail = (proc.stderr or "").strip()
            raise RuntimeError(
                "Could not discover host project dir on master "
                "(no bind-mount for /workspaces/confidential-storm found; "
                "is confidential-storm-devcontainer running with the workspace "
                "mount and /var/run/docker.sock accessible?){}"
                .format("\n  docker stderr: " + detail if detail else ""))
        self.host_project_dir = path
        return path


class ClusterTopology(object):
    """A master plus the slaves active for the current scale step."""

    def __init__(self, master, slaves):
        # type: (MasterHost, list) -> None
        self.master = master
        self.slaves = list(slaves)

    def n(self):
        # type: () -> int
        return len(self.slaves)

    def slave_hostnames(self):
        # type: () -> list
        return [s.hostname for s in self.slaves]

    def open_tunnels(self, wait_timeout=20):
        # type: (int) -> None
        """Open SSH tunnels and discover host project dirs for every non-local slave."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        remote = [s for s in self.slaves if not s.is_local]
        if not remote:
            return

        def _open(slave):
            slave.open_tunnel(wait_timeout=wait_timeout)
            slave.discover_host_project_dir()
            return slave.hostname

        with ThreadPoolExecutor(max_workers=len(remote)) as ex:
            futures = {ex.submit(_open, s): s for s in remote}
            for fut in as_completed(futures):
                slave = futures[fut]
                exc = fut.exception()
                if exc:
                    raise RuntimeError(
                        "Failed to open tunnel to {}: {}".format(slave.hostname, exc))

    def close_tunnels(self):
        # type: () -> None
        """Close all open SSH tunnels (best-effort, does not raise)."""
        for slave in self.slaves:
            try:
                slave.close_tunnel()
            except Exception:
                pass
