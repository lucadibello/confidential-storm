"""Host abstractions for the multi-node Storm cluster.

A SupervisorHost wraps SSH/SCP/rsync calls to a slave machine.  A
MasterHost holds the addresses the slaves use to reach Nimbus / ZooKeeper /
the Storm UI.  ClusterTopology bundles them for a given scale step.

When ``hostname`` resolves to the local machine (loopback or empty), the
SupervisorHost short-circuits SSH and runs the commands locally instead.
This preserves the degenerate N=1 single-host workflow used today.
"""

import os
import shlex
import socket
import subprocess
from pathlib import Path


_LOCAL_NAMES = {"localhost", "127.0.0.1", "::1"}


def _is_local(hostname):
    # type: (str) -> bool
    if not hostname:
        return True
    if hostname in _LOCAL_NAMES:
        return True
    try:
        if hostname == socket.gethostname():
            return True
        if hostname == socket.getfqdn():
            return True
    except OSError:
        pass
    return False


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
    """A remote (or local) Storm Supervisor machine."""

    def __init__(self, hostname, ssh_user, ssh_key=None, ssh_port=22,
                 remote_data_dir="/opt/confidential-storm",
                 slot_ports=None, rsync_bwlimit=0):
        # type: (str, str, Path, int, str, list[int], int) -> None
        self.hostname = hostname
        self.ssh_user = ssh_user
        self.ssh_key = Path(ssh_key) if ssh_key else None
        self.ssh_port = ssh_port
        self.remote_data_dir = remote_data_dir
        self.slot_ports = list(slot_ports) if slot_ports else [6700, 6701, 6702, 6703]
        self.rsync_bwlimit = rsync_bwlimit
        self.is_local = _is_local(hostname)

    # ---- SSH ---------------------------------------------------------------

    def _ssh_base(self):
        # type: () -> list[str]
        cmd = ["ssh",
               "-o", "StrictHostKeyChecking=accept-new",
               "-o", "BatchMode=yes",
               "-o", "ConnectTimeout=10",
               "-p", str(self.ssh_port)]
        if self.ssh_key:
            cmd.extend(["-i", str(self.ssh_key)])
        cmd.append("{}@{}".format(self.ssh_user, self.hostname))
        return cmd

    def ssh_cmd(self, *cmd):
        # type: (*str) -> list[str]
        """Build an argv that runs ``cmd`` on the remote host."""
        if self.is_local:
            return list(cmd)
        # Quote so the remote shell sees the same arguments.
        remote = " ".join(shlex.quote(c) for c in cmd)
        return self._ssh_base() + [remote]

    def run(self, *cmd, **kwargs):
        # type: (*str, **object) -> subprocess.CompletedProcess
        """Run ``cmd`` on the host, returning the CompletedProcess.

        kwargs: check (default False), capture_output (default False), input.
        """
        check = bool(kwargs.pop("check", False))
        capture_output = bool(kwargs.pop("capture_output", False))
        timeout = kwargs.pop("timeout", None)
        argv = self.ssh_cmd(*cmd)
        return subprocess.run(argv, check=check, capture_output=capture_output,
                              timeout=timeout, text=True)

    # ---- File transfer -----------------------------------------------------

    def scp_to(self, local_path, remote_path):
        # type: (Path, str) -> None
        """Copy a single local file to the remote path."""
        if self.is_local:
            Path(remote_path).parent.mkdir(parents=True, exist_ok=True)
            subprocess.run(["cp", "-f", str(local_path), remote_path], check=True)
            return
        # Ensure parent dir exists remotely.
        parent = os.path.dirname(remote_path) or "."
        self.run("mkdir", "-p", parent, check=True)
        cmd = ["scp",
               "-o", "StrictHostKeyChecking=accept-new",
               "-o", "BatchMode=yes",
               "-P", str(self.ssh_port)]
        if self.ssh_key:
            cmd.extend(["-i", str(self.ssh_key)])
        cmd.extend([str(local_path),
                    "{}@{}:{}".format(self.ssh_user, self.hostname, remote_path)])
        subprocess.run(cmd, check=True)

    def rsync_from(self, remote_path, local_dir, includes=None,
                   excludes=None, inplace=False):
        # type: (str, Path, list[str], list[str], bool) -> SyncStats
        """Pull ``remote_path`` (a directory, trailing slash assumed) into ``local_dir``.

        On failure, returns a SyncStats with ``self.hostname`` recorded in
        ``failed_hosts`` rather than raising.  Callers decide whether
        repeated failures are fatal.
        """
        local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)
        stats = SyncStats()

        cmd = ["rsync", "-az", "--partial",
               "--stats", "--out-format=%f"]
        if inplace:
            cmd.append("--inplace")
        if self.rsync_bwlimit > 0:
            cmd.append("--bwlimit={}".format(self.rsync_bwlimit))
        for pat in includes or []:
            cmd.append("--include={}".format(pat))
        for pat in excludes or []:
            cmd.append("--exclude={}".format(pat))

        # Make sure remote_path ends with '/' so rsync copies dir contents.
        if not remote_path.endswith("/"):
            remote_path = remote_path + "/"

        if self.is_local:
            src = remote_path
        else:
            ssh_e = ["ssh",
                     "-o", "StrictHostKeyChecking=accept-new",
                     "-o", "BatchMode=yes",
                     "-o", "ConnectTimeout=10",
                     "-p", str(self.ssh_port)]
            if self.ssh_key:
                ssh_e.extend(["-i", str(self.ssh_key)])
            cmd.extend(["-e", " ".join(shlex.quote(p) for p in ssh_e)])
            src = "{}@{}:{}".format(self.ssh_user, self.hostname, remote_path)

        cmd.extend([src, str(local_dir) + "/"])

        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        except subprocess.TimeoutExpired:
            stats.failed_hosts.append(self.hostname)
            return stats

        if proc.returncode not in (0, 24):  # 24 = "some files vanished during transfer"
            stats.failed_hosts.append(self.hostname)
            return stats

        # Best-effort parse of --stats output for byte/file counts.
        for line in (proc.stderr or "").splitlines():
            if line.startswith("Number of regular files transferred:"):
                try:
                    stats.files_transferred = int(line.split(":", 1)[1].strip().replace(",", ""))
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
        """True iff we can SSH and run ``docker version``."""
        if self.is_local:
            return subprocess.run(["docker", "version"],
                                  capture_output=True).returncode == 0
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


class ClusterTopology(object):
    """A master plus the slaves active for the current scale step."""

    def __init__(self, master, slaves):
        # type: (MasterHost, list[SupervisorHost]) -> None
        self.master = master
        self.slaves = list(slaves)

    def n(self):
        # type: () -> int
        return len(self.slaves)

    def slave_hostnames(self):
        # type: () -> list[str]
        return [s.hostname for s in self.slaves]
