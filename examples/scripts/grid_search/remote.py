"""Pull profiler CSVs + worker logs from every slave onto the master.

The orchestrator runs ``sync_profiler()`` once per poll iteration (every
``--poll-interval`` seconds) and ``sync_worker_logs()`` once per iteration in
``poll_completion``.  After each sync we refresh a symlink farm at
``<staging>/merged/{profiler,workers-artifacts}/`` so existing CSV/log
helpers can keep using a single logical directory.

Lifecycle events in the profiler CSVs are ``writer.flush()``-immediate
(ProfilerReport.java:150-163), so a 10s pull cadence catches them with
bounded lag.
"""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .hosts import ClusterTopology, SyncStats
from .paths import StagingPaths


class RemoteCollector(object):
    """Pulls profiler CSVs + worker logs from every slave.

    The remote layout (on each slave) mirrors today's host layout:

        <slave.remote_data_dir>/
          data/storm-logs/supervisor/
            profiler/*.csv
            workers-artifacts/<topo_name>-<sid>/<port>/{worker.log,worker.log.err,...}

    Local staging layout on the master is in StagingPaths.
    """

    def __init__(self, topology, staging_paths, parallel_syncs=8):
        # type: (ClusterTopology, StagingPaths, int) -> None
        self.topology = topology
        self.paths = staging_paths
        self.parallel_syncs = parallel_syncs
        self.paths.ensure(self.topology.slave_hostnames())

    # ---- Sync helpers ------------------------------------------------------

    def _profiler_remote(self, slave):
        return "{}/data/storm-logs/supervisor/profiler/".format(slave.remote_data_dir)

    def _workers_remote(self, slave):
        return "{}/data/storm-logs/supervisor/workers-artifacts/".format(slave.remote_data_dir)

    def _sync_one_profiler(self, slave):
        return slave.rsync_from(
            self._profiler_remote(slave),
            self.paths.slave_profiler(slave.hostname),
            includes=["*.csv", "*.txt"],
            excludes=["*"],
            inplace=True,  # the bolt holds the CSV writer open and appends
        )

    def _sync_one_workers(self, slave):
        return slave.rsync_from(
            self._workers_remote(slave),
            self.paths.slave_workers_artifacts(slave.hostname),
            inplace=False,
        )

    def _parallel(self, fn):
        # type: (callable) -> SyncStats
        merged = SyncStats()
        if not self.topology.slaves:
            return merged
        with ThreadPoolExecutor(max_workers=self.parallel_syncs) as ex:
            for stats in ex.map(fn, self.topology.slaves):
                merged.merge(stats)
        return merged

    def sync_profiler(self):
        # type: () -> SyncStats
        stats = self._parallel(self._sync_one_profiler)
        self.merge_view_profiler()
        return stats

    def sync_worker_logs(self):
        # type: () -> SyncStats
        stats = self._parallel(self._sync_one_workers)
        self.merge_view_workers()
        return stats

    def sync_all(self):
        # type: () -> SyncStats
        s1 = self.sync_profiler()
        s2 = self.sync_worker_logs()
        s1.merge(s2)
        return s1

    # ---- Merged symlink view ----------------------------------------------

    def _refresh_links(self, dest_dir, sources):
        # type: (Path, list[Path]) -> None
        """Ensure ``dest_dir`` contains symlinks pointing at every entry in ``sources``.

        Stale symlinks (whose target no longer exists) are removed.  Existing
        non-symlink files are left alone.
        """
        dest_dir.mkdir(parents=True, exist_ok=True)
        wanted = {p.name: p for p in sources}
        # Remove stale links.
        for entry in dest_dir.iterdir():
            if entry.is_symlink() and entry.name not in wanted:
                try:
                    entry.unlink()
                except OSError:
                    pass
        # Create/update links.
        for name, src in wanted.items():
            link = dest_dir / name
            if link.is_symlink():
                if link.resolve() == src.resolve():
                    continue
                try:
                    link.unlink()
                except OSError:
                    pass
            elif link.exists():
                # Real file with the same name -- skip to avoid clobbering.
                continue
            try:
                link.symlink_to(src.resolve())
            except OSError:
                pass

    def merge_view_profiler(self):
        sources = []
        for slave in self.topology.slaves:
            slave_dir = self.paths.slave_profiler(slave.hostname)
            if slave_dir.is_dir():
                sources.extend(slave_dir.glob("*.csv"))
                sources.extend(slave_dir.glob("*.txt"))
        self._refresh_links(self.paths.merged_profiler, sources)

    def merge_view_workers(self):
        sources = []
        for slave in self.topology.slaves:
            slave_dir = self.paths.slave_workers_artifacts(slave.hostname)
            if slave_dir.is_dir():
                for sub in slave_dir.iterdir():
                    if sub.is_dir():
                        sources.append(sub)
        self._refresh_links(self.paths.merged_workers_artifacts, sources)

    def merge_view(self):
        # type: () -> Path
        self.merge_view_profiler()
        self.merge_view_workers()
        return self.paths.merged_root

    # ---- Remote-side cleanup ----------------------------------------------

    def clean_remote_dirs(self, topo_name):
        # type: (str) -> None
        """Wipe per-topology artifacts from every slave (called pre-submit).

        sudo is required because the Storm supervisor container runs as root,
        so the files it writes to the bind-mounted volume are owned by root.
        The dev user has NOPASSWD:ALL in the devcontainer sudoers.
        """
        for slave in self.topology.slaves:
            slave.run("sh", "-c",
                      "sudo rm -f {0}/data/storm-logs/supervisor/profiler/*.csv "
                      "{0}/data/storm-logs/supervisor/profiler/*.txt".format(
                          slave.remote_data_dir),
                      check=False)
            slave.run("sh", "-c",
                      "sudo rm -rf {0}/data/storm-logs/supervisor/workers-artifacts/{1}-*".format(
                          slave.remote_data_dir, topo_name),
                      check=False)

    def clean_local_staging(self):
        """Wipe per-slave staging + the merged symlink farm (called pre-submit)."""
        import shutil
        for slave in self.topology.slaves:
            for d in (self.paths.slave_profiler(slave.hostname),
                      self.paths.slave_workers_artifacts(slave.hostname)):
                if d.exists():
                    shutil.rmtree(str(d), ignore_errors=True)
                d.mkdir(parents=True, exist_ok=True)
        for d in (self.paths.merged_profiler, self.paths.merged_workers_artifacts):
            if d.exists():
                shutil.rmtree(str(d), ignore_errors=True)
            d.mkdir(parents=True, exist_ok=True)
