"""Per-run filesystem layout on the master node.

The orchestrator pulls artifacts from every slave into a master-local staging
tree, then materialises a symlink farm (the "merged view") so that the
existing profiler-CSV and worker-log helpers can keep using a single logical
directory.

Layout under STAGING_ROOT (one staging dir per cluster bring-up, reused
across all runs at a given scale value):

    <staging_root>/
      <slave_host_a>/
        profiler/                  rsync target for /logs/storm/profiler/*
        workers-artifacts/         rsync target for /logs/storm/workers-artifacts/*
      <slave_host_b>/
        profiler/
        workers-artifacts/
      merged/
        profiler/                  symlinks into <slave>/profiler/*.csv
        workers-artifacts/         symlinks into <slave>/workers-artifacts/*

Profiler CSV filenames are globally unique across the topology (task IDs are
cluster-wide), so flat symlinks in merged/profiler/ cannot collide.  Worker
artifact directories are keyed by ``{topo_name}-{supervisor_id}`` which is
also unique per supervisor host.
"""

from pathlib import Path


class StagingPaths(object):
    """Bundle of derived paths for one cluster bring-up."""

    def __init__(self, staging_root):
        # type: (Path) -> None
        self.staging_root = Path(staging_root)
        self.merged_root = self.staging_root / "merged"
        self.merged_profiler = self.merged_root / "profiler"
        self.merged_workers_artifacts = self.merged_root / "workers-artifacts"

    def slave_root(self, hostname):
        # type: (str) -> Path
        return self.staging_root / hostname

    def slave_profiler(self, hostname):
        # type: (str) -> Path
        return self.slave_root(hostname) / "profiler"

    def slave_workers_artifacts(self, hostname):
        # type: (str) -> Path
        return self.slave_root(hostname) / "workers-artifacts"

    def ensure(self, slave_hostnames):
        # type: (list[str]) -> None
        self.merged_profiler.mkdir(parents=True, exist_ok=True)
        self.merged_workers_artifacts.mkdir(parents=True, exist_ok=True)
        for host in slave_hostnames:
            self.slave_profiler(host).mkdir(parents=True, exist_ok=True)
            self.slave_workers_artifacts(host).mkdir(parents=True, exist_ok=True)
