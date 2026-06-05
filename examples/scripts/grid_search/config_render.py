"""Render storm.yaml and docker-compose.yml templates for a given cluster.

Templates live in examples/scripts/templates/*.tmpl and use
``string.Template`` ($var) placeholders so they do not collide with YAML's
own braces.

Renders go under ``<output_dir>/<role>/`` so each scale step gets its own
self-contained set of configs that can be inspected after the fact.
"""

import string
from pathlib import Path

from .hosts import ClusterTopology, SupervisorHost


def _read_template(template_dir, name):
    # type: (Path, str) -> string.Template
    with open(str(template_dir / name)) as f:
        return string.Template(f.read())


def _slot_ports_yaml(ports):
    # type: (list[int]) -> str
    return "\n".join("  - {}".format(p) for p in ports)


class RenderedFile(object):
    """Pair of (local source path, optional remote destination path)."""

    def __init__(self, local_path, remote_path=None):
        # type: (Path, str) -> None
        self.local_path = local_path
        self.remote_path = remote_path


class ClusterRender(object):
    """Result of rendering all configs for one scale step."""

    def __init__(self):
        self.master_storm_yaml = None    # type: RenderedFile
        self.master_compose = None       # type: RenderedFile
        # slave hostname -> RenderedFile
        self.slave_storm_yamls = {}      # type: dict[str, RenderedFile]
        self.slave_composes = {}         # type: dict[str, RenderedFile]
        # For degenerate N=1 single-host mode (master and only "slave" are the
        # same machine), we use a combined compose + storm.yaml instead.
        self.combined_storm_yaml = None  # type: RenderedFile
        self.combined_compose = None     # type: RenderedFile


class ConfigRenderer(object):
    """Renders storm.yaml + docker-compose.yml for master + each slave."""

    def __init__(self, templates_dir, output_dir, storm_image="confidential-storm:latest",
                 storm_version="2.8.3", zookeeper_version="3.9"):
        # type: (Path, Path, str, str, str) -> None
        self.templates_dir = Path(templates_dir)
        self.output_dir = Path(output_dir)
        self.storm_image = storm_image
        self.storm_version = storm_version
        self.zookeeper_version = zookeeper_version

    # ---- Multi-host rendering ----------------------------------------------

    def render_multi_host(self, topology, host_logs_dir):
        # type: (ClusterTopology, Path) -> ClusterRender
        """Render master compose + per-slave compose for a multi-host cluster.

        host_logs_dir: storm-logs directory as the **host** Docker daemon sees
            it (e.g. /home/luca/confidential-storm/data/storm-logs).  This is
            used for bind-mount paths in docker-compose.yml files so that the
            host daemon can resolve them correctly when docker compose runs
            inside the devcontainer with Docker-outside-of-Docker.
        """
        out = ClusterRender()
        master_dir = self.output_dir / "master"
        master_dir.mkdir(parents=True, exist_ok=True)

        # Master storm.yaml
        master_yaml_path = master_dir / "storm.yaml"
        slots_per_slave = len(topology.slaves[0].slot_ports) if topology.slaves else 0
        topology_workers = len(topology.slaves) * slots_per_slave
        master_yaml_path.write_text(_read_template(
            self.templates_dir, "storm.master.yaml.tmpl").substitute(
                master_host=topology.master.hostname,
                zk_port=topology.master.zk_port,
                nimbus_thrift_port=topology.master.nimbus_thrift_port,
                ui_port=topology.master.ui_port,
                logviewer_port=topology.master.logviewer_port,
                topology_workers=topology_workers,
        ))
        out.master_storm_yaml = RenderedFile(master_yaml_path)

        # Master compose — volumes use the host-side path so the host Docker
        # daemon can resolve them when docker compose runs inside the devcontainer.
        master_compose_path = master_dir / "docker-compose.yml"
        master_host_logs = (
            "{}/data/storm-logs".format(topology.master.host_project_dir)
            if topology.master.host_project_dir
            else str(host_logs_dir.resolve()))
        master_host_conf = (
            str(Path(topology.master.host_project_dir) / "data" / "cluster"
                / master_yaml_path.relative_to(self.output_dir.parent.parent))
            if topology.master.host_project_dir
            else str(master_yaml_path.resolve()))
        master_compose_path.write_text(_read_template(
            self.templates_dir, "docker-compose.master.yml.tmpl").substitute(
                storm_image=self.storm_image,
                storm_version=self.storm_version,
                build_context=topology.master.container_project_dir,
                zookeeper_version=self.zookeeper_version,
                zk_port=topology.master.zk_port,
                local_conf_path=master_host_conf,
                local_logs_path=master_host_logs,
        ))
        out.master_compose = RenderedFile(master_compose_path)

        # Per-slave configs
        for slave in topology.slaves:
            sdir = self.output_dir / slave.hostname
            sdir.mkdir(parents=True, exist_ok=True)

            slave_yaml_local = sdir / "storm.yaml"
            slave_yaml_local.write_text(_read_template(
                self.templates_dir, "storm.slave.yaml.tmpl").substitute(
                    master_host=topology.master.hostname,
                    slave_host=slave.hostname,
                    zk_port=topology.master.zk_port,
                    nimbus_thrift_port=topology.master.nimbus_thrift_port,
                    logviewer_port=topology.master.logviewer_port,
                    slot_ports_yaml=_slot_ports_yaml(slave.slot_ports),
            ))
            # SCP target uses the devcontainer path (remote_data_dir).
            out.slave_storm_yamls[slave.hostname] = RenderedFile(
                slave_yaml_local,
                remote_path="{}/conf/storm.yaml".format(slave.remote_data_dir))

            # Volume paths in the compose file must be HOST paths so the host
            # Docker daemon resolves them correctly (DooD).  Fall back to
            # remote_data_dir if host_project_dir was not discovered yet.
            host_base = slave.host_project_dir or slave.remote_data_dir
            slave_compose_local = sdir / "docker-compose.yml"
            slave_compose_local.write_text(_read_template(
                self.templates_dir, "docker-compose.slave.yml.tmpl").substitute(
                    storm_image=self.storm_image,
                    storm_version=self.storm_version,
                    build_context=host_base,
                    remote_conf_path="{}/conf/storm.yaml".format(host_base),
                    remote_logs_path="{}/data/storm-logs".format(host_base),
            ))
            # SCP target uses the devcontainer path (same physical file via bind-mount).
            out.slave_composes[slave.hostname] = RenderedFile(
                slave_compose_local,
                remote_path="{}/docker-compose.yml".format(slave.remote_data_dir))

        return out

    # ---- Single-host (degenerate N=1) --------------------------------------

    def render_combined(self, topology, local_logs_dir, supervisor):
        # type: (ClusterTopology, Path, SupervisorHost) -> ClusterRender
        """Render one combined storm.yaml + compose for the legacy single-host setup.

        ``supervisor`` provides the slot_ports for the combined supervisor.
        """
        out = ClusterRender()
        combined_dir = self.output_dir / "combined"
        combined_dir.mkdir(parents=True, exist_ok=True)

        yaml_path = combined_dir / "storm.yaml"
        yaml_path.write_text(_read_template(
            self.templates_dir, "storm.combined.yaml.tmpl").substitute(
                master_host=topology.master.hostname,
                zk_port=topology.master.zk_port,
                nimbus_thrift_port=topology.master.nimbus_thrift_port,
                ui_port=topology.master.ui_port,
                logviewer_port=topology.master.logviewer_port,
                topology_workers=len(supervisor.slot_ports),
                slot_ports_yaml=_slot_ports_yaml(supervisor.slot_ports),
        ))
        out.combined_storm_yaml = RenderedFile(yaml_path)

        compose_path = combined_dir / "docker-compose.yml"
        compose_path.write_text(_read_template(
            self.templates_dir, "docker-compose.combined.yml.tmpl").substitute(
                storm_image=self.storm_image,
                storm_version=self.storm_version,
                build_context=topology.master.container_project_dir,
                zookeeper_version=self.zookeeper_version,
                zk_port=topology.master.zk_port,
                local_conf_path=str(yaml_path.resolve()),
                local_logs_path=str(local_logs_dir.resolve()),
        ))
        out.combined_compose = RenderedFile(compose_path)
        return out
