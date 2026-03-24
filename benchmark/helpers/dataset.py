from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Existing helpers
# ---------------------------------------------------------------------------

def _get_components(df: pd.DataFrame) -> list[str]:
    return sorted(df["component"].unique())


def _get_ecall_names(ecalls: pd.DataFrame, component: str) -> list[str]:
    return sorted(ecalls.loc[ecalls["component"] == component, "name"].unique())


def get_lifecycle_events(df: pd.DataFrame) -> pd.DataFrame:
    """Extract lifecycle events from profiler data.  """
    lifecycle = df[df["type"] == "lifecycle"][["component", "taskId", "name", "total", "timestamp", "elapsed_s"]].copy()
    lifecycle = lifecycle.rename(columns={"name": "event", "total": "epoch"})
    return lifecycle


# ---------------------------------------------------------------------------
# Grid-search helpers
# ---------------------------------------------------------------------------

_BOOL_FIELDS = {
    "ground_truth", "all_started", "completed",
    "worker_oom_detected", "worker_fatal_detected",
}

_INT_FIELDS = {
    "run_id", "tick_interval_secs", "max_time_steps", "parallelism", "mu",
    "num_users", "num_keys", "seed",
    "submit_time_unix", "end_time_unix", "duration_secs",
    "startup_elapsed_secs", "startup_timeout_secs",
    "wait_used_secs", "max_wait_secs",
    "profiler_csvs_archived", "worker_logs_archived",
    "worker_error_count", "worker_oom_count",
}

_PARAM_COLS = [
    "tick_interval_secs", "max_time_steps", "parallelism", "mu",
    "num_users", "num_keys",
]

_LABEL_ABBREV = {
    "tick_interval_secs": "t",
    "max_time_steps": "e",
    "parallelism": "p",
    "mu": "mu",
    "num_users": "u",
    "num_keys": "k",
}

# Maps baseline ecall names to their confidential (encrypted) equivalents.
# ECALLs not listed here have the same name in both topologies.
_ECALL_MAPPING = {
    "snapshot": "getEncryptedSnapshot",
}

# Canonical name used for matched-pair columns
_ECALL_CANONICAL = {
    "snapshot": "snapshot",
    "getEncryptedSnapshot": "snapshot",
    "getEncryptedDummyPartial": "dummyPartial",
    "addContribution": "addContribution",
    "mergePartial": "mergePartial",
    "snapshot_lock_wait": "snapshot_lock_wait",
    "checkAndClamp": "checkAndClamp",
}


def parse_params(params_path: Path) -> dict[str, Any] | None:
    """Read a params.txt file (key=value lines) and return a typed dict."""
    if not params_path.is_file():
        return None
    try:
        result: dict[str, Any] = {}
        for line in params_path.read_text().strip().splitlines():
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if key in _BOOL_FIELDS:
                result[key] = value.lower() == "true"
            elif key in _INT_FIELDS:
                try:
                    result[key] = int(value)
                except ValueError:
                    result[key] = value
            else:
                result[key] = value
        return result if result else None
    except Exception:
        return None


def discover_runs(base_dir: Path) -> pd.DataFrame:
    """Discover all grid-search runs under base_dir/<timestamp>/<run_dir>/.

    Returns a DataFrame with one row per run containing all params.txt fields
    plus metadata columns: grid_timestamp, run_path, profiler_dir, has_profiler_data.
    """
    if not base_dir.is_dir():
        raise FileNotFoundError(f"Runs base directory not found: {base_dir}")

    records: list[dict[str, Any]] = []
    for ts_dir in sorted(base_dir.iterdir()):
        if not ts_dir.is_dir():
            continue
        for run_dir in sorted(ts_dir.iterdir()):
            if not run_dir.is_dir():
                continue
            params_path = run_dir / "params.txt"
            params = parse_params(params_path)
            if params is None:
                print(f"  WARNING: Skipping {run_dir.name} (no valid params.txt)")
                continue
            profiler_dir = run_dir / "profiler"
            has_profiler = profiler_dir.is_dir() and any(profiler_dir.glob("profiler-*.csv"))
            params["grid_timestamp"] = ts_dir.name
            params["run_path"] = run_dir
            params["profiler_dir"] = profiler_dir
            params["has_profiler_data"] = has_profiler
            records.append(params)

    if not records:
        raise ValueError(f"No valid runs found under {base_dir}")

    return pd.DataFrame(records)


def discover_all_runs(
    *dirs: Path,
    require_all: bool = False,
) -> pd.DataFrame:
    """Discover runs from multiple base directories and return a unified catalog.

    Each directory is scanned with ``discover_runs``.  The ``topology_type``
    column (already present in ``params.txt``) is used to distinguish
    baseline from confidential runs.

    Args:
        *dirs: One or more base directories containing grid-search runs.
        require_all: If True, raise if any directory is missing.  If False
            (default), silently skip missing directories.

    Returns:
        A single DataFrame combining all discovered runs.
    """
    frames: list[pd.DataFrame] = []
    for d in dirs:
        if not d.is_dir():
            if require_all:
                raise FileNotFoundError(f"Runs directory not found: {d}")
            print(f"  INFO: Skipping missing directory: {d}")
            continue
        try:
            frames.append(discover_runs(d))
        except ValueError as exc:
            if require_all:
                raise
            print(f"  INFO: {exc}")

    if not frames:
        raise ValueError("No valid runs found in any of the provided directories")

    catalog = pd.concat(frames, ignore_index=True)

    # Ensure topology_type column exists (fallback: mark as "unknown")
    if "topology_type" not in catalog.columns:
        catalog["topology_type"] = "unknown"

    return catalog


def detect_varying_params(catalog: pd.DataFrame) -> list[str]:
    """Return parameter columns that have more than one unique value."""
    varying = []
    for col in _PARAM_COLS:
        if col in catalog.columns and catalog[col].nunique(dropna=False) > 1:
            varying.append(col)
    return varying


def compact_number(val) -> str:
    """Format a number compactly: 1000000 -> '1M', 500000 -> '500k', 42 -> '42'."""
    if not isinstance(val, (int, float)) or pd.isna(val):
        return str(val)
    if abs(val) >= 1_000_000:
        return f"{val / 1_000_000:g}M"
    if abs(val) >= 1_000:
        return f"{val / 1_000:g}k"
    if isinstance(val, float) and val == int(val):
        return str(int(val))
    return str(val)


def short_label(row: pd.Series, vary_cols: list[str]) -> str:
    """Build a compact label from the columns that vary across runs."""
    parts = []
    for col in vary_cols:
        abbrev = _LABEL_ABBREV.get(col, col)
        val = row.get(col)
        if val is not None and not (isinstance(val, float) and pd.isna(val)):
            parts.append(f"{abbrev}{compact_number(val)}")
    return " ".join(parts) if parts else f"run{row.get('run_id', '?')}"


def load_profiler_csvs(csv_dir: Path) -> pd.DataFrame | None:
    """Load and concatenate all profiler-*.csv files from the given directory.

    Returns None if no valid CSV data is found (instead of exiting).
    """
    files = sorted(csv_dir.glob("profiler-*.csv"))
    if not files:
        return None

    frames = []
    for f in files:
        try:
            frame = pd.read_csv(f)
            frame["source_file"] = f.name
            frames.append(frame)
        except Exception as e:
            print(f"  WARNING: Failed to read {f.name}: {e}")

    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True)

    # Drop duplicate header rows
    stale_mask = combined["timestamp"] == "timestamp"
    if stale_mask.any():
        combined = combined[~stale_mask].reset_index(drop=True)

    combined["timestamp"] = pd.to_datetime(combined["timestamp"], format="ISO8601")
    t0 = combined["timestamp"].min()
    combined["elapsed_s"] = (combined["timestamp"] - t0).dt.total_seconds()
    combined["task_label"] = combined["component"] + " (task " + combined["taskId"].astype(str) + ")"
    return combined


def compute_run_summary(
    run_row: pd.Series,
    profiler_df: pd.DataFrame,
) -> dict[str, Any]:
    """Compute aggregated metrics from a single run's profiler data."""
    summary: dict[str, Any] = {}

    lifecycle = get_lifecycle_events(profiler_df)

    # --- Epoch duration ---
    epoch_adv = lifecycle[lifecycle["event"] == "EPOCH_ADVANCED"]
    if not epoch_adv.empty:
        durations = []
        for (comp, task), grp in epoch_adv.groupby(["component", "taskId"]):
            grp = grp.sort_values("epoch")
            diffs = grp["elapsed_s"].diff().dropna()
            durations.extend(diffs.tolist())
        summary["avg_epoch_duration_s"] = float(pd.Series(durations).mean()) if durations else float("nan")
    else:
        summary["avg_epoch_duration_s"] = float("nan")

    # --- Snapshot duration ---
    snap_start = lifecycle[lifecycle["event"] == "SNAPSHOT_STARTED"]
    snap_end = lifecycle[lifecycle["event"] == "SNAPSHOT_COMPLETED"]
    if not snap_start.empty and not snap_end.empty:
        merged = snap_start.merge(snap_end, on=["component", "taskId", "epoch"],
                                  suffixes=("_start", "_end"))
        if not merged.empty:
            merged["snap_dur"] = merged["elapsed_s_end"] - merged["elapsed_s_start"]
            summary["avg_snapshot_duration_s"] = float(merged["snap_dur"].mean())
            summary["total_snapshot_time_s"] = float(merged["snap_dur"].sum())
        else:
            summary["avg_snapshot_duration_s"] = float("nan")
            summary["total_snapshot_time_s"] = float("nan")
    else:
        summary["avg_snapshot_duration_s"] = float("nan")
        summary["total_snapshot_time_s"] = float("nan")

    # --- Max epoch completed ---
    if not epoch_adv.empty:
        summary["max_epoch_completed"] = int(epoch_adv["epoch"].max())
    else:
        summary["max_epoch_completed"] = 0

    # --- Active duration ---
    started = lifecycle[lifecycle["event"] == "COMPONENT_STARTED"]
    stopping = lifecycle[lifecycle["event"] == "COMPONENT_STOPPING"]
    max_epoch_ev = lifecycle[lifecycle["event"] == "MAX_EPOCH_REACHED"]
    if not started.empty:
        topology_ready = started["elapsed_s"].max()
        if not max_epoch_ev.empty:
            end_s = max_epoch_ev["elapsed_s"].max()
        elif not stopping.empty:
            end_s = stopping["elapsed_s"].min()
        else:
            end_s = profiler_df["elapsed_s"].max()
        summary["active_duration_s"] = float(end_s - topology_ready)
    else:
        # Baseline may lack COMPONENT_STARTED; fall back to full span
        summary["active_duration_s"] = float(profiler_df["elapsed_s"].max() - profiler_df["elapsed_s"].min())

    # --- Per-ECALL avg latency and total invocations ---
    ecalls = profiler_df[profiler_df["type"] == "ecall"]
    if not ecalls.empty:
        # Accumulate total time per canonical ecall across all components
        canonical_time_s: dict[str, float] = {}
        for (comp, name), grp in ecalls.groupby(["component", "name"]):
            # Shorten component name for column key
            short_comp = str(comp).replace("bolt-", "").replace("-", "_")
            short_name = str(name)
            col_key = f"ecall_avg_{short_comp}_{short_name}_ms"
            summary[col_key] = float(grp["avgMs"].mean())
            total_key = f"ecall_total_{short_comp}_{short_name}"
            total_count = int(grp["total"].max())
            summary[total_key] = total_count

            # Total wall-clock time for this ecall (avg_ms * count / 1000)
            avg_ms = float(grp["avgMs"].mean())
            ecall_time_s = avg_ms * total_count / 1000.0
            canon = _ECALL_CANONICAL.get(short_name, short_name)
            canonical_time_s[canon] = canonical_time_s.get(canon, 0.0) + ecall_time_s

        for canon, t in canonical_time_s.items():
            summary[f"ecall_time_{canon}_s"] = t

    # --- Counters: final (max) value per component/name, summed across tasks ---
    counters = profiler_df[profiler_df["type"] == "counter"]
    if not counters.empty:
        for (comp, name), grp in counters.groupby(["component", "name"]):
            short_comp = str(comp).replace("bolt-", "").replace("-", "_")
            # Each task's counter is monotonically increasing; take its max then sum across tasks
            per_task_max = grp.groupby("taskId")["total"].max()
            col_key = f"counter_{short_comp}_{name}"
            summary[col_key] = int(per_task_max.sum())

    # --- Gauges: mean value per component/name (averaged across tasks and time) ---
    gauges = profiler_df[profiler_df["type"] == "gauge"]
    if not gauges.empty:
        for (comp, name), grp in gauges.groupby(["component", "name"]):
            short_comp = str(comp).replace("bolt-", "").replace("-", "_")
            col_key = f"gauge_avg_{short_comp}_{name}"
            summary[col_key] = float(grp["total"].mean())

    return summary


def build_comparison_df(
    catalog: pd.DataFrame,
    *,
    collect_ecall_distributions: bool = False,
) -> tuple[pd.DataFrame, dict[int, pd.DataFrame] | None]:
    """Build a summary DataFrame across all runs in the catalog.

    If collect_ecall_distributions is True, also returns a dict mapping
    catalog index -> DataFrame of ecall rows (for box plot comparisons).
    """
    summaries: list[dict[str, Any]] = []
    ecall_dists: dict[int, pd.DataFrame] | None = {} if collect_ecall_distributions else None

    for idx, row in catalog.iterrows():
        profiler_dir = row["profiler_dir"]
        if not row.get("has_profiler_data", False):
            print(f"  Skipping run {row.get('run_id', '?')} (no profiler data)")
            continue

        profiler_df = load_profiler_csvs(profiler_dir)
        if profiler_df is None:
            print(f"  Skipping run {row.get('run_id', '?')} (failed to load CSVs)")
            continue

        summary = compute_run_summary(row, profiler_df)
        # Carry over all catalog columns
        for col in catalog.columns:
            if col not in summary:
                summary[col] = row[col]
        summary["_catalog_idx"] = idx
        summaries.append(summary)

        if ecall_dists is not None:
            ecall_rows = profiler_df[profiler_df["type"] == "ecall"][
                ["component", "name", "avgMs", "taskId"]
            ].copy()
            ecall_dists[idx] = ecall_rows

        # Free memory
        del profiler_df

    if not summaries:
        raise ValueError("No runs with valid profiler data found")

    result = pd.DataFrame(summaries)
    return result, ecall_dists


def build_matched_pairs(
    comparison_df: pd.DataFrame,
) -> pd.DataFrame:
    """Match baseline and confidential runs that share the same parameter tuple.

    For each matched pair, computes overhead ratios for key metrics.

    Returns a DataFrame with one row per matched parameter configuration,
    containing both baseline and confidential metric columns plus overhead ratios.
    """
    if "topology_type" not in comparison_df.columns:
        raise ValueError("comparison_df must contain a 'topology_type' column")

    base = comparison_df[comparison_df["topology_type"] == "baseline"].copy()
    conf = comparison_df[comparison_df["topology_type"] == "enclave"].copy()

    if base.empty or conf.empty:
        print("  WARNING: Cannot build matched pairs (need both baseline and enclave runs)")
        return pd.DataFrame()

    # Use only param columns that exist in both AND have overlapping values
    join_cols = []
    for c in _PARAM_COLS:
        if c in base.columns and c in conf.columns:
            shared = set(base[c].dropna().unique()) & set(conf[c].dropna().unique())
            if shared:
                join_cols.append(c)

    if not join_cols:
        print("  WARNING: No shared parameter values between baseline and enclave runs")
        return pd.DataFrame()

    # Filter to rows with overlapping parameter values only
    for c in join_cols:
        shared = set(base[c].dropna().unique()) & set(conf[c].dropna().unique())
        base = base[base[c].isin(shared)]
        conf = conf[conf[c].isin(shared)]

    if base.empty or conf.empty:
        print("  WARNING: After filtering to shared parameter values, no runs remain")
        return pd.DataFrame()

    # Aggregate: if multiple runs share the same params, take the mean
    metric_cols = [
        "duration_secs", "startup_elapsed_secs", "avg_epoch_duration_s",
        "avg_snapshot_duration_s", "active_duration_s", "total_snapshot_time_s",
        "max_epoch_completed",
    ]
    # Also include any ecall_avg_*, ecall_time_*, counter_*, gauge_avg_* columns
    ecall_avg_cols = [c for c in comparison_df.columns if c.startswith("ecall_avg_")]
    ecall_time_cols = [c for c in comparison_df.columns if c.startswith("ecall_time_")]
    counter_cols = [c for c in comparison_df.columns if c.startswith("counter_")]
    gauge_cols = [c for c in comparison_df.columns if c.startswith("gauge_avg_")]

    agg_cols = [c for c in metric_cols + ecall_avg_cols + ecall_time_cols + counter_cols + gauge_cols
                if c in comparison_df.columns]

    def _agg_group(df: pd.DataFrame) -> pd.Series:
        result = {}
        for c in agg_cols:
            if c in df.columns:
                vals = pd.to_numeric(df[c], errors="coerce")
                result[c] = vals.mean()
        result["n_runs"] = len(df)
        return pd.Series(result)

    base_agg = base.groupby(join_cols, dropna=False).apply(_agg_group, include_groups=False).reset_index()
    conf_agg = conf.groupby(join_cols, dropna=False).apply(_agg_group, include_groups=False).reset_index()

    matched = base_agg.merge(conf_agg, on=join_cols, suffixes=("_base", "_conf"))

    if matched.empty:
        print("  WARNING: No matching parameter configurations found between baseline and enclave runs")
        return matched

    # Compute overhead ratios
    for col in ["duration_secs", "startup_elapsed_secs", "avg_epoch_duration_s",
                "avg_snapshot_duration_s", "active_duration_s"]:
        bc = f"{col}_base"
        cc = f"{col}_conf"
        if bc in matched.columns and cc in matched.columns:
            with np.errstate(divide="ignore", invalid="ignore"):
                matched[f"{col}_ratio"] = matched[cc] / matched[bc]

    # Build a short label for each matched pair
    vary = detect_varying_params(matched)
    matched["label"] = matched.apply(lambda r: short_label(r, vary), axis=1)

    return matched


def canonical_ecall_name(name: str) -> str:
    """Map an ecall name to its canonical form for cross-topology comparison."""
    return _ECALL_CANONICAL.get(name, name)
