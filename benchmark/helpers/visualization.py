from __future__ import annotations

from pathlib import Path
from typing import Sequence

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import TwoSlopeNorm
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Core utilities
# ---------------------------------------------------------------------------

def save_or_show(fig, output_dir: Path | None, name: str, fmt: str, show: bool):
   if not fig.get_constrained_layout():
       fig.tight_layout()
   if output_dir:
       path = output_dir / f"{name}.{fmt}"
       fig.savefig(path, dpi=150, bbox_inches="tight")
       print(f"  Saved: {path}")
   if show:
       plt.show()
   plt.close(fig)

def format_elapsed_axis(ax):
    ax.set_xlabel("Elapsed time (s)")


_PALETTE = plt.rcParams["axes.prop_cycle"].by_key()["color"]

# Consistent baseline / confidential colours
COLOR_BASELINE = "#4472C4"     # blue
COLOR_CONFIDENTIAL = "#ED7D31"  # orange


def color_for(idx: int) -> str:
    return _PALETTE[idx % len(_PALETTE)]


def adaptive_figsize(
    n_items: int,
    per_item: float = 0.8,
    min_width: float = 8,
    max_width: float = 24,
    height: float = 5,
) -> tuple[float, float]:
    """Compute a figure width that scales with the number of items."""
    return (min(max_width, max(min_width, n_items * per_item)), height)


# Names for parameter columns (reused across several helpers)
_PARAM_DISPLAY = {
    "tick_interval_secs": "tick interval (s)",
    "max_time_steps": "max epochs",
    "parallelism": "parallelism",
    "mu": "mu (threshold)",
    "num_users": "num users",
    "num_keys": "num keys",
}


def _format_param_val(col: str, val) -> str:
    """Format a parameter value for display in labels."""
    if pd.isna(val):
        return "?"
    v = int(val) if isinstance(val, (int, float)) and float(val) == int(val) else val
    if isinstance(v, (int, float)) and v >= 1_000_000:
        return f"{v / 1_000_000:.0f}M"
    if isinstance(v, (int, float)) and v >= 1_000:
        return f"{v / 1_000:.0f}k"
    return str(v)


def _held_fixed_label(held_cols: list[str], held_vals: tuple) -> str:
    """Build a compact label describing which parameters are held fixed."""
    from .dataset import _LABEL_ABBREV
    parts = []
    for col, val in zip(held_cols, held_vals):
        abbrev = _LABEL_ABBREV.get(col, col)
        parts.append(f"{abbrev}={_format_param_val(col, val)}")
    return ", ".join(parts)


# ---------------------------------------------------------------------------
# Existing plot functions (updated)
# ---------------------------------------------------------------------------

def comparison_bar_chart(
    summary_df: pd.DataFrame,
    value_col: str,
    labels: list[str],
    ylabel: str,
    title: str,
    *,
    color_groups: list[int] | None = None,
    figsize: tuple[float, float] | None = None,
    annotate: bool = True,
    horizontal: bool | None = None,
) -> plt.Figure:
    """Bar chart comparing a metric across configurations.

    Automatically switches to horizontal bars when there are more than 20 items.
    """
    values = summary_df[value_col].values
    n = len(labels)

    if horizontal is None:
        horizontal = n > 20

    if figsize is None:
        if horizontal:
            figsize = (10, max(5, n * 0.35))
        else:
            figsize = adaptive_figsize(n)

    fig, ax = plt.subplots(figsize=figsize)
    x = np.arange(n)

    if color_groups is not None:
        colors = [color_for(g) for g in color_groups]
    else:
        colors = [color_for(0)] * n

    if horizontal:
        bars = ax.barh(x, values, color=colors, alpha=0.8)
        ax.set_yticks(x)
        ax.set_yticklabels(labels, fontsize=8)
        ax.set_xlabel(ylabel)
        ax.set_title(title)
        ax.invert_yaxis()
        if annotate:
            for bar, val in zip(bars, values):
                if pd.notna(val) and val != 0:
                    fmt = f"{val:.0f}" if abs(val) >= 10 else f"{val:.1f}"
                    ax.text(bar.get_width(), bar.get_y() + bar.get_height() / 2,
                            f" {fmt}", ha="left", va="center", fontsize=7)
    else:
        bars = ax.bar(x, values, color=colors, alpha=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        if annotate:
            for bar, val in zip(bars, values):
                if pd.notna(val) and val != 0:
                    fmt = f"{val:.0f}" if abs(val) >= 10 else f"{val:.1f}"
                    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                            fmt, ha="center", va="bottom", fontsize=7)

    ax.grid(True, axis="y" if not horizontal else "x", alpha=0.3)
    return fig


def parameter_breakdown(
    df: pd.DataFrame,
    value_col: str,
    ylabel: str,
    metric_title: str,
    vary_cols: Sequence[str],
    *,
    aggregate_held_fixed: bool = False,
    output_dir: Path | None = None,
    plot_name: str = "breakdown",
    fmt: str = "png",
    show: bool = True,
) -> None:
    """For each varying parameter, show how a metric changes when that parameter
    varies while the others are held fixed.

    When ``aggregate_held_fixed`` is True, collapses all held-fixed combinations
    into a single mean +/- std curve (one line per ``topology_type`` if present).
    """
    if len(vary_cols) < 2:
        return  # nothing to break down with only one varying param

    for sweep_col in vary_cols:
        held_cols = [c for c in vary_cols if c != sweep_col]
        sweep_display = _PARAM_DISPLAY.get(sweep_col, sweep_col)

        if aggregate_held_fixed:
            _parameter_breakdown_aggregated(
                df, sweep_col, value_col, ylabel, metric_title,
                sweep_display, output_dir, f"{plot_name}-by-{sweep_col}", fmt, show,
            )
            continue

        # Group by held-fixed columns
        groups = df.groupby(held_cols, dropna=False)
        n_groups = len(groups)
        if n_groups == 0:
            continue

        ncols = min(n_groups, 3)
        nrows = (n_groups + ncols - 1) // ncols
        fig, axes = plt.subplots(
            nrows, ncols,
            figsize=(5 * ncols, 4 * nrows),
            squeeze=False, sharey=True,
        )

        ax_idx = 0
        for held_vals, group in groups:
            if not isinstance(held_vals, tuple):
                held_vals = (held_vals,)
            row_i, col_i = divmod(ax_idx, ncols)
            ax = axes[row_i, col_i]

            group = group.sort_values(sweep_col)
            x_vals = group[sweep_col].values
            y_vals = group[value_col].values
            x_labels = [_format_param_val(sweep_col, v) for v in x_vals]
            x = np.arange(len(x_labels))

            bars = ax.bar(x, y_vals, color=[color_for(i) for i in range(len(x))], alpha=0.8)
            for bar, val in zip(bars, y_vals):
                if pd.notna(val) and val != 0:
                    txt = f"{val:.0f}" if abs(val) >= 10 else f"{val:.1f}"
                    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                            txt, ha="center", va="bottom", fontsize=7)

            ax.set_xticks(x)
            ax.set_xticklabels(x_labels, fontsize=9)
            ax.set_xlabel(sweep_display)
            subtitle = _held_fixed_label(held_cols, held_vals)
            ax.set_title(subtitle, fontsize=9)
            ax.grid(True, axis="y", alpha=0.3)
            if col_i == 0:
                ax.set_ylabel(ylabel)
            ax_idx += 1

        # Hide unused axes
        for i in range(ax_idx, nrows * ncols):
            r, c = divmod(i, ncols)
            axes[r, c].set_visible(False)

        fig.suptitle(
            f"{metric_title} — effect of {sweep_display}",
            fontsize=13, fontweight="bold",
        )
        save_or_show(fig, output_dir, f"{plot_name}-by-{sweep_col}", fmt, show)


def _parameter_breakdown_aggregated(
    df: pd.DataFrame,
    sweep_col: str,
    value_col: str,
    ylabel: str,
    metric_title: str,
    sweep_display: str,
    output_dir: Path | None,
    plot_name: str,
    fmt: str,
    show: bool,
) -> None:
    """Aggregated breakdown: one line per topology_type, mean +/- std."""
    has_types = "topology_type" in df.columns and df["topology_type"].nunique() > 1
    fig, ax = plt.subplots(figsize=(8, 5))

    if has_types:
        for ttype, colour in [("baseline", COLOR_BASELINE), ("confidential", COLOR_CONFIDENTIAL),
                               ("enclave", COLOR_CONFIDENTIAL)]:
            sub = df[df["topology_type"] == ttype]
            if sub.empty:
                continue
            grouped = sub.groupby(sweep_col)[value_col].agg(["mean", "std"]).sort_index()
            x = grouped.index.values
            y = grouped["mean"].values
            err = grouped["std"].fillna(0).values
            label = "baseline" if ttype == "baseline" else "confidential"
            ax.plot(x, y, marker="o", color=colour, label=label, linewidth=2)
            ax.fill_between(x, y - err, y + err, color=colour, alpha=0.15)
    else:
        grouped = df.groupby(sweep_col)[value_col].agg(["mean", "std"]).sort_index()
        x = grouped.index.values
        y = grouped["mean"].values
        err = grouped["std"].fillna(0).values
        ax.plot(x, y, marker="o", color=color_for(0), linewidth=2)
        ax.fill_between(x, y - err, y + err, color=color_for(0), alpha=0.15)

    ax.set_xlabel(sweep_display)
    ax.set_ylabel(ylabel)
    ax.set_title(f"{metric_title} — effect of {sweep_display}")
    ax.legend()
    ax.grid(True, alpha=0.3)
    save_or_show(fig, output_dir, plot_name, fmt, show)


# ---------------------------------------------------------------------------
# New plot functions — Overhead Analysis (Section A)
# ---------------------------------------------------------------------------

def paired_bar_chart(
    matched_df: pd.DataFrame,
    metric: str,
    *,
    ylabel: str = "",
    title: str = "",
    output_dir: Path | None = None,
    plot_name: str = "paired",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure:
    """Side-by-side grouped bars comparing baseline vs confidential.

    Expects ``matched_df`` from ``build_matched_pairs`` with columns
    ``{metric}_base`` and ``{metric}_conf``.
    """
    base_col = f"{metric}_base"
    conf_col = f"{metric}_conf"
    labels = matched_df["label"].tolist()
    n = len(labels)

    fig, ax = plt.subplots(figsize=adaptive_figsize(n, per_item=1.6))
    x = np.arange(n)
    w = 0.35

    base_vals = matched_df[base_col].values
    conf_vals = matched_df[conf_col].values

    ax.bar(x - w / 2, base_vals, w, label="Baseline", color=COLOR_BASELINE, alpha=0.85)
    ax.bar(x + w / 2, conf_vals, w, label="Confidential (SGX)", color=COLOR_CONFIDENTIAL, alpha=0.85)

    # Annotate
    for i in range(n):
        for val, offset in [(base_vals[i], -w / 2), (conf_vals[i], w / 2)]:
            if pd.notna(val) and val != 0:
                txt = f"{val:.0f}" if abs(val) >= 10 else f"{val:.1f}"
                ax.text(i + offset, val, txt, ha="center", va="bottom", fontsize=7)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()
    ax.grid(True, axis="y", alpha=0.3)
    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


def overhead_scatter(
    matched_df: pd.DataFrame,
    metric: str,
    *,
    hue_col: str | None = None,
    xlabel: str = "Baseline",
    ylabel: str = "Confidential (SGX)",
    title: str = "",
    output_dir: Path | None = None,
    plot_name: str = "overhead-scatter",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure:
    """Scatter plot of baseline vs confidential metric values with y=x reference."""
    base_col = f"{metric}_base"
    conf_col = f"{metric}_conf"

    fig, ax = plt.subplots(figsize=(7, 7))

    base_vals = pd.to_numeric(matched_df[base_col], errors="coerce")
    conf_vals = pd.to_numeric(matched_df[conf_col], errors="coerce")
    valid = base_vals.notna() & conf_vals.notna()

    if hue_col and hue_col in matched_df.columns:
        hue_vals = matched_df.loc[valid, hue_col]
        unique_hues = sorted(hue_vals.unique())
        for i, hv in enumerate(unique_hues):
            mask = hue_vals == hv
            ax.scatter(
                base_vals[valid][mask], conf_vals[valid][mask],
                label=f"{_format_param_val(hue_col, hv)}",
                color=color_for(i), s=60, alpha=0.8, edgecolors="white", linewidths=0.5,
            )
        ax.legend(title=_PARAM_DISPLAY.get(hue_col, hue_col), fontsize=8)
    else:
        ax.scatter(
            base_vals[valid], conf_vals[valid],
            color=COLOR_CONFIDENTIAL, s=60, alpha=0.8, edgecolors="white", linewidths=0.5,
        )

    # y = x reference line
    lo = min(base_vals[valid].min(), conf_vals[valid].min()) * 0.9
    hi = max(base_vals[valid].max(), conf_vals[valid].max()) * 1.1
    ax.plot([lo, hi], [lo, hi], "--", color="grey", linewidth=1, label="no overhead (y=x)")
    ax.set_xlim(lo, hi)
    ax.set_ylim(lo, hi)
    ax.set_aspect("equal", adjustable="box")

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


def duration_breakdown_stacked(
    matched_df: pd.DataFrame,
    *,
    title: str = "Time Breakdown: Baseline vs Confidential",
    output_dir: Path | None = None,
    plot_name: str = "duration-breakdown",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure:
    """Stacked horizontal bars showing time breakdown for each matched pair.

    Segments (in order): Startup, Barrier wait, Snapshot, addContribution, mergePartial,
    checkAndClamp, dummyPartial, snapshot_lock_wait, Other, Idle (post-max).
    """
    labels = matched_df["label"].tolist()
    n = len(labels)

    fig, ax = plt.subplots(figsize=(12, max(4, n * 0.8)))

    # Ecall segments to show (canonical names → display labels)
    _ECALL_SEGMENTS = [
        ("addContribution", "addContribution"),
        ("mergePartial", "mergePartial"),
        ("checkAndClamp", "checkAndClamp"),
        ("dummyPartial", "dummyPartial"),
        ("snapshot_lock_wait", "Lock wait"),
    ]

    # Colour palette: (baseline, confidential) per segment
    colors_map = {
        "Startup":          ("#A9CCE3", "#F5CBA7"),  # light blue / light orange
        "Barrier wait":     ("#D4E6F1", "#FDEBD0"),  # very light blue / very light orange
        "Snapshot":         ("#2E86C1", "#E67E22"),  # dark blue / dark orange
        "addContribution":  ("#27AE60", "#82E0AA"),  # green
        "mergePartial":     ("#8E44AD", "#D2B4DE"),  # purple
        "checkAndClamp":    ("#E74C3C", "#F1948A"),  # red
        "dummyPartial":     ("#F39C12", "#F9E79F"),  # yellow
        "Lock wait":        ("#95A5A6", "#D5DBDB"),  # grey
        "Other":            ("#85C1E9", "#F0B27A"),  # medium blue / medium orange
        "Idle (post-max)":  ("#BDC3C7", "#E5E7E9"),  # light grey
    }

    y_positions = []
    y_labels = []
    bar_height = 0.35

    for i, (_, row) in enumerate(matched_df.iterrows()):
        y_base = i * 2
        y_conf = i * 2 + 0.5

        for y_pos, suffix, ci in [(y_base, "_base", 0), (y_conf, "_conf", 1)]:
            startup = row.get(f"startup_elapsed_secs{suffix}", 0) or 0
            barrier_wait = row.get(f"barrier_wait_s{suffix}", 0) or 0
            snap_total = row.get(f"total_snapshot_time_s{suffix}", 0) or 0
            active = row.get(f"active_duration_s{suffix}", 0) or 0
            idle_post_max = row.get(f"idle_duration_s{suffix}", 0) or 0

            # Collect ecall time segments
            segments: list[tuple[str, float]] = [
                ("Startup", startup),
                ("Barrier wait", barrier_wait),
                ("Snapshot", snap_total),
            ]
            accounted = snap_total
            for canon, display in _ECALL_SEGMENTS:
                t = row.get(f"ecall_time_{canon}_s{suffix}", 0) or 0
                segments.append((display, t))
                accounted += t

            # Residual idle/other time within active processing
            idle_other = max(0, active - accounted)
            segments.append(("Other", idle_other))

            # Post-max-epoch idle time (waiting for topology kill)
            segments.append(("Idle (post-max)", idle_post_max))

            left = 0
            for seg_name, val in segments:
                if val <= 0:
                    continue
                c = colors_map[seg_name][ci]
                ax.barh(y_pos, val, height=bar_height, left=left, color=c, edgecolor="white", linewidth=0.5)
                left += val

        y_positions.extend([y_base, y_conf])
        y_labels.extend([f"{labels[i]} (base)", f"{labels[i]} (conf)"])

    ax.set_yticks(y_positions)
    ax.set_yticklabels(y_labels, fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel("Time (s)")
    ax.set_title(title)
    ax.grid(True, axis="x", alpha=0.3)

    # Legend — only include segments that appear in at least one row
    all_seg_names = ["Startup", "Barrier wait", "Snapshot"] + [d for _, d in _ECALL_SEGMENTS] + ["Other", "Idle (post-max)"]
    legend_patches = []
    for seg_name in all_seg_names:
        legend_patches.append(mpatches.Patch(color=colors_map[seg_name][0], label=f"{seg_name} (baseline)"))
        legend_patches.append(mpatches.Patch(color=colors_map[seg_name][1], label=f"{seg_name} (confidential)"))
    ax.legend(handles=legend_patches, fontsize=6, ncol=4, loc="upper right")

    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


def ecall_overhead_heatmap(
    matched_df: pd.DataFrame,
    ecall_pairs: list[tuple[str, str, str]],
    *,
    title: str = "ECALL Overhead Ratio (confidential / baseline)",
    output_dir: Path | None = None,
    plot_name: str = "ecall-overhead-heatmap",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure | None:
    """Heatmap of overhead ratios for matched ECALLs.

    Args:
        ecall_pairs: list of (display_name, base_col_suffix, conf_col_suffix) tuples
            e.g. [("snapshot", "ecall_avg_data_perturbation_snapshot_ms",
                    "ecall_avg_data_perturbation_getEncryptedSnapshot_ms")]
    """
    labels = matched_df["label"].tolist()
    n_ecalls = len(ecall_pairs)
    n_configs = len(labels)

    if n_ecalls == 0 or n_configs == 0:
        return None

    data = np.full((n_ecalls, n_configs), np.nan)
    ecall_labels = []

    for ei, (display_name, base_suffix, conf_suffix) in enumerate(ecall_pairs):
        ecall_labels.append(display_name)
        bc = f"{base_suffix}_base"
        cc = f"{conf_suffix}_conf"
        if bc in matched_df.columns and cc in matched_df.columns:
            b = pd.to_numeric(matched_df[bc], errors="coerce").values
            c = pd.to_numeric(matched_df[cc], errors="coerce").values
            with np.errstate(divide="ignore", invalid="ignore"):
                data[ei] = np.where((b > 0) & np.isfinite(b) & np.isfinite(c), c / b, np.nan)

    # Remove rows that are all NaN
    valid_rows = ~np.all(np.isnan(data), axis=1)
    data = data[valid_rows]
    ecall_labels = [l for l, v in zip(ecall_labels, valid_rows) if v]
    if data.size == 0:
        return None

    fig, ax = plt.subplots(figsize=(max(8, n_configs * 0.9), max(3, len(ecall_labels) * 0.8)))

    vmin = np.nanmin(data) if np.any(np.isfinite(data)) else 0.5
    vmax = np.nanmax(data) if np.any(np.isfinite(data)) else 2.0
    # TwoSlopeNorm requires vmin < vcenter < vmax
    vmin = min(max(vmin, 0.1), 0.99)
    vmax = max(vmax, 1.01)
    norm = TwoSlopeNorm(vmin=vmin, vcenter=1.0, vmax=vmax)

    im = ax.imshow(data, aspect="auto", cmap="RdYlGn_r", norm=norm)
    ax.set_xticks(range(n_configs))
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
    ax.set_yticks(range(len(ecall_labels)))
    ax.set_yticklabels(ecall_labels, fontsize=9)

    # Annotate cells
    if n_configs <= 20:
        for i in range(len(ecall_labels)):
            for j in range(n_configs):
                val = data[i, j]
                if np.isfinite(val):
                    ax.text(j, i, f"{val:.1f}x", ha="center", va="center", fontsize=8,
                            color="white" if val > 2 else "black")

    fig.colorbar(im, ax=ax, label="Overhead ratio", shrink=0.8)
    ax.set_title(title)
    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


def paired_box_plot(
    comparison_df: pd.DataFrame,
    metric: str,
    group_col: str,
    *,
    ylabel: str = "",
    title: str = "",
    output_dir: Path | None = None,
    plot_name: str = "paired-box",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure:
    """Paired box plots: baseline vs confidential, grouped by a parameter."""
    has_types = "topology_type" in comparison_df.columns
    groups = sorted(comparison_df[group_col].unique())
    n = len(groups)

    fig, ax = plt.subplots(figsize=adaptive_figsize(n, per_item=2.0))

    positions_base = []
    positions_conf = []
    data_base = []
    data_conf = []

    for i, g in enumerate(groups):
        sub = comparison_df[comparison_df[group_col] == g]
        pos = i * 3
        if has_types:
            b = sub.loc[sub["topology_type"] == "baseline", metric].dropna().values
            c = sub.loc[sub["topology_type"] == "enclave", metric].dropna().values
        else:
            b = sub[metric].dropna().values
            c = np.array([])

        if len(b) > 0:
            data_base.append(b)
            positions_base.append(pos)
        if len(c) > 0:
            data_conf.append(c)
            positions_conf.append(pos + 1)

    if data_base:
        bp_b = ax.boxplot(data_base, positions=positions_base, widths=0.7, patch_artist=True)
        for patch in bp_b["boxes"]:
            patch.set_facecolor(COLOR_BASELINE)
            patch.set_alpha(0.7)

    if data_conf:
        bp_c = ax.boxplot(data_conf, positions=positions_conf, widths=0.7, patch_artist=True)
        for patch in bp_c["boxes"]:
            patch.set_facecolor(COLOR_CONFIDENTIAL)
            patch.set_alpha(0.7)

    tick_positions = [i * 3 + 0.5 for i in range(n)]
    ax.set_xticks(tick_positions)
    ax.set_xticklabels([_format_param_val(group_col, g) for g in groups], fontsize=9)
    ax.set_xlabel(_PARAM_DISPLAY.get(group_col, group_col))
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, axis="y", alpha=0.3)

    legend_handles = [
        mpatches.Patch(facecolor=COLOR_BASELINE, alpha=0.7, label="Baseline"),
        mpatches.Patch(facecolor=COLOR_CONFIDENTIAL, alpha=0.7, label="Confidential (SGX)"),
    ]
    ax.legend(handles=legend_handles, fontsize=8)

    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


# ---------------------------------------------------------------------------
# New plot functions — Parameter Sensitivity (Section B)
# ---------------------------------------------------------------------------

def parameter_heatmap(
    df: pd.DataFrame,
    x_param: str,
    y_param: str,
    value_col: str,
    *,
    agg_func: str = "mean",
    title: str = "",
    cmap: str = "YlOrRd",
    topology_type: str | None = None,
    output_dir: Path | None = None,
    plot_name: str = "param-heatmap",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure | None:
    """2D heatmap showing a metric as a function of two parameters."""
    sub = df.copy()
    if topology_type and "topology_type" in sub.columns:
        sub = sub[sub["topology_type"] == topology_type]
    if sub.empty:
        return None

    pivot = sub.pivot_table(index=y_param, columns=x_param, values=value_col, aggfunc=agg_func)
    if pivot.empty:
        return None

    fig, ax = plt.subplots(figsize=(max(6, len(pivot.columns) * 1.2), max(4, len(pivot.index) * 0.8)))
    im = ax.imshow(pivot.values, aspect="auto", cmap=cmap)

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([_format_param_val(x_param, v) for v in pivot.columns], fontsize=9)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels([_format_param_val(y_param, v) for v in pivot.index], fontsize=9)
    ax.set_xlabel(_PARAM_DISPLAY.get(x_param, x_param))
    ax.set_ylabel(_PARAM_DISPLAY.get(y_param, y_param))

    # Annotate cells
    if pivot.size <= 50:
        for i in range(len(pivot.index)):
            for j in range(len(pivot.columns)):
                val = pivot.values[i, j]
                if np.isfinite(val):
                    txt = f"{val:.0f}" if abs(val) >= 10 else f"{val:.1f}"
                    ax.text(j, i, txt, ha="center", va="center", fontsize=8)

    fig.colorbar(im, ax=ax, shrink=0.8)
    ax.set_title(title)
    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


def parameter_heatmap_faceted(
    df: pd.DataFrame,
    x_param: str,
    y_param: str,
    value_col: str,
    *,
    agg_func: str = "mean",
    title: str = "",
    cmap: str = "YlOrRd",
    output_dir: Path | None = None,
    plot_name: str = "param-heatmap-faceted",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure | None:
    """Side-by-side heatmaps for baseline and confidential."""
    has_types = "topology_type" in df.columns and df["topology_type"].nunique() > 1
    if not has_types:
        return parameter_heatmap(df, x_param, y_param, value_col,
                                 agg_func=agg_func, title=title, cmap=cmap,
                                 output_dir=output_dir, plot_name=plot_name, fmt=fmt, show=show)

    types = [("baseline", "Baseline"), ("enclave", "Confidential (SGX)")]
    pivots = {}
    for ttype, _ in types:
        sub = df[df["topology_type"] == ttype]
        if not sub.empty:
            pivots[ttype] = sub.pivot_table(index=y_param, columns=x_param, values=value_col, aggfunc=agg_func)

    if not pivots:
        return None

    n_panels = len(pivots)
    fig, axes = plt.subplots(1, n_panels, figsize=(7 * n_panels, 5),
                             squeeze=False, constrained_layout=True)

    # Shared color scale
    all_vals = np.concatenate([p.values.flatten() for p in pivots.values()])
    all_vals = all_vals[np.isfinite(all_vals)]
    vmin, vmax = (all_vals.min(), all_vals.max()) if len(all_vals) > 0 else (0, 1)

    im = None
    for pi, (ttype, display) in enumerate(types):
        if ttype not in pivots:
            continue
        pivot = pivots[ttype]
        ax = axes[0, pi]
        im = ax.imshow(pivot.values, aspect="auto", cmap=cmap, vmin=vmin, vmax=vmax)

        ax.set_xticks(range(len(pivot.columns)))
        ax.set_xticklabels([_format_param_val(x_param, v) for v in pivot.columns], fontsize=9)
        ax.set_yticks(range(len(pivot.index)))
        ax.set_yticklabels([_format_param_val(y_param, v) for v in pivot.index], fontsize=9)
        ax.set_xlabel(_PARAM_DISPLAY.get(x_param, x_param))
        if pi == 0:
            ax.set_ylabel(_PARAM_DISPLAY.get(y_param, y_param))
        ax.set_title(display)

        if pivot.size <= 50:
            for i in range(len(pivot.index)):
                for j in range(len(pivot.columns)):
                    val = pivot.values[i, j]
                    if np.isfinite(val):
                        txt = f"{val:.0f}" if abs(val) >= 10 else f"{val:.1f}"
                        ax.text(j, i, txt, ha="center", va="center", fontsize=8)

    if im is not None:
        fig.colorbar(im, ax=axes.ravel().tolist(), shrink=0.8)
    fig.suptitle(title, fontsize=13, fontweight="bold")
    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


def sensitivity_line(
    df: pd.DataFrame,
    sweep_col: str,
    value_col: str,
    *,
    ylabel: str = "",
    title: str = "",
    output_dir: Path | None = None,
    plot_name: str = "sensitivity",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure:
    """Line plot with confidence bands showing how a metric scales with a parameter.

    One line per topology_type (if present).
    """
    fig, ax = plt.subplots(figsize=(8, 5))
    has_types = "topology_type" in df.columns and df["topology_type"].nunique() > 1

    if has_types:
        for ttype, colour, label in [("baseline", COLOR_BASELINE, "Baseline"),
                                       ("enclave", COLOR_CONFIDENTIAL, "Confidential (SGX)")]:
            sub = df[df["topology_type"] == ttype]
            if sub.empty:
                continue
            grouped = sub.groupby(sweep_col)[value_col].agg(["mean", "std", "count"]).sort_index()
            x = grouped.index.values
            y = grouped["mean"].values
            err = grouped["std"].fillna(0).values
            ax.plot(x, y, marker="o", color=colour, label=label, linewidth=2)
            ax.fill_between(x, y - err, y + err, color=colour, alpha=0.15)
    else:
        grouped = df.groupby(sweep_col)[value_col].agg(["mean", "std", "count"]).sort_index()
        x = grouped.index.values
        y = grouped["mean"].values
        err = grouped["std"].fillna(0).values
        ax.plot(x, y, marker="o", color=color_for(0), linewidth=2)
        ax.fill_between(x, y - err, y + err, color=color_for(0), alpha=0.15)

    ax.set_xlabel(_PARAM_DISPLAY.get(sweep_col, sweep_col))
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3)
    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


# ---------------------------------------------------------------------------
# New plot functions — Grid Overview (Section C)
# ---------------------------------------------------------------------------

def completion_heatmap(
    catalog: pd.DataFrame,
    x_param: str,
    y_param: str,
    *,
    topology_type: str | None = None,
    title: str = "Run Completion Status",
    output_dir: Path | None = None,
    plot_name: str = "completion-heatmap",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure | None:
    """Heatmap showing completion rate (proportion completed) for a 2D parameter grid."""
    sub = catalog.copy()
    if topology_type and "topology_type" in sub.columns:
        sub = sub[sub["topology_type"] == topology_type]

    if "status" not in sub.columns:
        return None

    sub["_completed"] = (sub["status"] == "Completed").astype(float)
    pivot = sub.pivot_table(index=y_param, columns=x_param, values="_completed", aggfunc="mean")

    if pivot.empty:
        return None

    fig, ax = plt.subplots(figsize=(max(6, len(pivot.columns) * 1.2), max(4, len(pivot.index) * 0.8)))
    im = ax.imshow(pivot.values, aspect="auto", cmap="RdYlGn", vmin=0, vmax=1)

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([_format_param_val(x_param, v) for v in pivot.columns], fontsize=9)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels([_format_param_val(y_param, v) for v in pivot.index], fontsize=9)
    ax.set_xlabel(_PARAM_DISPLAY.get(x_param, x_param))
    ax.set_ylabel(_PARAM_DISPLAY.get(y_param, y_param))

    # Annotate with actual counts
    count_pivot = sub.pivot_table(index=y_param, columns=x_param, values="_completed", aggfunc="count")
    complete_pivot = sub.pivot_table(index=y_param, columns=x_param, values="_completed", aggfunc="sum")

    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            total = count_pivot.values[i, j] if np.isfinite(count_pivot.values[i, j]) else 0
            done = complete_pivot.values[i, j] if np.isfinite(complete_pivot.values[i, j]) else 0
            if total > 0:
                ax.text(j, i, f"{int(done)}/{int(total)}", ha="center", va="center", fontsize=8)

    fig.colorbar(im, ax=ax, label="Completion rate", shrink=0.8)
    ax.set_title(title)
    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


def status_distribution_plot(
    comparison_df: pd.DataFrame,
    *,
    title: str = "Duration Distribution by Run Status",
    output_dir: Path | None = None,
    plot_name: str = "status-distribution",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure:
    """Violin + strip plot showing duration distribution per status category."""
    fig, ax = plt.subplots(figsize=(8, 5))

    statuses = sorted(comparison_df["status"].unique())
    status_colors = {"Completed": "#27AE60", "Timed out": "#F39C12", "OOM": "#E74C3C"}

    data_groups = []
    positions = []
    for i, s in enumerate(statuses):
        vals = comparison_df.loc[comparison_df["status"] == s, "duration_secs"].dropna().values
        if len(vals) > 0:
            data_groups.append(vals)
            positions.append(i)

    if data_groups:
        parts = ax.violinplot(data_groups, positions=positions, showmeans=True, showmedians=True)
        for i, pc in enumerate(parts["bodies"]):
            s = statuses[positions[i]] if i < len(positions) else "Completed"
            pc.set_facecolor(status_colors.get(s, color_for(i)))
            pc.set_alpha(0.4)

        # Overlay individual points
        for i, (vals, pos) in enumerate(zip(data_groups, positions)):
            jitter = np.random.default_rng(42).uniform(-0.1, 0.1, size=len(vals))
            s = statuses[pos]
            ax.scatter(pos + jitter, vals, s=15, alpha=0.6,
                       color=status_colors.get(s, color_for(i)), edgecolors="white", linewidths=0.3)

    ax.set_xticks(range(len(statuses)))
    ax.set_xticklabels(statuses, fontsize=10)
    ax.set_ylabel("Duration (s)")
    ax.set_title(title)
    ax.grid(True, axis="y", alpha=0.3)
    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


def styled_summary_table(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    status_col: str = "status",
) -> "pd.io.formats.style.Styler | pd.DataFrame":
    """Return a pandas Styler with conditional formatting for Jupyter display."""
    if columns:
        display_df = df[[c for c in columns if c in df.columns]].copy()
    else:
        display_df = df.copy()

    # Format numeric columns
    format_dict = {}
    for col in display_df.columns:
        if display_df[col].dtype in ("float64", "float32"):
            if "ratio" in col:
                format_dict[col] = "{:.2f}x"
            elif "_ms" in col or "Ms" in col:
                format_dict[col] = "{:.1f}"
            elif "_s" in col or "secs" in col or "duration" in col:
                format_dict[col] = "{:.0f}"

    try:
        styler = display_df.style
    except AttributeError:
        # jinja2 not installed — fall back to plain DataFrame
        return display_df

    if format_dict:
        styler = styler.format(format_dict, na_rep="—")

    # Color-code status column
    if status_col in display_df.columns:
        def _status_color(val):
            if val == "Completed":
                return "background-color: #C6EFCE"
            elif val == "OOM":
                return "background-color: #FFC7CE"
            elif val == "Timed out":
                return "background-color: #FFEB9C"
            return ""
        styler = styler.map(_status_color, subset=[status_col])

    # Alternating row backgrounds
    styler = styler.set_table_styles([
        {"selector": "th", "props": [("background-color", "#4472C4"), ("color", "white"),
                                      ("font-weight", "bold"), ("font-size", "11px")]},
        {"selector": "td", "props": [("font-size", "10px")]},
    ])

    return styler


# ---------------------------------------------------------------------------
# New plot functions — Single Run Deep-Dive (Section D)
# ---------------------------------------------------------------------------

def run_timeline_gantt(
    profiler_df: pd.DataFrame,
    lifecycle: pd.DataFrame,
    *,
    title: str = "Run Timeline",
    output_dir: Path | None = None,
    plot_name: str = "timeline-gantt",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure:
    """Gantt-style timeline showing task phases from lifecycle events."""
    tasks = sorted(profiler_df["task_label"].unique())
    n_tasks = len(tasks)

    fig, ax = plt.subplots(figsize=(14, max(3, n_tasks * 0.5)))

    phase_colors = {
        "startup": "#A9CCE3",
        "barrier_wait": "#D4E6F1",
        "active": "#2ECC71",
        "snapshot": "#E74C3C",
        "idle_post_max": "#BDC3C7",
        "shutdown": "#95A5A6",
    }

    task_to_y = {t: i for i, t in enumerate(tasks)}

    started = lifecycle[lifecycle["event"] == "COMPONENT_STARTED"]
    barrier = lifecycle[lifecycle["event"] == "BARRIER_RELEASED"]
    stopping = lifecycle[lifecycle["event"] == "COMPONENT_STOPPING"]
    max_epoch = lifecycle[lifecycle["event"] == "MAX_EPOCH_REACHED"]
    snap_start = lifecycle[lifecycle["event"] == "SNAPSHOT_STARTED"]
    snap_end = lifecycle[lifecycle["event"] == "SNAPSHOT_COMPLETED"]

    t_min = profiler_df["elapsed_s"].min()
    t_max = profiler_df["elapsed_s"].max()

    for task_label in tasks:
        y = task_to_y[task_label]
        parts = task_label.split(" (task ")
        comp = parts[0] if parts else task_label
        task_id_str = parts[1].rstrip(")") if len(parts) > 1 else ""

        def _task_event(ev_df: pd.DataFrame) -> pd.DataFrame:
            return ev_df[(ev_df["component"] == comp) & (ev_df["taskId"].astype(str) == task_id_str)]

        task_started = _task_event(started)
        task_barrier = _task_event(barrier)
        task_stopping = _task_event(stopping)
        task_max_epoch = _task_event(max_epoch)

        start_t = task_started["elapsed_s"].min() if not task_started.empty else t_min
        # Processing starts at barrier release if available, else at component started
        processing_t = task_barrier["elapsed_s"].min() if not task_barrier.empty else start_t
        max_epoch_t = task_max_epoch["elapsed_s"].max() if not task_max_epoch.empty else None
        stop_t = task_stopping["elapsed_s"].min() if not task_stopping.empty else t_max

        # Startup phase: from t_min to COMPONENT_STARTED
        ax.barh(y, start_t - t_min, left=t_min, height=0.6, color=phase_colors["startup"], edgecolor="white")
        # Barrier wait phase: from COMPONENT_STARTED to BARRIER_RELEASED (if applicable)
        if processing_t > start_t:
            ax.barh(y, processing_t - start_t, left=start_t, height=0.6,
                    color=phase_colors["barrier_wait"], edgecolor="white")
        # Active phase: from processing_t to MAX_EPOCH_REACHED (or stop_t)
        active_end = max_epoch_t if max_epoch_t is not None else stop_t
        ax.barh(y, active_end - processing_t, left=processing_t, height=0.6,
                color=phase_colors["active"], edgecolor="white", alpha=0.5)
        # Idle (post-max) phase: from MAX_EPOCH_REACHED to COMPONENT_STOPPING
        if max_epoch_t is not None and max_epoch_t < stop_t:
            ax.barh(y, stop_t - max_epoch_t, left=max_epoch_t, height=0.6,
                    color=phase_colors["idle_post_max"], edgecolor="white")
        # Shutdown phase: from COMPONENT_STOPPING to t_max
        ax.barh(y, t_max - stop_t, left=stop_t, height=0.6, color=phase_colors["shutdown"], edgecolor="white")

        # Overlay snapshot blocks
        task_snaps = _task_event(snap_start)
        task_snap_ends = _task_event(snap_end)
        if not task_snaps.empty and not task_snap_ends.empty:
            merged = task_snaps.merge(task_snap_ends, on=["component", "taskId", "epoch"],
                                      suffixes=("_s", "_e"))
            for _, row in merged.iterrows():
                dur = row["elapsed_s_e"] - row["elapsed_s_s"]
                ax.barh(y, dur, left=row["elapsed_s_s"], height=0.6,
                        color=phase_colors["snapshot"], edgecolor="none", alpha=0.8)

    ax.set_yticks(range(n_tasks))
    ax.set_yticklabels(tasks, fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel("Elapsed time (s)")
    ax.set_title(title)
    ax.grid(True, axis="x", alpha=0.3)

    phase_labels = {
        "startup": "Startup",
        "barrier_wait": "Barrier wait",
        "active": "Active",
        "snapshot": "Snapshot",
        "idle_post_max": "Idle (post-max)",
        "shutdown": "Shutdown",
    }
    legend_patches = [mpatches.Patch(color=c, label=phase_labels[n]) for n, c in phase_colors.items()]
    ax.legend(handles=legend_patches, fontsize=8, loc="upper right")

    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


def emission_timeline(
    profiler_df: pd.DataFrame,
    lifecycle: pd.DataFrame,
    *,
    title: str = "Emission Timeline",
    output_dir: Path | None = None,
    plot_name: str = "emission-timeline",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure | None:
    """Timeline of real vs dummy emissions from each data-perturbation task.

    Top panel: stacked step chart of cumulative real + dummy emissions (summed
    across tasks).  Bottom panel: per-interval emission rate (delta) for each
    task, with real (green, upward) and dummy (red, downward) bars.

    Dotted vertical lines mark ideal tick boundaries; solid grey lines mark
    epoch advances.
    """
    COMP = "bolt-data-perturbation"

    # --- resolve tick interval and topology start ---
    tick_ev = lifecycle[
        (lifecycle["event"] == "TICK_INTERVAL_SECS") & (lifecycle["component"] == COMP)
    ]
    if tick_ev.empty:
        print("Skipped emission timeline: no TICK_INTERVAL_SECS event")
        return None
    tick_s = float(tick_ev["epoch"].iloc[0])

    started = lifecycle[
        (lifecycle["event"] == "COMPONENT_STARTED") & (lifecycle["component"] == COMP)
    ]
    t_start = float(started["elapsed_s"].min()) if not started.empty else 0.0

    # --- per-task counter time series ---
    counters = profiler_df[
        (profiler_df["component"] == COMP) & (profiler_df["type"] == "counter")
    ].copy()
    if counters.empty:
        print("Skipped emission timeline: no counter data for data-perturbation")
        return None

    tasks = sorted(counters["taskId"].unique())
    n_tasks = len(tasks)

    # --- epoch advance markers (any task, deduplicated by epoch) ---
    epoch_ev = lifecycle[
        (lifecycle["event"] == "EPOCH_ADVANCED") & (lifecycle["component"] == COMP)
    ]
    epoch_times = epoch_ev.groupby("epoch")["elapsed_s"].min().sort_index()

    # --- precompute per-task deltas ---
    task_series: dict[int, dict[str, pd.DataFrame]] = {}
    global_max_delta = 1.0
    for task_id in tasks:
        task_series[task_id] = {}
        task_data = counters[counters["taskId"] == task_id].sort_values("elapsed_s")
        for name in ["real_emissions", "dummy_emissions"]:
            s = task_data[task_data["name"] == name][["elapsed_s", "total"]].copy()
            if s.empty:
                continue
            s = s.sort_values("elapsed_s").reset_index(drop=True)
            s["delta"] = s["total"].diff().fillna(s["total"])
            task_series[task_id][name] = s
            mx = s["delta"].max()
            if mx > global_max_delta:
                global_max_delta = mx

    t_max = float(profiler_df["elapsed_s"].max())

    # --- helper to draw tick / epoch grid on an axes ---
    def _draw_grid(ax):
        t = t_start + tick_s
        while t <= t_max:
            ax.axvline(t, color="#CCCCCC", linestyle=":", linewidth=0.5)
            t += tick_s
        for epoch_num, et in epoch_times.items():
            ax.axvline(et, color="#888888", linestyle="-", linewidth=0.8, alpha=0.5)

    # --- figure: two panels ---
    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1, figsize=(14, max(5, 2 + n_tasks * 1.0)),
        height_ratios=[1, max(1, n_tasks * 0.5)],
        sharex=True, constrained_layout=True,
    )

    # ---- Top panel: cumulative emissions (summed across tasks) ----
    _draw_grid(ax_top)
    # Collect all unique timestamps, aggregate across tasks
    _real_parts = [ts["real_emissions"][["elapsed_s", "total"]] for ts in task_series.values()
                   if "real_emissions" in ts]
    all_real = pd.concat(_real_parts, ignore_index=True) if _real_parts else pd.DataFrame()
    _dummy_parts = [ts["dummy_emissions"][["elapsed_s", "total"]] for ts in task_series.values()
                    if "dummy_emissions" in ts]
    all_dummy = pd.concat(_dummy_parts, ignore_index=True) if _dummy_parts else pd.DataFrame()
    if not all_real.empty:
        cum_real = all_real.groupby("elapsed_s")["total"].sum().sort_index()
        ax_top.step(cum_real.index, cum_real.values, where="post",
                    color="#27AE60", linewidth=1.5, label="Real (cumulative)")
    if not all_dummy.empty:
        cum_dummy = all_dummy.groupby("elapsed_s")["total"].sum().sort_index()
        ax_top.step(cum_dummy.index, cum_dummy.values, where="post",
                    color="#E74C3C", linewidth=1.5, label="Dummy (cumulative)")
    elif not all_real.empty:
        # No dummy data — show a zero line so the absence is explicit
        ax_top.step(cum_real.index, np.zeros(len(cum_real)), where="post",
                    color="#E74C3C", linewidth=1.5, label="Dummy (cumulative)")
    ax_top.set_ylabel("Cumulative emissions\n(all tasks)")
    ax_top.set_title(title)
    ax_top.legend(fontsize=8, loc="upper left")
    ax_top.grid(True, axis="y", alpha=0.2)

    # ---- Bottom panel: per-task cumulative counters (one real + one dummy line per task) ----
    _draw_grid(ax_bot)
    cmap = plt.get_cmap("tab10")
    for i, task_id in enumerate(tasks):
        color = cmap(i % 10)
        # Real emissions
        s_real = task_series[task_id].get("real_emissions")
        if s_real is not None and not s_real.empty:
            ax_bot.step(s_real["elapsed_s"].values, s_real["total"].values, where="post",
                        color=color, linewidth=1.3, label=f"Task {task_id} real")
        # Dummy emissions — plot zeros along real timeline if no dummy data
        s_dummy = task_series[task_id].get("dummy_emissions")
        if s_dummy is not None and not s_dummy.empty:
            ax_bot.step(s_dummy["elapsed_s"].values, s_dummy["total"].values, where="post",
                        color=color, linewidth=1.0, linestyle="--", alpha=0.6,
                        label=f"Task {task_id} dummy")
        elif s_real is not None and not s_real.empty:
            ax_bot.step(s_real["elapsed_s"].values, np.zeros(len(s_real)), where="post",
                        color=color, linewidth=1.0, linestyle="--", alpha=0.6,
                        label=f"Task {task_id} dummy")

    ax_bot.set_xlabel("Elapsed time (s)")
    ax_bot.set_ylabel("Cumulative emissions\n(per task)")
    ax_bot.grid(True, axis="y", alpha=0.2)
    ax_bot.legend(fontsize=7, ncol=min(n_tasks * 2, 4), loc="upper left")

    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


def enriched_timeline(
    profiler_df: pd.DataFrame,
    lifecycle: pd.DataFrame,
    *,
    title: str = "Enriched Timeline",
    output_dir: Path | None = None,
    plot_name: str = "enriched-timeline",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure | None:
    """Per-task swim-lane timeline showing key DP lifecycle events.

    For each data-perturbation task, draws:
    - Snapshot blocks (red bars from SNAPSHOT_STARTED to SNAPSHOT_COMPLETED)
    - TICK_RECEIVED markers (grey triangles)
    - EPOCH_ADVANCED markers (green diamonds — real partial emitted)
    - DUMMY_RELEASED markers (orange circles — dummy partial emitted)

    The x-axis is elapsed time. Ideal tick-interval grid lines are overlaid.
    """
    COMP = "bolt-data-perturbation"

    dp_lc = lifecycle[lifecycle["component"] == COMP].copy()
    if dp_lc.empty:
        print("Skipped enriched timeline: no data-perturbation lifecycle events")
        return None

    tasks = sorted(dp_lc["taskId"].unique())
    n_tasks = len(tasks)

    # Tick interval
    tick_ev = dp_lc[dp_lc["event"] == "TICK_INTERVAL_SECS"]
    tick_s = float(tick_ev["epoch"].iloc[0]) if not tick_ev.empty else None

    # Time range: from barrier release (or component start) to max epoch reached
    barrier = dp_lc[dp_lc["event"] == "BARRIER_RELEASED"]
    t_start = float(barrier["elapsed_s"].min()) if not barrier.empty else 0.0
    max_ep = dp_lc[dp_lc["event"] == "MAX_EPOCH_REACHED"]
    t_end = float(max_ep["elapsed_s"].max()) if not max_ep.empty else float(dp_lc["elapsed_s"].max())
    # add a small margin
    t_end = t_end + (t_end - t_start) * 0.02

    fig, ax = plt.subplots(figsize=(16, max(3, n_tasks * 1.2)))

    task_to_y = {t: i for i, t in enumerate(tasks)}

    # Snapshot blocks
    snap_s = dp_lc[dp_lc["event"] == "SNAPSHOT_STARTED"]
    snap_e = dp_lc[dp_lc["event"] == "SNAPSHOT_COMPLETED"]
    if not snap_s.empty and not snap_e.empty:
        merged = snap_s.merge(
            snap_e, on=["component", "taskId", "epoch"], suffixes=("_s", "_e"),
        )
        for _, row in merged.iterrows():
            y = task_to_y.get(row["taskId"])
            if y is None:
                continue
            dur = row["elapsed_s_e"] - row["elapsed_s_s"]
            ax.barh(y, dur, left=row["elapsed_s_s"], height=0.5,
                    color="#E74C3C", alpha=0.35, edgecolor="none")

    # Marker styles per event
    _event_style = {
        "TICK_RECEIVED":   dict(marker="v", color="#555555", s=40, zorder=3, alpha=0.85),
        "EPOCH_ADVANCED":  dict(marker="D", color="#27AE60", s=40, zorder=5, alpha=0.9),
        "DUMMY_RELEASED":  dict(marker="o", color="#E67E22", s=30, zorder=4, alpha=0.8),
    }

    for event_name, style in _event_style.items():
        ev = dp_lc[dp_lc["event"] == event_name]
        if ev.empty:
            continue
        for task_id in tasks:
            task_ev = ev[ev["taskId"] == task_id]
            if task_ev.empty:
                continue
            y = task_to_y[task_id]
            ax.scatter(
                task_ev["elapsed_s"].values,
                [y] * len(task_ev),
                **style,
                label=event_name if task_id == tasks[0] else None,
            )

    # Tick grid
    if tick_s and tick_s > 0:
        t = t_start + tick_s
        while t <= t_end:
            ax.axvline(t, color="#CCCCCC", linestyle=":", linewidth=0.5, label="Ideal tick boundary")
            t += tick_s

    ax.set_yticks(range(n_tasks))
    ax.set_yticklabels([f"Task {t}" for t in tasks], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlim(t_start - 2, t_end)
    ax.set_xlabel("Elapsed time (s)")
    ax.set_title(title)
    # ax.grid(True, axis="x", alpha=0.15)
    ax.grid(False)

    # De-duplicate legend entries
    handles, labels = ax.get_legend_handles_labels()
    seen: dict[str, int] = {}
    unique_h, unique_l = [], []
    for h, l in zip(handles, labels):
        if l not in seen:
            seen[l] = 1
            unique_h.append(h)
            unique_l.append(l)
    # Add snapshot patch
    unique_h.append(mpatches.Patch(color="#E74C3C", alpha=0.35, label="Snapshot"))
    unique_l.append("Snapshot")

    ax.legend(unique_h, unique_l, fontsize=8, loc="upper right")

    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


def tick_tuple_validation(
    profiler_df: pd.DataFrame,
    lifecycle: pd.DataFrame,
    *,
    title: str = "Tick Tuple Validation",
    output_dir: Path | None = None,
    plot_name: str = "tick-tuple-validation",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure | None:
    """Analyze inter-tick-tuple timing irregularities per task over time.

    Top panel: scatter of per-task inter-tick deltas over elapsed time, with
    the expected tick interval shown as a horizontal band (±20%).
    Bottom panel: rolling standard deviation of deltas (window=5) to show
    whether irregularities diminish over time.
    """
    COMP = "bolt-data-perturbation"

    dp_lc = lifecycle[lifecycle["component"] == COMP].copy()
    ticks = dp_lc[dp_lc["event"] == "TICK_RECEIVED"].copy()
    if ticks.empty:
        print("Skipped tick tuple validation: no TICK_RECEIVED events found")
        return None

    # Expected tick interval
    tick_ev = dp_lc[dp_lc["event"] == "TICK_INTERVAL_SECS"]
    tick_s = float(tick_ev["epoch"].iloc[0]) if not tick_ev.empty else None
    if tick_s is None or tick_s <= 0:
        print("Skipped tick tuple validation: could not determine tick interval")
        return None

    tasks = sorted(ticks["taskId"].unique())
    n_tasks = len(tasks)
    colors = plt.cm.tab10(np.linspace(0, 1, max(n_tasks, 1)))

    fig, (ax_delta, ax_roll) = plt.subplots(
        2, 1, figsize=(14, 7), sharex=True,
        gridspec_kw={"height_ratios": [3, 2], "hspace": 0.08},
        layout="constrained",
    )

    # Tolerance band
    tol = 0.20
    lo, hi = tick_s * (1 - tol), tick_s * (1 + tol)

    all_violations = 0
    all_deltas = 0

    for idx, task_id in enumerate(tasks):
        t = ticks[ticks["taskId"] == task_id].sort_values("elapsed_s")
        if len(t) < 2:
            continue
        elapsed = t["elapsed_s"].values
        deltas = np.diff(elapsed)
        midpoints = elapsed[1:]  # x-position = time of the receiving tick

        c = colors[idx]
        label = f"Task {task_id}"

        # --- Top: delta scatter ---
        ax_delta.scatter(midpoints, deltas, s=28, color=c, alpha=0.8,
                         edgecolors="none", label=label, zorder=3)
        ax_delta.plot(midpoints, deltas, color=c, alpha=0.3, linewidth=0.8, zorder=2)

        # --- Bottom: rolling std ---
        win = min(5, len(deltas))
        if win >= 2:
            roll_std = pd.Series(deltas).rolling(win, min_periods=2).std().values
            ax_roll.plot(midpoints, roll_std, color=c, alpha=0.8, linewidth=1.2, label=label)
            ax_roll.scatter(midpoints, roll_std, s=16, color=c, alpha=0.6, edgecolors="none")

        violations = int(np.sum((deltas < lo) | (deltas > hi)))
        all_violations += violations
        all_deltas += len(deltas)

    # Top panel formatting
    ax_delta.axhspan(lo, hi, color="#27AE60", alpha=0.10, zorder=0,
                     label=f"Expected ±{int(tol*100)}%")
    ax_delta.axhline(tick_s, color="#27AE60", linestyle="--", linewidth=1, alpha=0.6)
    ax_delta.set_ylabel("Inter-tick delta (s)")
    ax_delta.set_title(title)
    ax_delta.legend(fontsize=8, loc="upper right", ncol=min(n_tasks + 1, 4))
    ax_delta.grid(True, alpha=0.15)

    # Bottom panel formatting
    ax_roll.axhline(0, color="#999999", linewidth=0.5)
    ax_roll.set_ylabel("Rolling σ of delta (s)")
    ax_roll.set_xlabel("Elapsed time (s)")
    ax_roll.legend(fontsize=8, loc="upper right", ncol=min(n_tasks, 4))
    ax_roll.grid(True, alpha=0.15)

    # Summary text
    pct = (all_violations / all_deltas * 100) if all_deltas else 0
    ax_delta.text(
        0.01, 0.97,
        f"Violations: {all_violations}/{all_deltas} ({pct:.0f}%) — expected Δ = {tick_s:.0f}s",
        transform=ax_delta.transAxes, fontsize=9, va="top",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8),
    )

    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


def traffic_pattern_analysis(
    profiler_df: pd.DataFrame,
    lifecycle: pd.DataFrame,
    *,
    title: str = "Traffic Pattern Analysis",
    output_dir: Path | None = None,
    plot_name: str = "traffic-pattern",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure | None:
    """Validate that each DP task emits exactly one message per tick interval.

    For each data-perturbation task, computes the time delta between consecutive
    emissions (EPOCH_ADVANCED or DUMMY_RELEASED). Plots per-task deltas as a
    scatter chart with the expected tick interval shown as a horizontal band.
    Prints a summary table of violations where the gap exceeds the tolerance.

    Returns the figure, or None if insufficient data.
    """
    COMP = "bolt-data-perturbation"

    dp_lc = lifecycle[lifecycle["component"] == COMP].copy()
    if dp_lc.empty:
        print("Skipped traffic pattern analysis: no data-perturbation lifecycle events")
        return None

    # Tick interval
    tick_ev = dp_lc[dp_lc["event"] == "TICK_INTERVAL_SECS"]
    if tick_ev.empty:
        print("Skipped traffic pattern analysis: no TICK_INTERVAL_SECS event")
        return None
    tick_s = float(tick_ev["epoch"].iloc[0])

    # Emission events: EPOCH_ADVANCED (real) and DUMMY_RELEASED (dummy)
    emission_events = dp_lc[dp_lc["event"].isin(["EPOCH_ADVANCED", "DUMMY_RELEASED"])].copy()
    if emission_events.empty:
        print("Skipped traffic pattern analysis: no emission events (EPOCH_ADVANCED / DUMMY_RELEASED)")
        return None

    tasks = sorted(emission_events["taskId"].unique())
    n_tasks = len(tasks)

    # Tolerance: allow ±20% of tick interval
    tolerance = tick_s * 0.20
    lo, hi = tick_s - tolerance, tick_s + tolerance

    # Build per-task delta series
    task_deltas: dict[int, pd.DataFrame] = {}
    for task_id in tasks:
        task_em = emission_events[emission_events["taskId"] == task_id].sort_values("elapsed_s")
        if len(task_em) < 2:
            continue
        rows = []
        prev_row = None
        for _, row in task_em.iterrows():
            if prev_row is not None:
                delta = row["elapsed_s"] - prev_row["elapsed_s"]
                rows.append({
                    "elapsed_s": row["elapsed_s"],
                    "delta_s": delta,
                    "event": row["event"],
                    "epoch": row["epoch"],
                    "prev_event": prev_row["event"],
                })
            prev_row = row
        task_deltas[task_id] = pd.DataFrame(rows)

    if not task_deltas:
        print("Skipped traffic pattern analysis: insufficient emission data")
        return None

    # --- Figure: scatter plot of inter-emission deltas per task ---
    fig, axes = plt.subplots(
        n_tasks, 1, figsize=(14, max(4, n_tasks * 2.0)),
        sharex=True, constrained_layout=True,
    )
    if n_tasks == 1:
        axes = [axes]

    total_violations = 0
    total_emissions = 0

    for ax, task_id in zip(axes, tasks):
        td = task_deltas.get(task_id)
        if td is None or td.empty:
            ax.set_ylabel(f"Task {task_id}")
            continue

        total_emissions += len(td)

        # Classify each delta
        ok = td[(td["delta_s"] >= lo) & (td["delta_s"] <= hi)]
        too_short = td[td["delta_s"] < lo]
        too_long = td[td["delta_s"] > hi]
        n_violations = len(too_short) + len(too_long)
        total_violations += n_violations

        # Expected band
        ax.axhspan(lo, hi, color="#27AE60", alpha=0.10, label=f"Expected ({lo:.0f}–{hi:.0f}s)")
        ax.axhline(tick_s, color="#27AE60", linewidth=1, linestyle="--", alpha=0.5)

        # OK points
        if not ok.empty:
            ax.scatter(ok["elapsed_s"], ok["delta_s"], c="#27AE60", s=25, alpha=0.7,
                       edgecolors="none", label=f"OK ({len(ok)})")
        # Too short
        if not too_short.empty:
            ax.scatter(too_short["elapsed_s"], too_short["delta_s"], c="#E74C3C", s=35,
                       marker="v", alpha=0.8, edgecolors="none",
                       label=f"Too short ({len(too_short)})")
        # Too long
        if not too_long.empty:
            ax.scatter(too_long["elapsed_s"], too_long["delta_s"], c="#E67E22", s=35,
                       marker="^", alpha=0.8, edgecolors="none",
                       label=f"Too long ({len(too_long)})")

        ax.set_ylabel(f"Task {task_id}\n\u0394t (s)", fontsize=9)
        ax.legend(fontsize=7, loc="upper right", ncol=3)
        ax.grid(True, alpha=0.2)

    axes[-1].set_xlabel("Elapsed time (s)")
    fig.suptitle(f"{title}  (tick={tick_s:.0f}s, violations={total_violations}/{total_emissions})",
                 fontsize=11)

    # --- Print summary ---
    print(f"Traffic pattern validation: tick_interval={tick_s:.0f}s, "
          f"tolerance=±{tolerance:.1f}s ({lo:.1f}–{hi:.1f}s)")
    print(f"  Total emissions: {total_emissions}, Violations: {total_violations}")
    for task_id in tasks:
        td = task_deltas.get(task_id)
        if td is None:
            continue
        violations = td[(td["delta_s"] < lo) | (td["delta_s"] > hi)]
        if violations.empty:
            print(f"  Task {task_id}: ALL OK ✓ ({len(td)} intervals)")
        else:
            print(f"  Task {task_id}: {len(violations)} violation(s) out of {len(td)} intervals:")
            for _, v in violations.iterrows():
                direction = "short" if v["delta_s"] < lo else "long"
                print(f"    t={v['elapsed_s']:.1f}s  Δ={v['delta_s']:.1f}s ({direction})  "
                      f"{v['prev_event']}→{v['event']}  epoch={int(v['epoch'])}")

    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


# ---------------------------------------------------------------------------
# Counter / Gauge comparison plots (Section A6+)
# ---------------------------------------------------------------------------

def counter_comparison_bar(
    matched_df: pd.DataFrame,
    counter_name: str,
    *,
    ylabel: str = "",
    title: str = "",
    output_dir: Path | None = None,
    plot_name: str = "counter-comparison",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure | None:
    """Side-by-side bars comparing a counter's total between baseline and confidential.

    ``counter_name`` is the column root (without ``_base``/``_conf`` suffix).
    """
    base_col = f"{counter_name}_base"
    conf_col = f"{counter_name}_conf"
    if base_col not in matched_df.columns or conf_col not in matched_df.columns:
        print(f"Skipped: {counter_name} not available in both topologies")
        return None
    return paired_bar_chart(
        matched_df, counter_name,
        ylabel=ylabel, title=title,
        output_dir=output_dir, plot_name=plot_name, fmt=fmt, show=show,
    )


def dummy_vs_real_ratio_bar(
    matched_df: pd.DataFrame,
    real_col: str = "counter_data_perturbation_real_emissions",
    dummy_col: str = "counter_data_perturbation_dummy_emissions",
    *,
    title: str = "Dummy / Real Emission Ratio: Baseline vs Confidential",
    output_dir: Path | None = None,
    plot_name: str = "dummy-real-ratio",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure | None:
    """Bar chart showing the ratio of dummy to real emissions for each matched pair."""
    real_b = f"{real_col}_base"
    real_c = f"{real_col}_conf"
    dummy_b = f"{dummy_col}_base"
    dummy_c = f"{dummy_col}_conf"

    needed = [real_b, real_c, dummy_b, dummy_c]
    if not all(c in matched_df.columns for c in needed):
        print(f"Skipped: need columns {needed}")
        return None

    labels = matched_df["label"].tolist()
    n = len(labels)

    with np.errstate(divide="ignore", invalid="ignore"):
        ratio_base = matched_df[dummy_b].values / matched_df[real_b].values
        ratio_conf = matched_df[dummy_c].values / matched_df[real_c].values

    fig, ax = plt.subplots(figsize=adaptive_figsize(n, per_item=1.6))
    x = np.arange(n)
    w = 0.35

    ax.bar(x - w / 2, ratio_base, w, label="Baseline", color=COLOR_BASELINE, alpha=0.85)
    ax.bar(x + w / 2, ratio_conf, w, label="Confidential (SGX)", color=COLOR_CONFIDENTIAL, alpha=0.85)

    for i in range(n):
        for val, offset in [(ratio_base[i], -w / 2), (ratio_conf[i], w / 2)]:
            if pd.notna(val) and np.isfinite(val):
                ax.text(i + offset, val, f"{val:.1f}", ha="center", va="bottom", fontsize=7)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("Dummy / Real ratio")
    ax.set_title(title)
    ax.legend()
    ax.grid(True, axis="y", alpha=0.3)
    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


def emission_stacked_bar(
    matched_df: pd.DataFrame,
    real_col: str = "counter_data_perturbation_real_emissions",
    dummy_col: str = "counter_data_perturbation_dummy_emissions",
    *,
    title: str = "Emission Breakdown: Real vs Dummy",
    output_dir: Path | None = None,
    plot_name: str = "emission-breakdown",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure | None:
    """Stacked bar chart showing real + dummy emissions per config, side-by-side base/conf."""
    needed_base = [f"{real_col}_base", f"{dummy_col}_base"]
    needed_conf = [f"{real_col}_conf", f"{dummy_col}_conf"]
    if not all(c in matched_df.columns for c in needed_base + needed_conf):
        print("Skipped: emission counter columns not available in both topologies")
        return None

    labels = matched_df["label"].tolist()
    n = len(labels)
    x = np.arange(n)
    w = 0.35

    real_b = matched_df[f"{real_col}_base"].values.astype(float)
    dummy_b = matched_df[f"{dummy_col}_base"].values.astype(float)
    real_c = matched_df[f"{real_col}_conf"].values.astype(float)
    dummy_c = matched_df[f"{dummy_col}_conf"].values.astype(float)

    fig, ax = plt.subplots(figsize=adaptive_figsize(n, per_item=1.6))

    # Baseline
    ax.bar(x - w / 2, real_b, w, label="Real (Baseline)", color=COLOR_BASELINE, alpha=0.85)
    ax.bar(x - w / 2, dummy_b, w, bottom=real_b, label="Dummy (Baseline)",
           color=COLOR_BASELINE, alpha=0.4, hatch="//")
    # Confidential
    ax.bar(x + w / 2, real_c, w, label="Real (Confidential)", color=COLOR_CONFIDENTIAL, alpha=0.85)
    ax.bar(x + w / 2, dummy_c, w, bottom=real_c, label="Dummy (Confidential)",
           color=COLOR_CONFIDENTIAL, alpha=0.4, hatch="//")

    # Annotate totals
    for i in range(n):
        for total, offset in [(real_b[i] + dummy_b[i], -w / 2), (real_c[i] + dummy_c[i], w / 2)]:
            if pd.notna(total) and total > 0:
                ax.text(i + offset, total, f"{total:.0f}", ha="center", va="bottom", fontsize=7)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("Total emissions")
    ax.set_title(title)
    ax.legend(fontsize=8)
    ax.grid(True, axis="y", alpha=0.3)
    save_or_show(fig, output_dir, plot_name, fmt, show)
    return fig


def counter_sensitivity_line(
    df: pd.DataFrame,
    sweep_col: str,
    counter_col: str,
    *,
    ylabel: str = "",
    title: str = "",
    output_dir: Path | None = None,
    plot_name: str = "counter-sensitivity",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure:
    """Sensitivity line plot for a counter metric, reusing the standard sensitivity_line."""
    return sensitivity_line(
        df, sweep_col, counter_col,
        ylabel=ylabel, title=title,
        output_dir=output_dir, plot_name=plot_name, fmt=fmt, show=show,
    )


def aggregation_partials_bar(
    matched_df: pd.DataFrame,
    real_col: str = "counter_histogram_aggregation_real_partials_received",
    dummy_col: str = "counter_histogram_aggregation_dummies_received",
    *,
    title: str = "Aggregation Bolt: Partials Received (Real vs Dummy)",
    output_dir: Path | None = None,
    plot_name: str = "aggregation-partials",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure | None:
    """Stacked bar chart of real vs dummy partials received at the aggregation bolt."""
    return emission_stacked_bar(
        matched_df, real_col=real_col, dummy_col=dummy_col,
        title=title, output_dir=output_dir, plot_name=plot_name, fmt=fmt, show=show,
    )


def gauge_epoch_lag_comparison(
    df: pd.DataFrame,
    sweep_col: str,
    *,
    title: str = "Epoch Lag vs Parameter",
    output_dir: Path | None = None,
    plot_name: str = "epoch-lag-sensitivity",
    fmt: str = "png",
    show: bool = True,
) -> plt.Figure | None:
    """Sensitivity line for avg epoch_lag gauge across configurations."""
    gauge_col = "gauge_avg_data_perturbation_epoch_lag"
    if gauge_col not in df.columns:
        print("Skipped: epoch_lag gauge not available")
        return None
    return sensitivity_line(
        df, sweep_col, gauge_col,
        ylabel="Avg epoch lag",
        title=title,
        output_dir=output_dir, plot_name=plot_name, fmt=fmt, show=show,
    )
