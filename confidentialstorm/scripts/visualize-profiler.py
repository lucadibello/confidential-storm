#!/usr/bin/env python3
"""
visualize-profiler.py - Visualize Confidential Storm bolt profiler CSV reports.

Reads all profiler-*.csv files from a directory and produces:

  1. ECALL latency over time (avg, p50, p95, p99) per component
  2. ECALL latency distribution (box plots) per component
  3. Throughput over time (total invocations per window)
  4. Counter evolution over time (dropped, forwarded, dummy_emissions, etc.)
  5. Gauge evolution over time (epoch_lag, ticks_to_completion, etc.)
  6. Task imbalance comparison (side-by-side ECALL latency & throughput across tasks)
  7. Summary dashboard (one page with key metrics across all components)

Usage:
    python3 visualize-profiler.py <csv-dir> [--output <output-dir>] [--format png|pdf|svg]

Examples:
    python3 visualize-profiler.py /logs/storm
    python3 visualize-profiler.py /logs/storm --output ./plots --format pdf
    python3 visualize-profiler.py /logs/storm --show  # interactive display

Requirements:
    pip install pandas matplotlib
"""

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_profiler_csvs(csv_dir: Path) -> pd.DataFrame:
    """Load and concatenate all profiler-*.csv files from the given directory."""
    files = sorted(csv_dir.glob("profiler-*.csv"))
    if not files:
        print(f"ERROR: No profiler-*.csv files found in '{csv_dir}'.")
        sys.exit(1)

    frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
            df["source_file"] = f.name
            frames.append(df)
        except Exception as e:
            print(f"  WARNING: Failed to read {f.name}: {e}")

    if not frames:
        print("ERROR: No valid CSV data loaded.")
        sys.exit(1)

    combined = pd.concat(frames, ignore_index=True)
    combined["timestamp"] = pd.to_datetime(combined["timestamp"])
    # Compute a relative time in seconds from the earliest timestamp
    t0 = combined["timestamp"].min()
    combined["elapsed_s"] = (combined["timestamp"] - t0).dt.total_seconds()
    # Build a label for each task
    combined["task_label"] = combined["component"] + " (task " + combined["taskId"].astype(str) + ")"
    return combined


# ---------------------------------------------------------------------------
# Helpers
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


def _format_elapsed_axis(ax):
    ax.set_xlabel("Elapsed time (s)")


def _get_components(df: pd.DataFrame) -> list[str]:
    return sorted(df["component"].unique())


def _get_ecall_names(ecalls: pd.DataFrame, component: str) -> list[str]:
    return sorted(ecalls.loc[ecalls["component"] == component, "name"].unique())


PALETTE = plt.rcParams["axes.prop_cycle"].by_key()["color"]


def _color_for(idx: int) -> str:
    return PALETTE[idx % len(PALETTE)]


# ---------------------------------------------------------------------------
# Plot 1: ECALL latency over time
# ---------------------------------------------------------------------------

def plot_ecall_latency_over_time(df: pd.DataFrame, output_dir, fmt, show):
    ecalls = df[df["type"] == "ecall"]
    if ecalls.empty:
        return
    components = _get_components(ecalls)

    for comp in components:
        comp_data = ecalls[ecalls["component"] == comp]
        ecall_names = _get_ecall_names(ecalls, comp)

        for ecall_name in ecall_names:
            ed = comp_data[comp_data["name"] == ecall_name]
            tasks = sorted(ed["taskId"].unique())
            n_tasks = len(tasks)

            # Layout: 1 overview plot + 1 per-task detail plot, stacked vertically
            n_rows = 1 + n_tasks
            fig, axes = plt.subplots(n_rows, 1, figsize=(12, 4 * n_rows), sharex=True, constrained_layout=True)
            if n_rows == 1:
                axes = [axes]

            # --- Row 0: overview — avg latency for all tasks on the same axes ---
            ax_overview = axes[0]
            for i, task in enumerate(tasks):
                td = ed[ed["taskId"] == task].sort_values("elapsed_s")
                ax_overview.plot(td["elapsed_s"], td["avgMs"],
                                label=f"task {task}", color=_color_for(i), linewidth=1.5)
            ax_overview.set_ylabel("Avg latency (ms)")
            ax_overview.set_title("Avg latency comparison (all tasks)", fontsize=10)
            ax_overview.legend(fontsize=8, ncol=min(n_tasks, 6))
            ax_overview.grid(True, alpha=0.3)

            # --- Rows 1..N: per-task detail with percentile bands ---
            for i, task in enumerate(tasks):
                ax = axes[1 + i]
                td = ed[ed["taskId"] == task].sort_values("elapsed_s")
                c = _color_for(i)
                ax.plot(td["elapsed_s"], td["avgMs"], label="avg",
                        color=c, linewidth=1.5)
                ax.fill_between(td["elapsed_s"], td["p50Ms"], td["p95Ms"],
                                alpha=0.20, color=c, label="p50–p95")
                ax.fill_between(td["elapsed_s"], td["p95Ms"], td["p99Ms"],
                                alpha=0.10, color=c, label="p95–p99")
                ax.set_ylabel("Latency (ms)")
                ax.set_title(f"Task {task}", fontsize=10)
                ax.legend(fontsize=7, ncol=3)
                ax.grid(True, alpha=0.3)

            _format_elapsed_axis(axes[-1])
            fig.suptitle(f"{comp} — {ecall_name} latency over time", fontsize=13, fontweight="bold")
            save_or_show(fig, output_dir, f"latency-{comp}-{ecall_name}", fmt, show)


# ---------------------------------------------------------------------------
# Plot 2: ECALL latency distribution (box plots)
# ---------------------------------------------------------------------------

def plot_ecall_latency_boxplot(df: pd.DataFrame, output_dir, fmt, show):
    ecalls = df[df["type"] == "ecall"]
    if ecalls.empty:
        return
    components = _get_components(ecalls)

    for comp in components:
        comp_data = ecalls[ecalls["component"] == comp]
        ecall_names = _get_ecall_names(ecalls, comp)

        # Collect avgMs per ecall name for box plot
        data_for_box = []
        labels = []
        for en in ecall_names:
            vals = comp_data.loc[comp_data["name"] == en, "avgMs"].dropna()
            if not vals.empty:
                data_for_box.append(vals.values)
                labels.append(en)

        if not data_for_box:
            continue

        fig, ax = plt.subplots(figsize=(max(8, len(labels) * 2.5), 5))
        bp = ax.boxplot(data_for_box, tick_labels=labels, patch_artist=True)
        for i, patch in enumerate(bp["boxes"]):
            patch.set_facecolor(_color_for(i))
            patch.set_alpha(0.6)
        ax.set_ylabel("Avg latency (ms)")
        ax.set_title(f"{comp} — ECALL latency distribution across windows")
        ax.grid(True, axis="y", alpha=0.3)
        plt.xticks(rotation=45, ha="right")
        save_or_show(fig, output_dir, f"boxplot-{comp}", fmt, show)


# ---------------------------------------------------------------------------
# Plot 3: Throughput over time
# ---------------------------------------------------------------------------

def plot_throughput_over_time(df: pd.DataFrame, output_dir, fmt, show):
    ecalls = df[df["type"] == "ecall"]
    if ecalls.empty:
        return
    components = _get_components(ecalls)

    for comp in components:
        comp_data = ecalls[ecalls["component"] == comp]
        ecall_names = _get_ecall_names(ecalls, comp)

        fig, ax = plt.subplots(figsize=(12, 5))
        for i, en in enumerate(ecall_names):
            ed = comp_data[comp_data["name"] == en]
            # Sum total across all tasks per timestamp window
            grouped = ed.groupby("elapsed_s")["total"].sum().sort_index()
            ax.plot(grouped.index, grouped.values, label=en, color=_color_for(i), linewidth=1.5)

        ax.set_ylabel("Total invocations (per window)")
        ax.set_title(f"{comp} — Throughput over time")
        _format_elapsed_axis(ax)
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
        save_or_show(fig, output_dir, f"throughput-{comp}", fmt, show)


# ---------------------------------------------------------------------------
# Plot 4: Counter evolution over time
# ---------------------------------------------------------------------------

def plot_counters_over_time(df: pd.DataFrame, output_dir, fmt, show):
    counters = df[df["type"] == "counter"]
    if counters.empty:
        return
    components = _get_components(counters)

    for comp in components:
        comp_data = counters[counters["component"] == comp]
        counter_names = sorted(comp_data["name"].unique())
        if not counter_names:
            continue

        tasks = sorted(comp_data["taskId"].unique())
        n_counters = len(counter_names)
        fig, axes = plt.subplots(n_counters, 1, figsize=(12, 3.5 * n_counters), sharex=True,
                                 squeeze=False)
        for i, cn in enumerate(counter_names):
            ax = axes[i, 0]
            cd = comp_data[comp_data["name"] == cn]
            for j, task in enumerate(tasks):
                td = cd[cd["taskId"] == task].sort_values("elapsed_s")
                lbl = f"task {task}" if len(tasks) > 1 else cn
                ax.plot(td["elapsed_s"], td["total"], label=lbl,
                        color=_color_for(j), linewidth=1.5, marker=".", markersize=3)
            ax.set_ylabel(cn)
            ax.legend(fontsize=7, ncol=min(len(tasks), 4))
            ax.grid(True, alpha=0.3)

        axes[0, 0].set_title(f"{comp} — Counters over time")
        _format_elapsed_axis(axes[-1, 0])
        save_or_show(fig, output_dir, f"counters-{comp}", fmt, show)


# ---------------------------------------------------------------------------
# Plot 5: Gauge evolution over time
# ---------------------------------------------------------------------------

def plot_gauges_over_time(df: pd.DataFrame, output_dir, fmt, show):
    gauges = df[df["type"] == "gauge"]
    if gauges.empty:
        return
    components = _get_components(gauges)

    for comp in components:
        comp_data = gauges[gauges["component"] == comp]
        gauge_names = sorted(comp_data["name"].unique())
        if not gauge_names:
            continue

        n_gauges = len(gauge_names)
        fig, axes = plt.subplots(n_gauges, 1, figsize=(12, 3.5 * n_gauges), sharex=True,
                                 squeeze=False)
        for i, gn in enumerate(gauge_names):
            ax = axes[i, 0]
            gd = comp_data[comp_data["name"] == gn]
            tasks = sorted(gd["taskId"].unique())
            for j, task in enumerate(tasks):
                td = gd[gd["taskId"] == task].sort_values("elapsed_s")
                lbl = f"task {task}" if len(tasks) > 1 else gn
                ax.plot(td["elapsed_s"], td["total"], label=lbl,
                        color=_color_for(j), linewidth=1.2)
            ax.set_ylabel(gn)
            ax.legend(fontsize=7, ncol=min(len(tasks), 4))
            ax.grid(True, alpha=0.3)

        axes[0, 0].set_title(f"{comp} — Gauges over time")
        _format_elapsed_axis(axes[-1, 0])
        save_or_show(fig, output_dir, f"gauges-{comp}", fmt, show)


# ---------------------------------------------------------------------------
# Plot 6: Task imbalance comparison
# ---------------------------------------------------------------------------

def plot_task_imbalance(df: pd.DataFrame, output_dir, fmt, show):
    ecalls = df[df["type"] == "ecall"]
    if ecalls.empty:
        return
    components = _get_components(ecalls)

    for comp in components:
        comp_data = ecalls[ecalls["component"] == comp]
        tasks = sorted(comp_data["taskId"].unique())
        if len(tasks) < 2:
            continue

        ecall_names = _get_ecall_names(ecalls, comp)

        for en in ecall_names:
            ed = comp_data[comp_data["name"] == en]

            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

            # Left: avg latency per task (box plot)
            task_latencies = []
            task_labels = []
            for t in tasks:
                vals = ed.loc[ed["taskId"] == t, "avgMs"].dropna()
                if not vals.empty:
                    task_latencies.append(vals.values)
                    task_labels.append(f"task {t}")

            if task_latencies:
                bp = ax1.boxplot(task_latencies, tick_labels=task_labels, patch_artist=True)
                for i, patch in enumerate(bp["boxes"]):
                    patch.set_facecolor(_color_for(i))
                    patch.set_alpha(0.6)
            ax1.set_ylabel("Avg latency (ms)")
            ax1.set_title(f"Latency distribution per task")
            ax1.grid(True, axis="y", alpha=0.3)

            # Right: total invocations per task (bar chart)
            task_totals = []
            for t in tasks:
                td = ed[ed["taskId"] == t]
                # Take the max total (cumulative) as the final count
                task_totals.append(td["total"].max() if not td.empty else 0)

            bars = ax2.bar([f"task {t}" for t in tasks], task_totals,
                           color=[_color_for(i) for i in range(len(tasks))], alpha=0.7)
            ax2.set_ylabel("Total invocations")
            ax2.set_title(f"Throughput per task")
            ax2.grid(True, axis="y", alpha=0.3)
            # Add value labels on bars
            for bar, val in zip(bars, task_totals):
                if val > 0:
                    ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                             f"{val:,.0f}", ha="center", va="bottom", fontsize=8)

            fig.suptitle(f"{comp} — {en}: Task imbalance comparison", fontsize=13)
            save_or_show(fig, output_dir, f"imbalance-{comp}-{en}", fmt, show)

    # Counter imbalance (if multiple tasks)
    counters = df[df["type"] == "counter"]
    if counters.empty:
        return

    for comp in components:
        comp_counters = counters[counters["component"] == comp]
        tasks = sorted(comp_counters["taskId"].unique())
        if len(tasks) < 2:
            continue

        counter_names = sorted(comp_counters["name"].unique())
        if not counter_names:
            continue

        fig, ax = plt.subplots(figsize=(max(8, len(counter_names) * 2), 5))
        bar_width = 0.8 / len(tasks)
        x = np.arange(len(counter_names))

        for i, t in enumerate(tasks):
            td = comp_counters[comp_counters["taskId"] == t]
            vals = []
            for cn in counter_names:
                v = td.loc[td["name"] == cn, "total"]
                vals.append(v.max() if not v.empty else 0)
            ax.bar(x + i * bar_width, vals, bar_width, label=f"task {t}",
                   color=_color_for(i), alpha=0.7)

        ax.set_xticks(x + bar_width * (len(tasks) - 1) / 2)
        ax.set_xticklabels(counter_names, rotation=45, ha="right")
        ax.set_ylabel("Counter value (cumulative)")
        ax.set_title(f"{comp} — Counter values per task")
        ax.legend(fontsize=8)
        ax.grid(True, axis="y", alpha=0.3)
        save_or_show(fig, output_dir, f"imbalance-counters-{comp}", fmt, show)


# ---------------------------------------------------------------------------
# Plot 7: Summary dashboard
# ---------------------------------------------------------------------------

def plot_summary_dashboard(df: pd.DataFrame, output_dir, fmt, show):
    ecalls = df[df["type"] == "ecall"]
    counters_df = df[df["type"] == "counter"]
    gauges_df = df[df["type"] == "gauge"]

    components = _get_components(df)
    if not components:
        return

    # Collect summary stats per component
    rows = []
    for comp in components:
        ce = ecalls[ecalls["component"] == comp]
        for en in sorted(ce["name"].unique()):
            ed = ce[ce["name"] == en]
            rows.append({
                "Component": comp,
                "ECALL": en,
                "Avg (ms)": f"{ed['avgMs'].mean():.2f}",
                "p50 (ms)": f"{ed['p50Ms'].mean():.2f}",
                "p95 (ms)": f"{ed['p95Ms'].mean():.2f}",
                "p99 (ms)": f"{ed['p99Ms'].mean():.2f}",
                "Max (ms)": f"{ed['maxMs'].max():.2f}",
                "Total calls": f"{int(ed['total'].max()):,}",
                "Tasks": len(ed["taskId"].unique()),
            })

    if not rows:
        return

    summary = pd.DataFrame(rows)

    fig, ax = plt.subplots(figsize=(14, max(3, 1 + 0.5 * len(rows))))
    ax.axis("off")
    table = ax.table(
        cellText=summary.values,
        colLabels=summary.columns,
        cellLoc="center",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.auto_set_column_width(list(range(len(summary.columns))))
    # Style header
    for j in range(len(summary.columns)):
        table[(0, j)].set_facecolor("#4472C4")
        table[(0, j)].set_text_props(color="white", fontweight="bold")
    # Alternate row colors
    for i in range(1, len(rows) + 1):
        color = "#F2F2F2" if i % 2 == 0 else "white"
        for j in range(len(summary.columns)):
            table[(i, j)].set_facecolor(color)

    ax.set_title("Profiler Summary — All Components", fontsize=14, pad=20)
    save_or_show(fig, output_dir, "summary-dashboard", fmt, show)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Visualize Confidential Storm bolt profiler CSV reports.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /logs/storm                          # save PNGs to /logs/storm/plots/
  %(prog)s /logs/storm --output ./my-plots      # custom output dir
  %(prog)s /logs/storm --format pdf             # PDF output
  %(prog)s /logs/storm --show                   # interactive matplotlib display
  %(prog)s /logs/storm --only latency,imbalance # only specific plots
""",
    )
    parser.add_argument("csv_dir", help="Directory containing profiler-*.csv files")
    parser.add_argument("--output", "-o", default=None,
                        help="Output directory for plots (default: <csv_dir>/plots)")
    parser.add_argument("--format", "-f", default="png", choices=["png", "pdf", "svg"],
                        help="Output file format (default: png)")
    parser.add_argument("--show", action="store_true",
                        help="Show plots interactively instead of only saving")
    parser.add_argument("--only", default=None,
                        help="Comma-separated list of plot types to generate: "
                             "latency,boxplot,throughput,counters,gauges,imbalance,summary")
    args = parser.parse_args()

    csv_dir = Path(args.csv_dir)
    if not csv_dir.is_dir():
        print(f"ERROR: '{csv_dir}' is not a directory.")
        sys.exit(1)

    output_dir = Path(args.output) if args.output else csv_dir / "plots"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading profiler CSVs from: {csv_dir}")
    df = load_profiler_csvs(csv_dir)
    print(f"  Loaded {len(df)} rows from {df['source_file'].nunique()} file(s)")
    print(f"  Components: {', '.join(_get_components(df))}")
    print(f"  Time span: {df['elapsed_s'].max():.1f}s")
    print(f"  Output: {output_dir}/")
    print()

    # Determine which plots to generate
    all_plots = {
        "latency": ("ECALL latency over time", plot_ecall_latency_over_time),
        "boxplot": ("ECALL latency distribution", plot_ecall_latency_boxplot),
        "throughput": ("Throughput over time", plot_throughput_over_time),
        "counters": ("Counter evolution", plot_counters_over_time),
        "gauges": ("Gauge evolution", plot_gauges_over_time),
        "imbalance": ("Task imbalance comparison", plot_task_imbalance),
        "summary": ("Summary dashboard", plot_summary_dashboard),
    }

    if args.only:
        selected = [s.strip() for s in args.only.split(",")]
        invalid = [s for s in selected if s not in all_plots]
        if invalid:
            print(f"ERROR: Unknown plot type(s): {', '.join(invalid)}")
            print(f"  Valid types: {', '.join(all_plots.keys())}")
            sys.exit(1)
    else:
        selected = list(all_plots.keys())

    for key in selected:
        label, fn = all_plots[key]
        print(f"Generating: {label}...")
        fn(df, output_dir, args.format, args.show)

    print()
    print("Done.")


if __name__ == "__main__":
    main()
