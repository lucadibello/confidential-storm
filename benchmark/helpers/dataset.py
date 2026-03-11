from __future__ import annotations

from pathlib import Path
import pandas as pd

def _get_components(df: pd.DataFrame) -> list[str]:
    return sorted(df["component"].unique())


def _get_ecall_names(ecalls: pd.DataFrame, component: str) -> list[str]:
    return sorted(ecalls.loc[ecalls["component"] == component, "name"].unique())


def get_lifecycle_events(df: pd.DataFrame) -> pd.DataFrame:
    """Extract lifecycle events from profiler data.  """
    lifecycle = df[df["type"] == "lifecycle"][["component", "taskId", "name", "total", "timestamp", "elapsed_s"]].copy()
    lifecycle = lifecycle.rename(columns={"name": "event", "total": "epoch"})
    return lifecycle
