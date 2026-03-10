from pathlib import Path
import matplotlib.pyplot as plt

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


def color_for(idx: int) -> str:
    return _PALETTE[idx % len(_PALETTE)]