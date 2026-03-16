"""GPS-based throttle/brake colour-coded track map — single plot."""

from __future__ import annotations

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.collections import LineCollection


def _merge_telemetry(
    car_data: list[dict],
    location_data: list[dict],
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    if not car_data or not location_data:
        return np.array([]), np.array([]), np.array([]), np.array([])

    def _ts(s: str) -> float:
        from datetime import datetime
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()

    loc_times = np.array([_ts(r["date"]) for r in location_data])
    loc_x = np.array([r.get("x", 0) for r in location_data], dtype=float)
    loc_y = np.array([r.get("y", 0) for r in location_data], dtype=float)

    xs, ys, throttles, brakes = [], [], [], []
    for row in car_data:
        if not row.get("date"):
            continue
        t = _ts(row["date"])
        idx = int(np.searchsorted(loc_times, t, side="left"))
        idx = min(idx, len(loc_times) - 1)
        xs.append(loc_x[idx])
        ys.append(loc_y[idx])
        throttles.append(float(row.get("throttle", 0)))
        brakes.append(float(row.get("brake", 0)))

    return (np.array(xs), np.array(ys),
            np.array(throttles), np.array(brakes))


def plot_lap_map(
    car_data: list[dict],
    location_data: list[dict],
    title: str = "Track Map",
) -> plt.Figure:
    """
    Single track map showing throttle (green→red gradient) with braking zones
    overlaid as bright red on top.
    """
    fig, ax = plt.subplots(figsize=(10, 8))
    fig.patch.set_facecolor("black")
    ax.set_facecolor("black")

    xs, ys, throttles, brakes = _merge_telemetry(car_data, location_data)

    if not len(xs):
        ax.text(0.5, 0.5, "No telemetry data", color="white",
                ha="center", va="center", transform=ax.transAxes)
        ax.set_axis_off()
        fig.suptitle(title, color="white", fontsize=14)
        return fig

    def _make_segments(x, y):
        pts = np.array([x, y]).T.reshape(-1, 1, 2)
        return np.concatenate([pts[:-1], pts[1:]], axis=1)

    segs = _make_segments(xs, ys)
    mid_throttle = (throttles[:-1] + throttles[1:]) / 2
    mid_brake = (brakes[:-1] + brakes[1:]) / 2

    # Layer 1: throttle (full track, green=on throttle, red=lift)
    lc_throttle = LineCollection(
        segs,
        cmap="RdYlGn",
        norm=mcolors.Normalize(0, 100),
        linewidth=3,
        zorder=1,
    )
    lc_throttle.set_array(mid_throttle)
    ax.add_collection(lc_throttle)

    # Layer 2: braking zones overlaid in bright red, thicker
    brake_mask = mid_brake > 5  # >5% brake pressure considered braking
    if brake_mask.any():
        lc_brake = LineCollection(
            segs[brake_mask],
            colors="#FF2D2D",
            linewidth=5,
            zorder=2,
            alpha=0.85,
        )
        ax.add_collection(lc_brake)

    ax.autoscale()
    ax.set_aspect("equal")
    ax.set_axis_off()

    # Throttle colorbar
    cb = fig.colorbar(lc_throttle, ax=ax, orientation="horizontal",
                      pad=0.02, fraction=0.03, shrink=0.6)
    cb.set_label("Throttle (%)", color="white", fontsize=10)
    cb.ax.tick_params(colors="white")

    # Brake legend patch
    from matplotlib.lines import Line2D
    _brake_handle = Line2D([0], [0], color="#FF2D2D", linewidth=4, label="Braking (>5%)")
    ax.legend(handles=[_brake_handle], loc="lower right",
              frameon=True, facecolor="black", edgecolor="white", labelcolor="white")

    fig.suptitle(title, color="white", fontsize=13)
    fig.tight_layout()
    return fig
