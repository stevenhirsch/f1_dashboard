"""Lap time sector breakdown chart — stacked S1/S2/S3 bars for every lap."""

from __future__ import annotations

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

S1_COLOUR = "#E67E22"   # orange
S2_COLOUR = "#F1C40F"   # yellow
S3_COLOUR = "#1ABC9C"   # teal
NO_DATA_COLOUR = "#555555"


def _safe_float(v) -> float | None:
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def plot_lap_times(
    laps: list[dict],
    driver_number: int,
    selected_lap: int,
    title: str = "Lap Times",
) -> plt.Figure:
    """
    Stacked bar chart: S1 / S2 / S3 for every lap.

    - Each lap shows three stacked segments (orange S1, yellow S2, teal S3).
    - If sector data is missing for a lap, a grey bar for total lap time is shown.
    - The selected lap is outlined with a white border.
    - Reference lines for personal best and race best total lap time.
    """
    fig, ax = plt.subplots(figsize=(14, 5))
    fig.patch.set_facecolor("black")
    ax.set_facecolor("black")

    # Only include laps that have all three sector times
    driver_laps = sorted(
        [l for l in laps
         if l.get("driver_number") == driver_number
         and _safe_float(l.get("duration_sector_1")) is not None
         and _safe_float(l.get("duration_sector_2")) is not None
         and _safe_float(l.get("duration_sector_3")) is not None],
        key=lambda l: l.get("lap_number", 0),
    )

    if not driver_laps:
        ax.text(0.5, 0.5, "No sector data available", color="white",
                ha="center", va="center", transform=ax.transAxes)
        ax.set_axis_off()
        fig.suptitle(title, color="white", fontsize=14)
        return fig

    width = 0.8

    for lap in driver_laps:
        x = lap["lap_number"]
        is_selected = (x == selected_lap)
        edge = "white" if is_selected else "none"
        lw = 1.5 if is_selected else 0

        s1 = _safe_float(lap["duration_sector_1"])
        s2 = _safe_float(lap["duration_sector_2"])
        s3 = _safe_float(lap["duration_sector_3"])
        ax.bar(x, s1, width=width, color=S1_COLOUR, edgecolor=edge, linewidth=lw)
        ax.bar(x, s2, width=width, bottom=s1, color=S2_COLOUR, edgecolor=edge, linewidth=lw)
        ax.bar(x, s3, width=width, bottom=s1 + s2, color=S3_COLOUR, edgecolor=edge, linewidth=lw)

    # Reference lines (total lap time = s1+s2+s3)
    _totals = [(_safe_float(l["duration_sector_1"]) or 0)
               + (_safe_float(l["duration_sector_2"]) or 0)
               + (_safe_float(l["duration_sector_3"]) or 0)
               for l in driver_laps]
    _session_totals = [
        (_safe_float(l.get("duration_sector_1")) or 0)
        + (_safe_float(l.get("duration_sector_2")) or 0)
        + (_safe_float(l.get("duration_sector_3")) or 0)
        for l in laps
        if _safe_float(l.get("duration_sector_1"))
        and _safe_float(l.get("duration_sector_2"))
        and _safe_float(l.get("duration_sector_3"))
    ]

    if _totals:
        ax.axhline(min(_totals), color="#2ECC71", linestyle="--",
                   linewidth=1.2, label=f"Personal best ({min(_totals):.3f}s)", alpha=0.9)
    if _session_totals and (_totals and min(_session_totals) < min(_totals)):
        ax.axhline(min(_session_totals), color="#9B59B6", linestyle="--",
                   linewidth=1.2, label=f"Session best ({min(_session_totals):.3f}s)", alpha=0.9)

    # Legend
    legend_handles = [
        mpatches.Patch(color=S1_COLOUR, label="Sector 1"),
        mpatches.Patch(color=S2_COLOUR, label="Sector 2"),
        mpatches.Patch(color=S3_COLOUR, label="Sector 3"),
    ]
    ax.legend(handles=legend_handles, loc="upper right",
              frameon=True, facecolor="black", edgecolor="white",
              labelcolor="white", fontsize=9)

    # Selected lap annotation
    sel = next((l for l in driver_laps if l.get("lap_number") == selected_lap), None)
    if sel:
        _s1 = _safe_float(sel.get("duration_sector_1"))
        _s2 = _safe_float(sel.get("duration_sector_2"))
        _s3 = _safe_float(sel.get("duration_sector_3"))
        _tot = _safe_float(sel.get("lap_duration"))
        if _s1 and _s2 and _s3:
            _label = f"Lap {selected_lap}:  S1 {_s1:.3f}s  S2 {_s2:.3f}s  S3 {_s3:.3f}s  Total {_tot:.3f}s"
        elif _tot:
            _label = f"Lap {selected_lap}: {_tot:.3f}s (no sector breakdown)"
        else:
            _label = ""
        if _label:
            ax.set_xlabel(_label, color="#F1C40F", fontsize=10)
    else:
        ax.set_xlabel("Lap", color="white", fontsize=12)

    ax.set_ylabel("Duration (s)", color="white", fontsize=12)
    ax.set_title(title, color="white", fontsize=14)
    ax.tick_params(colors="white")
    ax.grid(axis="y", color="white", alpha=0.08, linestyle="--")

    for spine in ax.spines.values():
        spine.set_color("white")

    fig.tight_layout()
    return fig
