"""Tyre strategy plot — adapted from ~/f1_data_explorations/plotting/tyre_plots.py.

Data comes from OpenF1 /stints and /race_control instead of FastF1.
"""

from __future__ import annotations

from datetime import datetime

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np

# Compound colours matching F1 official palette
COMPOUND_COLOURS: dict[str, str] = {
    "SOFT": "#FF3333",
    "MEDIUM": "#FFD700",
    "HARD": "#FFFFFF",
    "INTERMEDIATE": "#39B54A",
    "WET": "#0067FF",
    "UNKNOWN": "#999999",
}

# Race-wide status code → (colour, alpha, hatch, label)
# Individual sector yellow flags are NOT included — they are too numerous and localised.
# Only race-wide statuses that affect all drivers are shown.
FLAG_STYLES: dict[int, tuple[str, float, str | None, str]] = {
    4: ("#FFAC1C", 0.25, None, "Safety Car"),
    5: ("red", 0.40, None, "Red Flag"),
    6: ("#FFAC1C", 0.25, "|||", "Virtual Safety Car"),
}


def _compound_colour(compound: str) -> str:
    return COMPOUND_COLOURS.get(compound.upper(), COMPOUND_COLOURS["UNKNOWN"])


def _parse_dt(s: str) -> datetime:
    """Parse ISO-8601 timestamp from OpenF1 (with or without trailing Z)."""
    s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)


def _flag_events_to_lap_ranges(
    race_control: list[dict],
    laps: list[dict],
) -> list[tuple[float, float, int]]:
    """
    Return list of (start_lap, end_lap, status_code) for SC, VSC, and Red Flag.

    Only race-wide statuses are included.  Individual sector yellow flags are
    intentionally ignored — they are too numerous and localised to be meaningful
    as full-width chart overlays.

    SC/VSC periods are built by pairing DEPLOYED → ENDING events.
    """
    # Build sorted (datetime, lap_number) from laps that have date_start
    lap_starts: list[tuple[datetime, int]] = []
    for lap in laps:
        if lap.get("date_start") and lap.get("lap_number"):
            try:
                lap_starts.append((_parse_dt(lap["date_start"]), lap["lap_number"]))
            except (ValueError, TypeError):
                pass
    if not lap_starts:
        return []
    lap_starts.sort()
    _lap_times_arr = [t for t, _ in lap_starts]

    def _ts_to_lap(ts: datetime) -> float:
        idx = np.searchsorted(_lap_times_arr, ts, side="right") - 1
        idx = max(0, min(idx, len(lap_starts) - 1))
        return float(lap_starts[idx][1])

    # Collect chronologically-sorted SafetyCar / Red Flag events
    sc_raw: list[tuple[datetime, str]] = []   # (time, "start"|"end")
    vsc_raw: list[tuple[datetime, str]] = []
    red_raw: list[tuple[datetime, str]] = []

    for ev in race_control:
        cat = ev.get("category", "")
        flag = (ev.get("flag") or "").upper()
        msg = (ev.get("message") or "").upper()
        dt = _parse_dt(ev.get("date") or "")
        if dt is None:
            continue

        if cat == "SafetyCar":
            is_vsc = "VSC" in msg or "VIRTUAL" in msg
            if "DEPLOYED" in msg:
                (vsc_raw if is_vsc else sc_raw).append((dt, "start"))
            elif "ENDING" in msg or "IN THIS LAP" in msg:
                (vsc_raw if is_vsc else sc_raw).append((dt, "end"))
        elif cat == "Flag" and flag == "RED":
            red_raw.append((dt, "start"))
        elif cat == "SessionStatus" and "STARTED" in msg:
            # Session restart clears a red flag
            red_raw.append((dt, "end"))

    def _pair(events: list[tuple[datetime, str]]) -> list[tuple[datetime, datetime]]:
        """Pair start/end events into (start_dt, end_dt) intervals."""
        out = []
        start_dt: datetime | None = None
        for dt, kind in sorted(events):
            if kind == "start":
                start_dt = dt
            elif kind == "end" and start_dt is not None:
                out.append((start_dt, dt))
                start_dt = None
        return out

    ranges: list[tuple[float, float, int]] = []
    for code, pairs in ((4, _pair(sc_raw)), (6, _pair(vsc_raw)), (5, _pair(red_raw))):
        for start_ts, end_ts in pairs:
            s = _ts_to_lap(start_ts)
            e = _ts_to_lap(end_ts)
            if e >= s:
                ranges.append((s, e, code))

    return ranges


def neutralized_lap_numbers(race_control: list[dict], laps: list[dict]) -> set[int]:
    """Return the set of lap numbers that were under SC, VSC, or Red Flag."""
    ranges = _flag_events_to_lap_ranges(race_control, laps)
    result: set[int] = set()
    for start, end, _ in ranges:
        for n in range(int(start), int(end) + 1):
            result.add(n)
    return result


def plot_stints(
    stints: list[dict],
    laps: list[dict],
    race_control: list[dict],
    drivers_ordered: list[dict],
    title: str = "Tyre Strategy",
) -> plt.Figure:
    """
    Parameters
    ----------
    stints:
        OpenF1 /stints response for the session.
    laps:
        OpenF1 /laps response for the session (all drivers).
    race_control:
        OpenF1 /race_control response.
    drivers_ordered:
        List of driver dicts (from /drivers), sorted by finishing position
        (1st at index 0).  Each dict must have 'driver_number' and
        'name_acronym' keys.
    title:
        Chart title.
    """
    fig, ax = plt.subplots(figsize=(15, 10))
    fig.patch.set_facecolor("black")
    ax.set_facecolor("black")

    driver_numbers = [d["driver_number"] for d in drivers_ordered]
    labels = [d["name_acronym"] for d in drivers_ordered]
    n_drivers = len(labels)

    compound_colours_used: dict[str, str] = {}
    track_status_used: set[int] = set()

    # Build a lookup: driver_number → list of stints sorted by stint_number
    stints_by_driver: dict[int, list[dict]] = {}
    for s in stints:
        dn = s.get("driver_number")
        if dn is not None:
            stints_by_driver.setdefault(dn, []).append(s)
    for dn in stints_by_driver:
        stints_by_driver[dn].sort(key=lambda s: s.get("stint_number", 0))

    for y_idx, (driver_number, label) in enumerate(zip(driver_numbers, labels)):
        driver_stints = stints_by_driver.get(driver_number, [])
        compound_first_seen: set[str] = set()

        for stint in driver_stints:
            compound = (stint.get("compound") or "UNKNOWN").upper()
            lap_start = stint.get("lap_start", 1) or 1
            lap_end = stint.get("lap_end") or lap_start
            stint_len = max(1, lap_end - lap_start + 1)

            colour = _compound_colour(compound)
            compound_colours_used[compound] = colour

            is_fresh = stint.get("tyre_age_at_start", 0) == 0
            # Also treat it as used if we've already seen this compound for this driver
            if compound in compound_first_seen:
                is_fresh = False
            compound_first_seen.add(compound)

            ax.barh(
                y=y_idx,
                width=stint_len,
                left=lap_start - 1,
                color=colour,
                edgecolor="white",
                linewidth=0.5,
                fill=True,
                hatch=None if is_fresh else "///",
                alpha=0.8 if is_fresh else 0.6,
            )

    # Race-control overlays
    flag_ranges = _flag_events_to_lap_ranges(race_control, laps)
    for start_lap, end_lap, code in flag_ranges:
        if code not in FLAG_STYLES:
            continue
        colour, alpha, hatch, _ = FLAG_STYLES[code]
        track_status_used.add(code)
        rect = mpatches.Rectangle(
            (start_lap - 1, -0.5),
            end_lap - start_lap + 1,
            n_drivers,
            facecolor=colour,
            alpha=alpha,
            hatch=hatch,
            edgecolor="none",
            zorder=10,
        )
        ax.add_patch(rect)

    # Legend
    legend_elements: list[mpatches.Patch] = []
    for compound, colour in sorted(compound_colours_used.items()):
        legend_elements.append(
            mpatches.Patch(facecolor=colour, edgecolor="white", alpha=0.8,
                           label=f"{compound} (Fresh)")
        )
        legend_elements.append(
            mpatches.Patch(facecolor=colour, edgecolor="white", alpha=0.6,
                           hatch="///", label=f"{compound} (Used)")
        )
    for code in sorted(track_status_used):
        colour, alpha, hatch, label = FLAG_STYLES[code]
        legend_elements.append(
            mpatches.Patch(facecolor=colour, edgecolor="none", alpha=alpha,
                           hatch=hatch, label=label)
        )

    ax.legend(
        handles=legend_elements,
        bbox_to_anchor=(1.02, 1),
        loc="upper left",
        frameon=True,
        facecolor="black",
        edgecolor="white",
        labelcolor="white",
    )

    ax.set_yticks(range(n_drivers))
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlabel("Lap Number", color="white", fontsize=14)
    ax.set_title(title, color="white", fontsize=20)
    ax.tick_params(colors="white")
    ax.grid(False)

    for spine in ax.spines.values():
        spine.set_color("white")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)

    fig.tight_layout()
    return fig
