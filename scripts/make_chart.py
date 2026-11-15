"""
Draw docs/daily_max_temp.png from the database: daily max temperature per city.
The README shows this image, so anyone on GitHub can see results without running anything.

Run from the project folder (with the database running):
  python scripts/make_chart.py
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))  # so `pipeline` imports from the project root

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from pipeline.extract_load import connect

# Categorical colors in a fixed order (checked for color-blind separation), one per city
COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
SURFACE, TEXT, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"


def load_series():
    with connect() as conn:
        rows = conn.execute(
            "SELECT city, weather_date, temp_max_c FROM analytics.v_daily_weather "
            "WHERE temp_max_c IS NOT NULL ORDER BY city, weather_date"
        ).fetchall()
    series = {}
    for city, day, temp in rows:
        series.setdefault(city, ([], []))
        series[city][0].append(day)
        series[city][1].append(float(temp))
    return series


def main() -> None:
    series = load_series()
    if not series:
        raise SystemExit("no data yet: run the pipeline first")

    fig, ax = plt.subplots(figsize=(10, 5.2), dpi=160)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    end_labels = []
    for i, (city, (days, temps)) in enumerate(series.items()):
        # Giza sits exactly on top of Cairo (same API grid point), so draw it dashed
        style = "--" if city == "Giza" else "-"
        ax.plot(days, temps, style, color=COLORS[i % len(COLORS)], linewidth=2,
                marker="o", markersize=4, label=city, zorder=3)
        end_labels.append((temps[-1], city, days[-1]))

    # Direct labels at the end of each line, in text color (not the series color).
    # Cities with the same last value share one label; labels are pushed apart so they never overlap.
    merged = {}
    for value, city, day in end_labels:
        merged.setdefault(value, (day, []))[1].append(city)
    low, high = ax.get_ylim()
    min_gap = (high - low) * 0.045
    placed = []
    for value in sorted(merged):
        y = value if not placed or value - placed[-1] >= min_gap else placed[-1] + min_gap
        placed.append(y)
        day, cities = merged[value]
        label = " & ".join(cities) + (" (same grid point)" if len(cities) > 1 else "")
        ax.text(day, y, "  " + label, va="center", fontsize=9, color=TEXT, clip_on=False)

    ax.set_title("Daily maximum temperature by city", loc="left", fontsize=14,
                 fontweight="bold", color=TEXT, pad=22)
    ax.text(0, 1.02, "Source: Open-Meteo API, loaded daily by the Nile Weather pipeline",
            transform=ax.transAxes, fontsize=9, color=MUTED)
    ax.set_ylabel("°C", color=MUTED)
    ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=9)
    n_days = len(next(iter(series.values()))[0])
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, n_days // 7)))  # evenly spaced ticks
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax.legend(loc="upper left", bbox_to_anchor=(0, -0.1), ncol=len(series), frameon=False,
              fontsize=9, labelcolor=TEXT)
    ax.margins(x=0.02)
    fig.subplots_adjust(right=0.8, bottom=0.2)

    out = ROOT / "docs" / "daily_max_temp.png"
    out.parent.mkdir(exist_ok=True)
    fig.savefig(out, facecolor=SURFACE)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
