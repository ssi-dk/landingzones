#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Plot shared transfer TSV logs for dashboard-style review."""

import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


DEFAULT_WINDOWS = ("1d", "7d", "all")


def normalize_directory_suffix(value):
    """Return only the final directory component for labels."""
    text = str(value).strip() if value is not None else ""
    if not text or text == "nan":
        return ""
    text = text.rstrip("/")
    if ":" in text and "/" not in text.split(":", 1)[0]:
        text = text.split(":", 1)[1].rstrip("/")
    if "/" in text:
        return text.rsplit("/", 1)[-1]
    return text


def load_transfer_log(path):
    """Load and normalize a transfer TSV log."""
    df = pd.read_csv(path, sep="\t")
    if df.empty:
        return df
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d %H:%M:%S%z")
    for column in ("identifier", "directory", "source", "destination", "status"):
        if column in df.columns:
            df[column] = df[column].fillna("").astype(str).str.strip()
    df["directory_suffix"] = df["directory"].apply(normalize_directory_suffix)
    df = df.sort_values(["datetime", "identifier", "directory"]).reset_index(drop=True)
    return df


def parse_window_spec(window, anchor_time):
    """Return the start timestamp and human label for a window spec."""
    value = str(window).strip().lower()
    if value == "all":
        return None, "All data"
    if value.endswith("d") and value[:-1].isdigit():
        days = int(value[:-1])
        label = "Last {0} day".format(days) if days == 1 else "Last {0} days".format(days)
        return anchor_time - pd.Timedelta(days=days), label
    raise ValueError("Unsupported window: {0}".format(window))


def latest_completed_by_identifier(df, window, anchor_time=None):
    """Select the latest completed transfer per identifier within a time window."""
    if df.empty:
        return df.copy(), "All data"
    completed = df[df["status"] == "completed"].copy()
    if completed.empty:
        return completed, "All data"
    anchor = anchor_time or completed["datetime"].max()
    window_start, label = parse_window_spec(window, anchor)
    if window_start is not None:
        completed = completed[completed["datetime"] >= window_start]
    if completed.empty:
        return completed, label
    latest = (
        completed.sort_values(["datetime", "identifier"])
        .groupby("identifier", as_index=False)
        .tail(1)
        .sort_values(["datetime", "identifier"])
        .reset_index(drop=True)
    )
    return latest, label


def build_color_map(values):
    """Create a deterministic color map for directory suffixes."""
    categories = sorted({value for value in values if value})
    palette = plt.get_cmap("tab10")
    return {
        value: palette(index % 10)
        for index, value in enumerate(categories)
    }


def _set_time_axis(ax, data, anchor_time, window_start):
    """Set sensible time limits for a panel."""
    if data.empty:
        if window_start is not None:
            left = window_start
            right = anchor_time
        else:
            left = anchor_time - pd.Timedelta(minutes=30)
            right = anchor_time + pd.Timedelta(minutes=30)
    else:
        left = data["datetime"].min()
        right = data["datetime"].max()
        if window_start is not None:
            left = min(left, window_start)
            right = max(right, anchor_time)
        if left == right:
            left = left - pd.Timedelta(minutes=10)
            right = right + pd.Timedelta(minutes=10)
    ax.set_xlim(left.to_pydatetime(), right.to_pydatetime())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d\n%H:%M"))


def create_transfer_plot(df, output_path, windows=None, title=None):
    """Create a multi-panel plot showing latest completed directories."""
    if df.empty:
        raise ValueError("Transfer log is empty: no rows to plot")

    windows = tuple(windows or DEFAULT_WINDOWS)
    completed = df[df["status"] == "completed"].copy()
    if completed.empty:
        raise ValueError("Transfer log has no completed transfers to plot")

    anchor_time = completed["datetime"].max()
    latest_overall = completed.loc[completed["datetime"].idxmax()]
    color_map = build_color_map(completed["directory_suffix"].tolist())

    fig, axes = plt.subplots(
        len(windows),
        1,
        figsize=(14, 3.8 + 2.2 * len(windows)),
        constrained_layout=True,
    )
    if len(windows) == 1:
        axes = [axes]

    fig.suptitle(
        title or "Latest Completed Transfers by Window",
        fontsize=16,
        fontweight="bold",
        y=1.01,
    )
    fig.text(
        0.01,
        0.99,
        "Latest overall: {0} via {1} at {2}".format(
            latest_overall["directory_suffix"] or latest_overall["directory"],
            latest_overall["identifier"],
            latest_overall["datetime"].strftime("%Y-%m-%d %H:%M:%S%z"),
        ),
        ha="left",
        va="top",
        fontsize=10,
    )

    for ax, window in zip(axes, windows):
        latest, label = latest_completed_by_identifier(
            completed,
            window,
            anchor_time=anchor_time,
        )
        window_start, _ = parse_window_spec(window, anchor_time)

        ax.set_title(label, loc="left", fontsize=12, fontweight="bold")
        ax.grid(axis="x", color="#d9d9d9", linewidth=0.8)
        ax.set_axisbelow(True)

        if latest.empty:
            _set_time_axis(ax, latest, anchor_time, window_start)
            ax.set_yticks([])
            ax.text(
                0.5,
                0.5,
                "No completed transfers in this window",
                ha="center",
                va="center",
                transform=ax.transAxes,
                fontsize=11,
                color="#666666",
            )
            continue

        identifiers = latest["identifier"].tolist()
        y_positions = list(range(len(identifiers)))
        ax.set_yticks(y_positions)
        ax.set_yticklabels(identifiers)

        for y_pos, (_, row) in zip(y_positions, latest.iterrows()):
            suffix = row["directory_suffix"] or row["directory"]
            color = color_map.get(suffix, "#1f77b4")
            ax.scatter(row["datetime"], y_pos, s=90, color=color, zorder=3)
            ax.annotate(
                suffix,
                (row["datetime"], y_pos),
                xytext=(8, 0),
                textcoords="offset points",
                va="center",
                fontsize=9,
                color="#222222",
            )

        _set_time_axis(ax, latest, anchor_time, window_start)
        ax.set_ylabel("Route")

    axes[-1].set_xlabel("Completed at")
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def build_default_output_path(input_path):
    """Return the default output path for a plot."""
    stem, _ = os.path.splitext(input_path)
    return stem + ".latest_by_window.png"


def main():
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Plot latest completed directories from a shared transfer TSV log",
    )
    parser.add_argument("input", help="Path to the shared transfer TSV log")
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output PNG path (default: <input>.latest_by_window.png)",
    )
    parser.add_argument(
        "--windows",
        nargs="+",
        default=list(DEFAULT_WINDOWS),
        help="Window list, for example: 1d 7d all",
    )
    parser.add_argument(
        "--title",
        default=None,
        help="Optional plot title override",
    )
    args = parser.parse_args()

    output_path = args.output or build_default_output_path(args.input)
    df = load_transfer_log(args.input)
    create_transfer_plot(df, output_path, windows=args.windows, title=args.title)
    print("Wrote plot to {0}".format(output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
