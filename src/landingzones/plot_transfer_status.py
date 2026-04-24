#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Generate a transfer health dashboard from shared TSV logs."""

import argparse
import html
import os
import re
import sys

try:
    import pandas as pd
except ModuleNotFoundError:
    pd = None

from landingzones.config import config
from landingzones.transfer_loading import (
    definitions_for_system,
    load_reporting_transfers,
)
from landingzones.transfer_definitions import (
    normalize_tags,
    normalize_tags_text,
    normalize_transfer_path,
    tags_match_any,
)


WINDOW_SPECS = (
    ("1d", "Last day"),
    ("7d", "Last 7 days"),
    ("30d", "Last 30 days"),
)
RUN_LIST_WINDOW = "7d"


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


def require_pandas():
    """Return False and print a clear message when report dependencies are missing."""
    if pd is not None:
        return True
    print(
        "Report generation was skipped because pandas is not installed.\n"
        "Install the reporting extra with `pip install 'landingzones[report]'` "
        "or refresh the local Pixi environment, then rerun `landingzones report transfers`.",
        file=sys.stderr,
    )
    return False


def parse_window_spec(window, anchor_time):
    """Return the start timestamp and display label for a window."""
    value = str(window).strip().lower()
    if value == "all":
        return None, "All data"
    if value.endswith("d") and value[:-1].isdigit():
        days = int(value[:-1])
        label = "Last day" if days == 1 else "Last {0} days".format(days)
        return anchor_time - pd.Timedelta(days=days), label
    raise ValueError("Unsupported window: {0}".format(window))


def load_transfer_log(path):
    """Load and normalize a transfer TSV log."""
    df = pd.read_csv(path, sep="\t")
    if df.empty:
        return df
    df = df.copy()
    column_aliases = {
        "event_time_utc": "datetime",
        "transfer_identifier": "identifier",
        "source_path": "source",
        "destination_path": "destination",
    }
    for source_column, target_column in column_aliases.items():
        if source_column in df.columns and target_column not in df.columns:
            df[target_column] = df[source_column]
    if "directory" not in df.columns and "run_name" in df.columns:
        df["directory"] = df["run_name"]

    df["datetime"] = pd.to_datetime(df["datetime"])
    for column in ("identifier", "directory", "source", "destination", "status", "tags"):
        if column in df.columns:
            df[column] = df[column].fillna("").astype(str).str.strip()
    if "tags" in df.columns:
        df["tags"] = df["tags"].apply(normalize_tags_text)
    else:
        df["tags"] = ""
    if "run_id" in df.columns:
        df["run_id"] = df["run_id"].fillna("").astype(str).str.strip()
    else:
        df["run_id"] = ""
    if "run_name" in df.columns:
        df["run_name"] = df["run_name"].fillna("").astype(str).str.strip()
    else:
        df["run_name"] = df["directory"]
    df["directory_suffix"] = df["directory"].apply(normalize_directory_suffix)
    df["run_group"] = df["run_id"]
    empty_run_group = df["run_group"] == ""
    df.loc[empty_run_group, "run_group"] = df.loc[empty_run_group, "directory_suffix"]
    df = df.sort_values(["datetime", "identifier", "directory"]).reset_index(drop=True)
    return df


def load_transfer_metadata(transfers_file):
    """Load shared transfer definitions through the main parser pipeline."""
    return load_reporting_transfers(transfers_file=transfers_file)


def infer_system_from_log_path(path):
    """Infer the system name from a shared transfer TSV log file name."""
    match = re.search(r"Landing_Zone_(.+)\.transfers\.tsv$", os.path.basename(path))
    if not match:
        raise ValueError(
            "Could not infer system from log file name: {0}".format(path)
        )
    return match.group(1)


def build_flow_graph(transfers_df, system):
    """Return transfer metadata and terminal identifiers for a system."""
    system_df = transfers_df[transfers_df["system"] == system].copy()
    if system_df.empty:
        raise ValueError(
            "No enabled transfers found for system '{0}'".format(system)
        )
    system_definitions = definitions_for_system(transfers_df, system)
    edges = {
        definition.identifier: set() for definition in system_definitions
    }
    for upstream in system_definitions:
        upstream_destination = normalize_transfer_path(
            upstream.destination,
            strip_wildcard=False,
        )
        if not upstream_destination:
            continue
        for downstream in system_definitions:
            if upstream.identifier == downstream.identifier:
                continue
            downstream_source = normalize_transfer_path(
                downstream.source,
                strip_wildcard=True,
            )
            if upstream_destination == downstream_source:
                edges[upstream.identifier].add(downstream.identifier)

    terminal_identifiers = sorted(
        identifier for identifier, children in edges.items() if not children
    )
    return system_df, edges, terminal_identifiers


def format_timedelta(delta):
    """Render a timedelta as a short human-readable string."""
    if pd.isna(delta):
        return ""
    total_seconds = max(int(delta.total_seconds()), 0)
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, _ = divmod(remainder, 60)
    parts = []
    if days:
        parts.append("{0}d".format(days))
    if hours or days:
        parts.append("{0}h".format(hours))
    parts.append("{0}m".format(minutes))
    return " ".join(parts)


def describe_state_logic(state, warning_hours):
    """Return a human-readable explanation of the dashboard state heuristic."""
    threshold_text = "{0:g}h".format(warning_hours)
    descriptions = {
        "success": (
            "Success: a completed event exists on a terminal transfer and is "
            "newer than any error event."
        ),
        "failed": "Failed: the most recent decisive event is an error event.",
        "warning": (
            "Warning: no newer terminal success or error exists, and the latest "
            "initiated event is older than the {0} threshold."
        ).format(threshold_text),
        "in_progress": (
            "In progress: no newer terminal success or error exists, and the "
            "latest initiated event is within the {0} threshold."
        ).format(threshold_text),
    }
    return descriptions.get(
        state,
        "State is derived from recent initiated, completed, and error events.",
    )


def render_status_badge(state, warning_hours):
    """Render a status badge with hover text describing the classification."""
    return (
        '<span class="status {state}" title="{title}">{label}</span>'.format(
            state=html.escape(str(state)),
            title=html.escape(describe_state_logic(state, warning_hours)),
            label=html.escape(str(state).replace("_", " ")),
        )
    )


def render_metric_label(label, warning_hours):
    """Render a metric label, including hover text for state-based metrics."""
    normalized = str(label).strip().lower().replace(" ", "_")
    if normalized in {"success", "failed", "warning", "in_progress"}:
        return (
            '<span title="{0}">{1}</span>'.format(
                html.escape(describe_state_logic(normalized, warning_hours)),
                html.escape(label),
            )
        )
    return "<span>{0}</span>".format(html.escape(label))


def _select_state_row(rows, terminal_identifiers, latest_start, anchor_time, warning_hours):
    """Determine the state and representative row for a run."""
    recent_rows = rows[rows["datetime"] >= latest_start].copy()
    latest_row = recent_rows.sort_values("datetime").iloc[-1]
    terminal_rows = recent_rows[
        (recent_rows["identifier"].isin(terminal_identifiers))
        & (recent_rows["status"] == "completed")
    ].sort_values("datetime")
    error_rows = recent_rows[recent_rows["status"] == "error"].sort_values("datetime")

    success_row = terminal_rows.iloc[-1] if not terminal_rows.empty else None
    error_row = error_rows.iloc[-1] if not error_rows.empty else None

    if success_row is not None and (
        error_row is None or success_row["datetime"] > error_row["datetime"]
    ):
        state = "success"
        state_row = success_row
    elif error_row is not None:
        state = "failed"
        state_row = error_row
    else:
        elapsed = anchor_time - latest_start
        state = "warning" if elapsed > pd.Timedelta(hours=warning_hours) else "in_progress"
        state_row = latest_row
    return state, state_row, latest_row


def aggregate_runs(log_df, terminal_identifiers, anchor_time=None, warning_hours=2):
    """Aggregate event rows into run-level health records."""
    if log_df.empty:
        return pd.DataFrame()
    log_df = log_df.copy()
    if "run_id" not in log_df.columns:
        log_df["run_id"] = ""
    if "run_name" not in log_df.columns:
        log_df["run_name"] = log_df.get("directory", "")
    if "directory_suffix" not in log_df.columns:
        log_df["directory_suffix"] = log_df["directory"].apply(normalize_directory_suffix)
    if "run_group" not in log_df.columns:
        log_df["run_group"] = log_df["run_id"].fillna("").astype(str).str.strip()
        empty_run_group = log_df["run_group"] == ""
        log_df.loc[empty_run_group, "run_group"] = log_df.loc[
            empty_run_group, "directory_suffix"
        ]
    anchor = anchor_time or log_df["datetime"].max()
    records = []

    for run_group, run_rows in log_df.groupby("run_group", sort=False):
        rows = run_rows.sort_values("datetime").reset_index(drop=True)
        initiated_rows = rows[rows["status"] == "initiated"].sort_values("datetime")
        started_at = (
            initiated_rows.iloc[0]["datetime"]
            if not initiated_rows.empty
            else rows.iloc[0]["datetime"]
        )
        latest_start = (
            initiated_rows.iloc[-1]["datetime"]
            if not initiated_rows.empty
            else rows.iloc[0]["datetime"]
        )
        state, state_row, latest_row = _select_state_row(
            rows,
            terminal_identifiers,
            latest_start,
            anchor,
            warning_hours,
        )
        records.append(
            {
                "run": rows.iloc[-1].get("run_name") or rows.iloc[-1]["directory_suffix"],
                "run_group": run_group,
                "run_id": rows.iloc[-1].get("run_id", ""),
                "tags": normalize_tags_text(rows["tags"].tolist()),
                "started_at": started_at,
                "latest_start": latest_start,
                "last_event_time": latest_row["datetime"],
                "last_identifier": latest_row["identifier"],
                "last_status": latest_row["status"],
                "state": state,
                "state_time": state_row["datetime"],
                "state_identifier": state_row["identifier"],
                "state_source": state_row["source"],
                "state_destination": state_row["destination"],
                "source": latest_row["source"],
                "destination": latest_row["destination"],
                "age": anchor - latest_start,
                "event_count": len(rows),
            }
        )

    runs_df = pd.DataFrame(records)
    if runs_df.empty:
        return runs_df
    return runs_df.sort_values(["started_at", "run"]).reset_index(drop=True)


def windowed_runs(runs_df, window, anchor_time):
    """Filter runs to those that started within a given window."""
    if runs_df.empty:
        return runs_df.copy(), "All data"
    window_start, label = parse_window_spec(window, anchor_time)
    if window_start is None:
        return runs_df.copy(), label
    filtered = runs_df[runs_df["started_at"] >= window_start].copy()
    return filtered, label


def filter_runs_by_tags(runs_df, requested_tags):
    """Return only runs matching any requested tag."""
    if runs_df.empty or not requested_tags:
        return runs_df.copy()
    return runs_df[runs_df["tags"].apply(
        lambda value: tags_match_any(value, requested_tags)
    )].copy()


def build_tag_summary(runs_df):
    """Return tag counts across filtered runs."""
    counts = {}
    for value in runs_df.get("tags", []):
        for tag in normalize_tags(value):
            counts[tag] = counts.get(tag, 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


def build_metric_cards(runs_df, anchor_time):
    """Summarize run health in multiple time windows."""
    cards = []
    for window, label in WINDOW_SPECS:
        filtered, _ = windowed_runs(runs_df, window, anchor_time)
        counts = filtered["state"].value_counts().to_dict() if not filtered.empty else {}
        cards.append(
            {
                "label": label,
                "total": len(filtered),
                "success": counts.get("success", 0),
                "failed": counts.get("failed", 0),
                "warning": counts.get("warning", 0),
                "in_progress": counts.get("in_progress", 0),
            }
        )
    return cards


def select_run_list(runs_df, statuses, window, anchor_time, max_runs, sort_column):
    """Return a capped list of runs plus overflow count."""
    filtered, label = windowed_runs(runs_df, window, anchor_time)
    filtered = filtered[filtered["state"].isin(statuses)].copy()
    if filtered.empty:
        return filtered, 0, label
    filtered = filtered.sort_values(sort_column, ascending=False).reset_index(drop=True)
    overflow = max(len(filtered) - max_runs, 0)
    return filtered.head(max_runs), overflow, label


def dashboard_context(
    log_df,
    transfers_df,
    system,
    warning_hours=2,
    max_runs=10,
    filter_tags=None,
):
    """Build all data needed to render the HTML dashboard."""
    if log_df.empty:
        raise ValueError("Transfer log is empty: no rows to report")
    _, _, terminal_identifiers = build_flow_graph(transfers_df, system)
    anchor_time = log_df["datetime"].max()
    runs_df = aggregate_runs(
        log_df,
        terminal_identifiers=terminal_identifiers,
        anchor_time=anchor_time,
        warning_hours=warning_hours,
    )
    filtered_runs_df = filter_runs_by_tags(runs_df, filter_tags or [])
    metric_cards = build_metric_cards(filtered_runs_df, anchor_time)
    unfinished_runs, unfinished_overflow, unfinished_label = select_run_list(
        filtered_runs_df,
        statuses=("failed", "warning", "in_progress"),
        window=RUN_LIST_WINDOW,
        anchor_time=anchor_time,
        max_runs=max_runs,
        sort_column="last_event_time",
    )
    success_runs, success_overflow, success_label = select_run_list(
        filtered_runs_df,
        statuses=("success",),
        window=RUN_LIST_WINDOW,
        anchor_time=anchor_time,
        max_runs=max_runs,
        sort_column="state_time",
    )
    return {
        "anchor_time": anchor_time,
        "runs_df": filtered_runs_df,
        "metric_cards": metric_cards,
        "unfinished_runs": unfinished_runs,
        "unfinished_overflow": unfinished_overflow,
        "unfinished_label": unfinished_label,
        "success_runs": success_runs,
        "success_overflow": success_overflow,
        "success_label": success_label,
        "terminal_identifiers": terminal_identifiers,
        "system": system,
        "warning_hours": warning_hours,
        "filter_tags": normalize_tags_text(filter_tags or []),
        "tag_summary": build_tag_summary(filtered_runs_df),
    }


def render_run_table(rows_df, empty_message, warning_hours):
    """Render a run table as HTML."""
    if rows_df.empty:
        return '<p class="empty">{0}</p>'.format(html.escape(empty_message))

    header = (
        "<tr>"
        "<th>Run</th><th>Status</th><th>Last identifier</th><th>Last event</th>"
        "<th>Age</th><th>Tags</th><th>Source</th><th>Destination</th>"
        "</tr>"
    )
    body_rows = []
    for _, row in rows_df.iterrows():
        body_rows.append(
            "<tr>"
            "<td>{run}</td>"
            "<td>{status_badge}</td>"
            "<td>{identifier}</td>"
            "<td>{last_event}</td>"
            "<td>{age}</td>"
            "<td>{tags}</td>"
            "<td class=\"path\">{source}</td>"
            "<td class=\"path\">{destination}</td>"
            "</tr>".format(
                run=html.escape(str(row["run"])),
                status_badge=render_status_badge(row["state"], warning_hours),
                identifier=html.escape(str(row["last_identifier"])),
                last_event=html.escape(
                    row["last_event_time"].strftime("%Y-%m-%d %H:%M:%S%z")
                ),
                age=html.escape(format_timedelta(row["age"])),
                tags=html.escape(str(row.get("tags", "") or "(none)")),
                source=html.escape(str(row["source"])),
                destination=html.escape(str(row["destination"])),
            )
        )
    return "<table><thead>{0}</thead><tbody>{1}</tbody></table>".format(
        header,
        "".join(body_rows),
    )


def render_dashboard(context, output_path, title=None):
    """Render a self-contained HTML dashboard."""
    metric_cards = []
    for card in context["metric_cards"]:
        metric_cards.append(
            """
            <section class="metric-card">
              <h3>{label}</h3>
              <div class="metric-grid">
                <div>{total_label}<strong>{total}</strong></div>
                <div>{success_label}<strong>{success}</strong></div>
                <div>{failed_label}<strong>{failed}</strong></div>
                <div>{warning_label}<strong>{warning}</strong></div>
                <div>{in_progress_label}<strong>{in_progress}</strong></div>
              </div>
            </section>
            """.format(
                label=html.escape(card["label"]),
                total_label=render_metric_label("Total", context["warning_hours"]),
                total=card["total"],
                success_label=render_metric_label("Success", context["warning_hours"]),
                success=card["success"],
                failed_label=render_metric_label("Failed", context["warning_hours"]),
                failed=card["failed"],
                warning_label=render_metric_label("Warning", context["warning_hours"]),
                warning=card["warning"],
                in_progress_label=render_metric_label(
                    "In progress",
                    context["warning_hours"],
                ),
                in_progress=card["in_progress"],
            )
        )

    unfinished_more = ""
    if context["unfinished_overflow"] > 0:
        unfinished_more = (
            '<p class="overflow">Showing 10 most recent unfinished runs; '
            'and {0} more.</p>'.format(context["unfinished_overflow"])
        )

    success_more = ""
    if context["success_overflow"] > 0:
        success_more = (
            '<p class="overflow">Showing 10 most recent successes; '
            'and {0} more.</p>'.format(context["success_overflow"])
        )

    tag_summary = ""
    if context["tag_summary"]:
        tag_rows = "".join(
            "<tr><td>{0}</td><td>{1}</td></tr>".format(
                html.escape(tag),
                count,
            )
            for tag, count in context["tag_summary"]
        )
        tag_summary = (
            '<section class="section">'
            '<h2>Tag Summary</h2>'
            '<table><thead><tr><th>Tag</th><th>Runs</th></tr></thead>'
            '<tbody>{0}</tbody></table>'
            '</section>'.format(tag_rows)
        )
    else:
        tag_summary = (
            '<section class="section"><h2>Tag Summary</h2>'
            '<p class="empty">No tags present in the filtered runs.</p></section>'
        )

    document = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    :root {{
      --bg: #f4f1ea;
      --panel: #fffdf8;
      --ink: #1f2430;
      --muted: #69707d;
      --line: #ddd4c6;
      --success: #227c5d;
      --failed: #b42318;
      --warning: #b7791f;
      --in-progress: #2563eb;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Iowan Old Style", "Palatino Linotype", serif;
      background: linear-gradient(180deg, #f0ece3 0%, #faf8f3 100%);
      color: var(--ink);
    }}
    .dashboard {{
      max-width: 1360px;
      margin: 0 auto;
      padding: 28px;
    }}
    .hero {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 24px;
      box-shadow: 0 16px 40px rgba(37, 42, 52, 0.08);
    }}
    .hero h1 {{
      margin: 0 0 6px;
      font-size: 2rem;
    }}
    .hero p {{
      margin: 0;
      color: var(--muted);
      line-height: 1.5;
    }}
    .meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 14px;
      margin-top: 14px;
      color: var(--muted);
      font-size: 0.95rem;
    }}
    .section {{
      margin-top: 22px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 20px;
      box-shadow: 0 12px 32px rgba(37, 42, 52, 0.05);
    }}
    .section h2 {{
      margin: 0 0 14px;
      font-size: 1.25rem;
    }}
    .metric-row {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
    }}
    .metric-card {{
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 16px;
      background: linear-gradient(180deg, #fffcf7 0%, #f8f4eb 100%);
    }}
    .metric-card h3 {{
      margin: 0 0 12px;
      font-size: 1rem;
    }}
    .metric-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px 16px;
    }}
    .metric-grid div {{
      padding-top: 8px;
      border-top: 1px solid rgba(221, 212, 198, 0.7);
    }}
    .metric-grid span {{
      display: block;
      color: var(--muted);
      font-size: 0.85rem;
    }}
    .metric-grid strong {{
      font-size: 1.35rem;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }}
    th, td {{
      text-align: left;
      vertical-align: top;
      padding: 10px 12px;
      border-top: 1px solid var(--line);
      font-size: 0.95rem;
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
      border-top: none;
      padding-top: 0;
    }}
    td.path {{
      word-break: break-word;
      font-family: "SFMono-Regular", Menlo, Consolas, monospace;
      font-size: 0.85rem;
    }}
    .status {{
      display: inline-block;
      padding: 3px 9px;
      border-radius: 999px;
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      font-weight: 700;
    }}
    .status.success {{ background: rgba(34, 124, 93, 0.14); color: var(--success); }}
    .status.failed {{ background: rgba(180, 35, 24, 0.12); color: var(--failed); }}
    .status.warning {{ background: rgba(183, 121, 31, 0.14); color: var(--warning); }}
    .status.in_progress {{ background: rgba(37, 99, 235, 0.12); color: var(--in-progress); }}
    .empty, .overflow {{
      margin: 10px 0 0;
      color: var(--muted);
    }}
    @media (max-width: 900px) {{
      .dashboard {{ padding: 14px; }}
      .section, .hero {{ padding: 16px; }}
      table, thead, tbody, th, td, tr {{ display: block; }}
      thead {{ display: none; }}
      tr {{
        border-top: 1px solid var(--line);
        padding: 10px 0;
      }}
      td {{
        border-top: none;
        padding: 6px 0;
      }}
      td::before {{
        content: attr(data-label);
        display: block;
        color: var(--muted);
        font-size: 0.8rem;
        margin-bottom: 2px;
      }}
    }}
  </style>
</head>
<body>
  <main class="dashboard">
    <section class="hero">
      <h1>{heading}</h1>
      <p>{summary}</p>
      <div class="meta">
        <span>Anchor time: {anchor_time}</span>
        <span>System: {system}</span>
        <span>Terminal identifiers: {terminal_ids}</span>
        <span>Warning threshold: {warning_hours}h</span>
        <span>Tag filter: {filter_tags}</span>
        <span>Hover a status label for classification logic</span>
      </div>
    </section>
    <section class="section">
      <h2>Transfer Volume and Health</h2>
      <div class="metric-row">
        {metric_cards}
      </div>
    </section>
    {tag_summary}
    <section class="section">
      <h2>Unfinished Runs ({unfinished_label})</h2>
      {unfinished_table}
      {unfinished_more}
    </section>
    <section class="section">
      <h2>Recent Successes ({success_label})</h2>
      {success_table}
      {success_more}
    </section>
  </main>
</body>
</html>
""".format(
        title=html.escape(title or "Transfer Health Dashboard"),
        heading=html.escape(title or "Transfer Health Dashboard"),
        summary=html.escape(
            "Assess transfer flow health at a glance. Unfinished runs combine "
            "failed, warning, and in-progress states."
        ),
        anchor_time=html.escape(
            context["anchor_time"].strftime("%Y-%m-%d %H:%M:%S%z")
        ),
        system=html.escape(context["system"]),
        terminal_ids=html.escape(", ".join(context["terminal_identifiers"]) or "(none)"),
        warning_hours=context["warning_hours"],
        filter_tags=html.escape(context["filter_tags"] or "(none)"),
        metric_cards="".join(metric_cards),
        tag_summary=tag_summary,
        unfinished_label=html.escape(context["unfinished_label"]),
        unfinished_table=render_run_table(
            context["unfinished_runs"],
            "No unfinished runs in this window.",
            context["warning_hours"],
        ),
        unfinished_more=unfinished_more,
        success_label=html.escape(context["success_label"]),
        success_table=render_run_table(
            context["success_runs"],
            "No successful runs in this window.",
            context["warning_hours"],
        ),
        success_more=success_more,
    )

    with open(output_path, "w") as handle:
        handle.write(document)
    return output_path


def create_transfer_dashboard(
    log_df,
    transfers_df,
    system,
    output_path,
    warning_hours=2,
    max_runs=10,
    title=None,
    filter_tags=None,
):
    """Create a self-contained HTML dashboard."""
    context = dashboard_context(
        log_df,
        transfers_df,
        system=system,
        warning_hours=warning_hours,
        max_runs=max_runs,
        filter_tags=filter_tags,
    )
    return render_dashboard(context, output_path, title=title)


def create_transfer_plot(*args, **kwargs):
    """Backward-compatible alias for dashboard generation."""
    return create_transfer_dashboard(*args, **kwargs)


def build_default_output_path(input_path):
    """Return the default output path for the dashboard."""
    stem, _ = os.path.splitext(input_path)
    return stem + ".health_dashboard.html"


def load_transfers_for_reporting(config_file=None, transfers_file=None):
    """Load enabled transfers after resolving config defaults."""
    return load_reporting_transfers(
        config_file=config_file,
        transfers_file=transfers_file,
    )


def resolve_report_input_path(input_path=None, config_file=None, transfers_file=None):
    """Resolve the default report TSV path from CLI input or config."""
    if input_path:
        return input_path
    config.load_config(config_file=config_file, transfers_file=transfers_file)
    return config.report_transfer_log_file


def main(argv=None):
    """CLI entrypoint."""
    if not require_pandas():
        return 2
    parser = argparse.ArgumentParser(
        description="Generate a transfer health dashboard from a shared TSV log",
    )
    parser.add_argument(
        "input",
        nargs="?",
        default=None,
        help="Path to the transfer TSV report input (defaults to report_transfer_log_file from config)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output HTML path (default: <input>.health_dashboard.html)",
    )
    parser.add_argument(
        "--config",
        "-c",
        default=None,
        help="Optional config.yaml path used to resolve transfers.tsv",
    )
    parser.add_argument(
        "--transfers-file",
        "-t",
        default=None,
        help="Optional transfers.tsv path overriding config",
    )
    parser.add_argument(
        "--system",
        default=None,
        help="System name; inferred from the log filename when omitted",
    )
    parser.add_argument(
        "--warning-hours",
        type=float,
        default=2,
        help="Hours before an unfinished run is marked warning (default: 2)",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=10,
        help="Maximum rows to show in unfinished/success lists (default: 10)",
    )
    parser.add_argument(
        "--title",
        default=None,
        help="Optional dashboard title override",
    )
    parser.add_argument(
        "--tag",
        action="append",
        default=[],
        help="Filter runs to those matching any requested tag; repeatable",
    )
    args = parser.parse_args(argv)

    input_path = resolve_report_input_path(
        input_path=args.input,
        config_file=args.config,
        transfers_file=args.transfers_file,
    )
    if not input_path:
        parser.error(
            "missing transfer log path: pass INPUT or set report_transfer_log_file in config"
        )

    system = args.system or infer_system_from_log_path(input_path)
    output_path = args.output or build_default_output_path(input_path)
    log_df = load_transfer_log(input_path)
    transfers_df = load_transfers_for_reporting(
        config_file=args.config,
        transfers_file=args.transfers_file,
    )
    create_transfer_dashboard(
        log_df,
        transfers_df,
        system=system,
        output_path=output_path,
        warning_hours=args.warning_hours,
        max_runs=args.max_runs,
        title=args.title,
        filter_tags=args.tag,
    )
    print("Wrote dashboard to {0}".format(output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
