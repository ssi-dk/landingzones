#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Generate a transfer health dashboard from shared TSV logs."""

import argparse
import html
import os
import re

import pandas as pd

from landingzones.config import config


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


def normalize_transfer_path(path, strip_wildcard=False):
    """Normalize a source or destination path for graph matching."""
    value = str(path).strip() if path is not None else ""
    if not value or value == "nan":
        return ""
    remote = ""
    inner = value
    if ":" in value:
        remote, inner = value.split(":", 1)
    if strip_wildcard and inner.endswith("/*"):
        inner = inner[:-2]
    elif strip_wildcard and inner.endswith("*"):
        inner = inner[:-1]
    inner = inner.rstrip("/")
    if remote:
        return "{0}:{1}".format(remote, inner)
    return inner


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
    df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d %H:%M:%S%z")
    for column in ("identifier", "directory", "source", "destination", "status"):
        if column in df.columns:
            df[column] = df[column].fillna("").astype(str).str.strip()
    df["directory_suffix"] = df["directory"].apply(normalize_directory_suffix)
    df = df.sort_values(["datetime", "identifier", "directory"]).reset_index(drop=True)
    return df


def load_transfer_metadata(transfers_file):
    """Load transfer definitions needed to infer the flow graph."""
    df = pd.read_csv(transfers_file, sep="\t")
    df = df.copy()
    if "system" in df.columns:
        df = df[~df["system"].astype(str).str.startswith("#")]
    if "enabled" in df.columns:
        df["enabled"] = df["enabled"].fillna("").astype(str).str.strip().str.upper()
        df = df[df["enabled"] == "TRUE"]
    for column in ("identifiers", "system", "source", "destination"):
        if column in df.columns:
            df[column] = df[column].fillna("").astype(str).str.strip()
    return df


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
            "No enabled transfers found for system '{0}' in {1}".format(
                system,
                config.transfers_file,
            )
        )
    system_df["source_root"] = system_df["source"].apply(
        lambda value: normalize_transfer_path(value, strip_wildcard=True)
    )
    system_df["destination_root"] = system_df["destination"].apply(
        lambda value: normalize_transfer_path(value, strip_wildcard=False)
    )

    edges = {identifier: set() for identifier in system_df["identifiers"]}
    for _, upstream in system_df.iterrows():
        for _, downstream in system_df.iterrows():
            if upstream["identifiers"] == downstream["identifiers"]:
                continue
            if upstream["destination_root"] and upstream["destination_root"] == downstream["source_root"]:
                edges[upstream["identifiers"]].add(downstream["identifiers"])

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
    anchor = anchor_time or log_df["datetime"].max()
    records = []

    for directory_suffix, run_rows in log_df.groupby("directory_suffix", sort=False):
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
                "run": directory_suffix,
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


def dashboard_context(log_df, transfers_df, system, warning_hours=2, max_runs=10):
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
    metric_cards = build_metric_cards(runs_df, anchor_time)
    unfinished_runs, unfinished_overflow, unfinished_label = select_run_list(
        runs_df,
        statuses=("failed", "warning", "in_progress"),
        window=RUN_LIST_WINDOW,
        anchor_time=anchor_time,
        max_runs=max_runs,
        sort_column="last_event_time",
    )
    success_runs, success_overflow, success_label = select_run_list(
        runs_df,
        statuses=("success",),
        window=RUN_LIST_WINDOW,
        anchor_time=anchor_time,
        max_runs=max_runs,
        sort_column="state_time",
    )
    return {
        "anchor_time": anchor_time,
        "runs_df": runs_df,
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
    }


def render_run_table(rows_df, empty_message):
    """Render a run table as HTML."""
    if rows_df.empty:
        return '<p class="empty">{0}</p>'.format(html.escape(empty_message))

    header = (
        "<tr>"
        "<th>Run</th><th>Status</th><th>Last identifier</th><th>Last event</th>"
        "<th>Age</th><th>Source</th><th>Destination</th>"
        "</tr>"
    )
    body_rows = []
    for _, row in rows_df.iterrows():
        body_rows.append(
            "<tr>"
            "<td>{run}</td>"
            "<td><span class=\"status {state}\">{state_label}</span></td>"
            "<td>{identifier}</td>"
            "<td>{last_event}</td>"
            "<td>{age}</td>"
            "<td class=\"path\">{source}</td>"
            "<td class=\"path\">{destination}</td>"
            "</tr>".format(
                run=html.escape(str(row["run"])),
                state=html.escape(str(row["state"])),
                state_label=html.escape(str(row["state"]).replace("_", " ")),
                identifier=html.escape(str(row["last_identifier"])),
                last_event=html.escape(
                    row["last_event_time"].strftime("%Y-%m-%d %H:%M:%S%z")
                ),
                age=html.escape(format_timedelta(row["age"])),
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
                <div><span>Total</span><strong>{total}</strong></div>
                <div><span>Success</span><strong>{success}</strong></div>
                <div><span>Failed</span><strong>{failed}</strong></div>
                <div><span>Warning</span><strong>{warning}</strong></div>
                <div><span>In progress</span><strong>{in_progress}</strong></div>
              </div>
            </section>
            """.format(
                label=html.escape(card["label"]),
                total=card["total"],
                success=card["success"],
                failed=card["failed"],
                warning=card["warning"],
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
      </div>
    </section>
    <section class="section">
      <h2>Transfer Volume and Health</h2>
      <div class="metric-row">
        {metric_cards}
      </div>
    </section>
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
        metric_cards="".join(metric_cards),
        unfinished_label=html.escape(context["unfinished_label"]),
        unfinished_table=render_run_table(
            context["unfinished_runs"],
            "No unfinished runs in this window.",
        ),
        unfinished_more=unfinished_more,
        success_label=html.escape(context["success_label"]),
        success_table=render_run_table(
            context["success_runs"],
            "No successful runs in this window.",
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
):
    """Create a self-contained HTML dashboard."""
    context = dashboard_context(
        log_df,
        transfers_df,
        system=system,
        warning_hours=warning_hours,
        max_runs=max_runs,
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
    config.load_config(config_file=config_file, transfers_file=transfers_file)
    return load_transfer_metadata(config.transfers_file)


def main():
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Generate a transfer health dashboard from a shared TSV log",
    )
    parser.add_argument("input", help="Path to the shared transfer TSV log")
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
    args = parser.parse_args()

    system = args.system or infer_system_from_log_path(args.input)
    output_path = args.output or build_default_output_path(args.input)
    log_df = load_transfer_log(args.input)
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
    )
    print("Wrote dashboard to {0}".format(output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
