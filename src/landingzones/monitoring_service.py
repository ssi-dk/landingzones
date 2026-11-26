#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Live HTTP and JSON views over the Transfer Event monitoring database."""

from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import math
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs, parse_qsl, unquote, urlencode, urlsplit

from landingzones.scheduler_status import read_scheduler_status
from landingzones.monitoring import _message_fields, ingest_event_spool, query_run_detail, query_run_summaries


FILTER_ARGUMENTS = {
    "runtime_id": "runtime_ids",
    "system": "systems",
    "execution_user": "execution_users",
    "transfer_identifier": "transfer_identifiers",
    "tag": "tags",
    "state": "states",
    "reason_code": "reason_codes",
}
AUTO_REFRESH_SECONDS = 60


def _query_filters(query_string):
    values = parse_qs(query_string, keep_blank_values=False)
    return {
        argument_name: values.get(query_name)
        for query_name, argument_name in FILTER_ARGUMENTS.items()
        if values.get(query_name)
    }


def _show_without_directory(query_string):
    """Return whether the report should include route-only observations."""
    values = parse_qs(query_string, keep_blank_values=False)
    return values.get("show_without_directory", [""])[-1].lower() in (
        "1",
        "true",
        "yes",
    )


def _toggle_url(query_string, show_without_directory):
    """Return a report URL that preserves filters while changing visibility."""
    parameters = [
        (name, value)
        for name, value in parse_qsl(query_string, keep_blank_values=True)
        if name != "show_without_directory"
    ]
    if show_without_directory:
        parameters.append(("show_without_directory", "1"))
    encoded = urlencode(parameters)
    return "/?{0}".format(encoded) if encoded else "/"


def _queried_at_utc():
    """Return the current report-query timestamp in the event format."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _json_response(value, status=200):
    return (
        status,
        {"Content-Type": "application/json; charset=utf-8"},
        json.dumps(value, sort_keys=True),
    )


def _format_age(age_seconds):
    if age_seconds is None:
        return ""
    if age_seconds < 60:
        return "{0}s".format(age_seconds)
    if age_seconds < 3600:
        return "{0}m".format(age_seconds // 60)
    if age_seconds < 86400:
        return "{0}h".format(age_seconds // 3600)
    return "{0}d".format(age_seconds // 86400)


REPORT_CSS = """
:root { color-scheme:light; --ink:#182d31; --sub:#4e666b; --line:#cedad8;
 --paper:#fbfcfa; --panel:#fff; --teal:#09634e; --pale:#dff3e9; --amber:#825917; --red:#993c30; }
* { box-sizing:border-box; }
body { margin:0; background:var(--paper); color:var(--ink); font:15px/1.45 Inter,system-ui,sans-serif; }
a { color:#075e50; text-underline-offset:3px; }
a:hover { color:#023e35; }
a:focus-visible,button:focus-visible,input:focus-visible { outline:3px solid #c08816; outline-offset:3px; }
header { background:#163b39; color:#f3faf5; }
.topbar,main { max-width:1480px; margin:auto; padding:0 clamp(14px,3.3vw,58px); }
.topbar { min-height:50px; display:flex; align-items:center; justify-content:space-between; gap:1rem; }
.brand { color:inherit; font-weight:740; text-decoration:none; letter-spacing:.015em; }
.topbar nav { display:flex; gap:1.1rem; } .topbar nav a { color:#e9f5ee; font-size:.87rem; }
main { padding-bottom:3rem; }
.page-heading { display:flex; align-items:end; justify-content:space-between; gap:1rem; margin:2rem 0 1.4rem; }
h1,h2,h3,p { margin:0; } h1 { font-size:clamp(1.7rem,3vw,2.65rem); letter-spacing:-.045em; line-height:1.1; overflow-wrap:anywhere; }
h2 { font-size:1.3rem; letter-spacing:-.025em; margin:1.65rem 0 .7rem; } h3 { font-size:1rem; overflow-wrap:anywhere; }
.stamp,.observation,.muted,small { color:var(--sub); } .stamp { font-size:.8rem; text-align:right; }
.observation,small { display:block; font-size:.8rem; }
.metrics { display:grid; grid-template-columns:repeat(3,1fr); border:1px solid var(--line); border-radius:9px; background:var(--panel); }
.metric { padding:.9rem 1.3rem; } .metric+.metric { border-left:1px solid var(--line); }
.metric span,.eyelabel { color:var(--sub); font-size:.78rem; font-weight:600; }
.metric strong { font-variant-numeric:tabular-nums; display:block; font-size:1.7rem; letter-spacing:-.04em; }
.toolbar { display:flex; align-items:center; gap:.5rem; flex-wrap:wrap; margin:1rem 0; }
input,button { font:inherit; border-radius:5px; border:1px solid #9aaca8; padding:.36rem .65rem; }
input { background:white; width:min(26rem,100%); } button { background:#eaf0ec; cursor:pointer; color:var(--ink); }
.run-list { display:grid; gap:.6rem; }
.run { background:var(--panel); border:1px solid var(--line); border-left:4px solid #3f8974; border-radius:6px;
 display:grid; grid-template-columns:minmax(170px,1.65fr) minmax(145px,1.2fr) minmax(96px,.75fr) minmax(110px,.9fr) minmax(110px,.9fr);
 align-items:center; gap:1rem; padding:.9rem 1.05rem; }
.run h3 a { text-decoration:none; } .run h3 a:hover { text-decoration:underline; }
.run.stale { border-left-color:#b48435; } .run .route { overflow-wrap:anywhere; }
.value { font-weight:620; font-variant-numeric:tabular-nums; } .badge { display:inline-block; border-radius:4px; background:var(--pale); color:#115b48; padding:.1rem .45rem; font-size:.78rem; font-weight:680; }
.stale .badge { background:#fbefce; color:#78531a; }
progress { width:100%; height:.44rem; display:block; accent-color:var(--teal); margin:.3rem 0; }
.empty { background:#f0f4f0; color:var(--sub); padding:.9rem 1rem; border:1px dashed var(--line); border-radius:6px; }
.table-scroll { overflow-x:auto; } table { border-collapse:collapse; width:100%; background:var(--panel); font-size:.85rem; }
th,td { border-bottom:1px solid var(--line); padding:.55rem .65rem; vertical-align:top; text-align:left; }
th { font-weight:650; background:#eef3f0; white-space:nowrap; } .nowrap { white-space:nowrap; font-variant-numeric:tabular-nums; }
td { overflow-wrap:anywhere; } td progress { min-width:5rem; }
.progress-complete {color:var(--teal);} .progress-active {color:var(--amber);} .progress-failed {color:var(--red);}
.trace { margin:.65rem 0; font-size:.84rem; } .alerts { padding:0; margin:0; list-style:none; }
.alerts li { background:#fff; border-left:3px solid #b07938; border-bottom:1px solid var(--line); padding:.55rem .85rem; }
@media(max-width:900px) { .run {grid-template-columns:2fr 1.6fr 1fr;} .run .timing,.run .seen {align-self:start;} }
@media(max-width:600px) { .run {grid-template-columns:1fr 1fr;gap:.65rem;} .run .identity {grid-column:1/-1;}
 .metric {padding:.65rem;} .metric strong {font-size:1.25rem;} .page-heading {align-items:start;flex-direction:column;} .stamp{text-align:left;}
 .topbar nav {gap:.55rem;} .topbar nav a {font-size:.75rem;} }
"""


def _page(title, content, queried_at_utc=None, discovery=False):
    stamp = escape(queried_at_utc or _queried_at_utc())
    overview = title == "Landing Zones Transfer Runs"
    nav = ('<nav aria-label="Run views"><a href="#current">Current runs</a>'
           '<a href="#attention">Needs attention</a><a href="#history">History</a></nav>'
           if overview else '<nav><a href="/">All runs</a></nav>')
    if overview and discovery:
        nav = nav.replace('<a href="#history">', '<a href="#discovery">Route discovery</a><a href="#history">')
    heading = 'Transfer runs' if overview else escape(title)
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="{AUTO_REFRESH_SECONDS}">
<title>{escape(title)}</title><style>{REPORT_CSS}</style></head><body>
<header><div class="topbar"><a class="brand" href="/">Landing Zones</a>{nav}</div></header>
<main><div class="page-heading"><h1>{heading}</h1><p class="stamp">Last queried: {stamp}<br>
Auto-refreshes every {AUTO_REFRESH_SECONDS} seconds. <a href="">Refresh now</a></p></div>{content}</main></body></html>"""


def _progress_text(run):
    """Return the operator-facing state and current or failed lifecycle step."""
    state = run["state"]
    if state == "completed":
        return state
    phase = (
        run["latest_failure_phase"]
        if "failed" in state
        else run["current_phase"]
    )
    if phase:
        if "failed" in state:
            return "{0} at {1}".format(state, phase)
        return "{0} ({1})".format(state, phase)
    return state


def _progress_class(state):
    """Return a stable CSS class for the high-level run state."""
    if state == "completed":
        return "progress-complete"
    if "failed" in state:
        return "progress-failed"
    if state in ("in progress", "delivered with cleanup pending", "delivered with submission pending", "waiting"):
        return "progress-active"
    return ""


def _format_bytes(number):
    if number is None:
        return "—"
    for unit in ("B", "kB", "MB", "GB", "TB", "PB", "EB"):
        if number < 1000 or unit == "EB":
            return "{:g} {}".format(round(number, 1), unit)
        number /= 1000


def _payload_cell(progress):
    if not progress:
        return "No byte measurements"
    done, total = progress["bytes_done"], progress["bytes_total"]
    label = "{} / {}".format(_format_bytes(done), _format_bytes(total))
    if progress["files_total"] is not None:
        count = progress["files_total"]
        label += '<small class="muted observation">{:,} file{}</small>'.format(count, "" if count == 1 else "s")
    if done is not None and total:
        label += '<progress aria-label="Payload bytes transferred" value="{}" max="{}"></progress>'.format(done, total)
    return label


def _activity_table(rows, include_package):
    heading = "<th>Package</th>" if include_package else ""
    body = []
    for name, item in rows:
        package = ""
        if include_package:
            package = '<td><a href="/runs/{}">{}</a></td>'.format(escape(item["run_id"]), escape(name))
        phase = escape(item["phase"])
        if item["phase"] == "promotion":
            phase = "publication (promotion)"
        sample = item.get("progress") or {}
        previous = item.get("previous_payload", {})
        payload = _payload_cell(sample or previous.get("progress"))
        if previous.get("progress"):
            payload += '<small class="observation muted">Last transfer attempt</small>'
        if item["measurement"] == "stale":
            payload += '<small class="observation progress-active">Stale byte sample</small>'
        stamp = item["sampled_at_utc"] or previous.get("sampled_at_utc")
        if stamp:
            payload += '<small class="observation muted">Sampled {}</small>'.format(escape(stamp))
        observation = {"last reported":"Last reported", "recent":"Recently observed",
                       "stale":"Stale observation", "ended":"Ended"}[item["observation"]]
        rate = sample.get("rate_bytes_per_second")
        eta = sample.get("eta_seconds")
        body.append(
            '<tr>{}<td>{}<small class="muted observation">{}</small></td>'
            '<td>{}<small class="muted observation">{}</small></td><td>{}</td>'
            '<td>{}</td><td class="nowrap">{}</td><td>{}</td><td>{}'
            '<small class="muted observation">{}</small></td></tr>'.format(
                package, escape(item["transfer_identifier"]), escape(item["runtime_id"]),
                phase, escape(item["status"]), _format_age(item["elapsed_seconds"]), payload,
                _format_bytes(rate) + "/s" if rate is not None else "—",
                _format_age(int(eta)) if eta is not None else "—", observation,
                escape(item["last_event_time_utc"])))
    return ('<div class="table-scroll"><table><thead><tr>' + heading +
            '<th>Route</th><th>Phase</th><th>Elapsed</th><th>Payload bytes</th>'
            '<th>Rate</th><th>Payload ETA</th><th>Last observation</th>'
            '</tr></thead><tbody>' + "".join(body) + '</tbody></table></div>')


def _run_card(name, item, now):
    live = item["observation"]
    phase = {"transfer": "Transfer", "promotion": "Publication", "cleanup": "Source cleanup"}.get(
        item["phase"], item["phase"].title())
    sample = item.get("progress") or {}
    previous = item.get("previous_payload", {}).get("progress") or {}
    counts = sample or previous
    done, total = counts.get("bytes_done"), counts.get("bytes_total")
    pct = f"{100 * done / total:.0f}%" if done is not None and total else "—"
    rate, eta = sample.get("rate_bytes_per_second"), sample.get("eta_seconds")
    rate_text = _format_bytes(rate) + "/s" if rate is not None else "—"
    eta_text = _format_age(int(eta)) if eta is not None else "—"
    stamp = item["last_event_time_utc"]
    last = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    age = max(0, int((now - last).total_seconds()))
    observed = {"recent":"Reporting", "stale":"Stale observation",
                "last reported":"Last reported", "ended":"Ended"}[live]
    payload = _payload_cell(counts)
    if item["measurement"] == "stale":
        payload += '<small>Stale byte sample</small>'
    return (f'<article class="run {escape(live)}"><div class="identity"><h3><a href="/runs/{escape(item["run_id"])}">'
            f'{escape(name)}</a></h3><small class="route">{escape(item["transfer_identifier"])}</small>'
            f'<span class="badge">{escape(phase)}</span></div>'
            f'<div><span class="eyelabel">Payload · {pct}</span><div>{payload}</div></div>'
            f'<div><span class="eyelabel">Payload rate</span><div class="value">{rate_text}</div>'
            f'<small>Copy ETA {eta_text}</small></div><div class="timing"><span class="eyelabel">Phase elapsed</span>'
            f'<div class="value">{_format_age(item["elapsed_seconds"])}</div></div>'
            f'<div class="seen"><span class="eyelabel">{observed}</span><div title="{escape(stamp)}">{_format_age(age)} ago</div>'
            f'<small>{escape(item["system"])}</small></div></article>')


def _search_form(query_string):
    pairs = parse_qsl(query_string, keep_blank_values=False)
    current = dict(pairs).get('q', '')
    keep = ''.join(f'<input type="hidden" name="{escape(k)}" value="{escape(v)}">'
                   for k, v in pairs if k != 'q')
    clear_url = '/?' + urlencode([(k, v) for k, v in pairs if k != 'q'])
    clear = f'<a href="{escape(clear_url)}">Clear search</a>' if current else ''
    return (f'<form class="toolbar" action="/" method="get">{keep}<label for="run-search">Find a run</label>'
            f'<input id="run-search" name="q" type="search" placeholder="Package, route or host" '
            f'value="{escape(current)}"><button>Search</button>{clear}</form>')


def _scheduler_section(status):
    if status is None:
        return ''
    title = '<section id="discovery"><h2>Route discovery</h2>'
    if status['observation'] == 'unavailable':
        return title + '<p class="empty">The scheduler status could not be read.</p></section>'
    stamp = escape(status['observed_at_utc'])
    if status['observation'] == 'held':
        heading = 'Scheduler held'
    elif status['observation'] == 'stale':
        heading = 'Stale scheduler observation'
    elif not status['enabled']:
        heading = 'Scheduler held · draining active routes'
    else:
        heading = 'Scheduler enabled'
    overview = (f'<p><strong>{heading}</strong> · {escape(status["runtime_id"])} · '
                f'Last observed {stamp}</p><p class="muted">'
                f'{status["running_routes"]} of {status["max_concurrent_transfers"]} slots in use at observation · '
                f'Discovery interval {status["discovery_interval_seconds"]}s</p>')
    rows = []
    for route in status['routes']:
        fields = (route['transfer_identifier'], route['state_label'], route['last_scanned_at_utc'],
                  route['next_due_at_utc'], route['started_at_utc'], route['worker_pid'])
        rows.append('<tr>' + ''.join(f'<td>{escape(str(value)) if value is not None else "—"}</td>'
                                     for value in fields) + '</tr>')
    if not rows:
        rows.append('<tr><td colspan="6">No matching discovery routes.</td></tr>')
    return (title + overview + '<div class="table-scroll"><table><thead><tr><th>Route</th>'
            '<th>Reported state</th><th>Last scan finished</th><th>Next due</th><th>Scan started</th>'
            '<th>Worker PID</th></tr></thead><tbody>' + ''.join(rows) + '</tbody></table></div></section>')


def _render_runs(
    runs,
    query_string="",
    show_without_directory=False,
    queried_at_utc=None,
    scheduler=None,
):
    now = datetime.fromisoformat((queried_at_utc or _queried_at_utc()).replace("Z", "+00:00"))
    search = parse_qs(query_string).get('q', [''])[-1].strip().casefold()
    if search:
        runs = [run for run in runs if any(search in (str(value or '').casefold()) for value in
                [run['directory'], run['run_name'], run['system'], run['runtime_id'], run['transfer_identifier'],
                 *(item[key] for item in run['activity'] for key in ('transfer_identifier', 'system'))])]
    hidden_runs = [run for run in runs if not (run["directory"] or run["run_name"])]
    visible_runs = runs if show_without_directory else [
        run for run in runs if run["directory"] or run["run_name"]
    ]
    hidden_errors = [run for run in hidden_runs if "failed" in run["state"]]
    active = []
    stale = []
    failed = []
    rows = []
    for run in visible_runs:
        run_id = run["run_id"]
        directory_value = escape(run["directory"] or run["run_name"] or "—")
        if run_id:
            directory_value = '<a href="/runs/{0}">{1}</a>'.format(
                escape(run_id),
                directory_value,
            )
        failure = ""
        if run["latest_failure_phase"]:
            failure = run["latest_failure_phase"]
            if run["reason_code"]:
                failure = "{0}: {1}".format(failure, run["reason_code"])
        elif run["reason_code"]:
            failure = run["reason_code"]
        if run["exit_code"] is not None:
            failure = "{0} (exit {1})".format(
                failure or "failure",
                run["exit_code"],
            )
        if run["message"]:
            failure = "{0}: {1}".format(
                failure or "failure",
                run["message"],
            )
        activity = run["activity"]
        for item in activity:
            if item["ended_at_utc"] is None:
                group = stale if item["observation"] == "stale" else active
                group.append((run["run_name"] or run["directory"] or run_id, item))
        if "failed" in run["state"]:
            failed.append((directory_value, escape(run["transfer_identifier"]),
                           escape(_progress_text(run)), escape(failure)))
        current = next((a for a in reversed(activity) if a["runtime_id"] == run["runtime_id"]
                        and a["transfer_identifier"] == run["transfer_identifier"]), {})
        payload = current.get("progress") or current.get("previous_payload", {}).get("progress")
        rows.append(
            "<tr>"
            "<td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td>"
            "<td>{4}</td><td>{5}</td><td>{6}</td><td>{7}</td>"
            "<td>{8}</td><td>{9}</td><td>{10}</td><td>{11}</td>"
            "</tr>".format(
                directory_value,
                escape(run["runtime_id"]),
                escape(run["system"]),
                escape(run["execution_user"]),
                escape(run["transfer_identifier"]),
                "yes" if run["enabled"] else ("no" if run["enabled"] is False else ""),
                '<strong class="{0}">{1}</strong>'.format(
                    _progress_class(run["state"]),
                    escape(_progress_text(run)),
                ),
                escape(run["last_event_time_utc"] or ""),
                escape(_format_age(run["age_seconds"])),
                (
                    str(run["attempt_count"])
                    if run["attempt_count"] is not None
                    else "unknown"
                ),
                escape(failure),
                _payload_cell(payload) if payload else "—",
            )
        )
    if not rows:
        rows.append('<tr><td colspan="12" class="muted">No matching runs.</td></tr>')
    table = (
        "<table><thead><tr><th>Directory</th><th>Runtime ID</th><th>System</th>"
        "<th>Execution user</th><th>Route</th><th>Enabled</th><th>Progress / step</th>"
        "<th>Last event</th><th>Age</th><th>Attempts</th><th>Latest failure</th><th>Last payload</th>"
        "</tr></thead><tbody>{0}</tbody></table>"
    ).format("".join(rows))
    if show_without_directory:
        toggle = '<a href="{0}">Hide transfers without directories</a>'.format(
            escape(_toggle_url(query_string, False))
        )
        visibility = "Showing transfers with and without directories. {0}.".format(
            toggle
        )
    elif hidden_runs:
        hidden_description = "{0} transfer{1} without a directory hidden".format(
            len(hidden_runs), "s" if len(hidden_runs) != 1 else ""
        )
        if hidden_errors:
            hidden_description += " · ⚠️ {0} hidden error{1}".format(
                len(hidden_errors), "s" if len(hidden_errors) != 1 else ""
            )
        toggle = '<a href="{0}">Show them</a>'.format(
            escape(_toggle_url(query_string, True))
        )
        visibility = "{0}. {1}.".format(hidden_description, toggle)
    else:
        visibility = "Showing runs with a package name or directory."
    controls = '<p class="muted">{0}</p>'.format(visibility)
    measured = [a['progress']['rate_bytes_per_second'] for _, a in active if
                a['phase'] == 'transfer' and a['measurement'] == 'recent' and
                (a.get('progress') or {}).get('rate_bytes_per_second') is not None]
    speed = _format_bytes(sum(measured)) + '/s' if measured else '—'
    metrics = (f'<div class="metrics"><div class="metric"><span>Current attempts</span><strong>{len(active)}</strong></div>'
               f'<div class="metric"><span>Failed runs / stale attempts</span><strong>{len(failed)} / {len(stale)}</strong></div>'
               f'<div class="metric" title="Sum of recent measured payload rates across {len(measured)} attempts">'
               f'<span>Measured payload rate</span><strong>{speed}</strong></div></div>')
    current = ''.join(_run_card(name, item, now) for name, item in active)
    attention = ''.join(_run_card(name, item, now) for name, item in stale)
    if failed:
        attention += '<ul class="alerts">' + ''.join(
            f'<li>{name} · {route} <strong class="progress-failed">{state}</strong><small>{diagnostic}</small></li>'
            for name, route, state, diagnostic in failed[:8]) + '</ul>'
        if len(failed) > 8:
            attention += '<a href="#history">See remaining failed runs in history</a>'
    return _page(
        "Landing Zones Transfer Runs", _search_form(query_string) + metrics +
        '<section id="current"><h2>Current runs</h2><div class="run-list">' +
        (current or '<p class="empty">No current attempts reported.</p>') + '</div></section>' +
        '<section id="attention"><h2>Needs attention</h2><div class="run-list">' +
        (attention or '<p class="empty">No failed runs or stale attempts in this view.</p>') + '</div></section>' +
        _scheduler_section(scheduler) + '<section id="history"><h2>Run history</h2>' + controls +
        '<div class="table-scroll">' + table + '</div></section>', queried_at_utc=queried_at_utc,
        discovery=scheduler is not None)



def _render_detail(detail, queried_at_utc=None):
    rows = []
    for event in detail["timeline"]:
        fields = _message_fields(event)
        if event["status"] == "started" and (fields.get("heartbeat") is True
                or isinstance(fields.get("progress"), dict) and fields["progress"].get("bytes_done") is not None):
            continue
        diagnostic = event["reason_code"] or ""
        if event["exit_code"] is not None:
            diagnostic = "{0} exit={1}".format(diagnostic, event["exit_code"]).strip()
        message = fields.get("detail", event["message"])
        if isinstance(message, str) and message:
            diagnostic = "{0} {1}".format(diagnostic, message).strip()
        rows.append(
            "<tr><td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td>"
            "<td>{4}</td><td>{5}</td></tr>".format(
                escape(event["event_time_utc"]),
                escape(event["transfer_identifier"]),
                escape(event["status"]),
                escape(event["phase"]),
                escape(event["attempt_id"] or ""),
                escape(diagnostic),
            )
        )
    table = (
        "<table><thead><tr><th>Event time</th><th>Route</th><th>Status</th>"
        "<th>Phase</th><th>Attempt ID</th><th>Diagnostic</th></tr></thead>"
        "<tbody>{0}</tbody></table>"
    ).format("".join(rows))
    return _page(
        "Transfer Run {0}".format(detail["run_id"]),
        '<p><a href="/">All runs</a></p><h2>Phase attempts</h2>' +
        _activity_table([(None, item) for item in reversed(detail["attempts"])], False) +
        '<h2>Lifecycle events</h2><div class="table-scroll">' + table + '</div>',
        queried_at_utc=queried_at_utc,
    )


class MonitoringApplication:
    """Request boundary that queries current database state for every call."""

    def __init__(self, database_url, scheduler_state_dir=None, scheduler_runtime_ids=()):
        self.database_url = database_url
        self.scheduler_state_dir = scheduler_state_dir
        self.scheduler_runtime_ids = scheduler_runtime_ids

    def _scheduler(self, query_string, queried_at_utc):
        now = datetime.fromisoformat(queried_at_utc.replace('Z', '+00:00'))
        status = read_scheduler_status(self.scheduler_state_dir, self.scheduler_runtime_ids, now)
        if not status or 'runtime_id' not in status:
            return status
        query = parse_qs(query_string)
        if query.get('runtime_id') and status['runtime_id'] not in query['runtime_id']:
            return None
        identifiers = query.get('transfer_identifier', [])
        search = query.get('q', [''])[-1].strip().casefold()
        status['routes'] = [
            row for row in status['routes']
            if (not identifiers or row['transfer_identifier'] in identifiers)
            and (not search or search in row['transfer_identifier'].casefold()
                 or search in status['runtime_id'].casefold())
        ]
        return status

    def respond(self, path, query_string=""):
        """Return an HTTP-style status, headers, and text response body."""
        queried_at_utc = _queried_at_utc()
        if path == "/api/scheduler":
            return _json_response({"queried_at_utc": queried_at_utc,
                                   "scheduler": self._scheduler(query_string, queried_at_utc)})
        if path == "/api/runs":
            runs = query_run_summaries(
                self.database_url,
                **_query_filters(query_string)
            )
            result = {"queried_at_utc": queried_at_utc, "runs": runs}
            scheduler = self._scheduler(query_string, queried_at_utc)
            if scheduler is not None:
                result["scheduler"] = scheduler
            return _json_response(result)
        if path.startswith("/api/runs/"):
            run_id = unquote(path[len("/api/runs/") :])
            detail = query_run_detail(self.database_url, run_id)
            if detail is None:
                return _json_response({"error": "Transfer Run not found"}, status=404)
            return _json_response(detail)
        if path.startswith("/runs/"):
            run_id = unquote(path[len("/runs/") :])
            detail = query_run_detail(self.database_url, run_id)
            if detail is None:
                return (
                    404,
                    {"Content-Type": "text/html; charset=utf-8"},
                    _page(
                        "Transfer Run not found",
                        "<p>No matching event history.</p>",
                        queried_at_utc=queried_at_utc,
                    ),
                )
            return (
                200,
                {"Content-Type": "text/html; charset=utf-8"},
                _render_detail(detail, queried_at_utc=queried_at_utc),
            )
        if path == "/":
            runs = query_run_summaries(
                self.database_url,
                **_query_filters(query_string)
            )
            return (
                200,
                {"Content-Type": "text/html; charset=utf-8"},
                _render_runs(
                    runs,
                    query_string=query_string,
                    show_without_directory=_show_without_directory(query_string),
                    scheduler=self._scheduler(query_string, queried_at_utc),
                    queried_at_utc=queried_at_utc,
                ),
            )
        return _json_response({"error": "Not found"}, status=404)


class SpoolIngestion:
    """Poll explicit event sources independently of HTTP page requests."""

    def __init__(self, database_url, spools, interval=60):
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("ingest_interval must be finite and positive")
        self.database_url = database_url
        self.spools = tuple(spools)
        self.interval = interval
        self.next_poll = 0

    def poll(self, startup=False):
        now = time.monotonic()
        if not startup and now < self.next_poll:
            return
        for path in self.spools:
            try:
                result = ingest_event_spool(self.database_url, path)
                for warning in result.warnings:
                    logging.warning("%s: %s", path, warning)
            except FileNotFoundError:
                logging.warning("Monitoring event spool does not exist yet: %s", path)
            except Exception:
                if startup:
                    raise
                logging.exception("Monitoring ingestion failed for %s; will retry", path)
        self.next_poll = time.monotonic() + self.interval


def serve_monitoring(database_url, host="127.0.0.1", port=8080,
                     spools=(), ingest_interval=60, scheduler_state_dir=None, scheduler_runtime_ids=()):
    """Serve live monitoring pages until interrupted."""
    ingestor = SpoolIngestion(database_url, spools, ingest_interval)
    ingestor.poll(startup=True)
    application = MonitoringApplication(database_url, scheduler_state_dir, scheduler_runtime_ids)

    class RequestHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            request = urlsplit(self.path)
            status, headers, body = application.respond(
                request.path,
                request.query,
            )
            body_bytes = body.encode("utf-8")
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(body_bytes)))
            self.end_headers()
            self.wfile.write(body_bytes)

    class MonitoringServer(ThreadingHTTPServer):
        def service_actions(self):
            ingestor.poll()

    server = MonitoringServer((host, port), RequestHandler)
    try:
        server.serve_forever()
    finally:
        server.server_close()
