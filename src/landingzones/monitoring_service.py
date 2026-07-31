#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Live HTTP and JSON views over the Transfer Event monitoring database."""

from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from datetime import datetime, timezone
from urllib.parse import parse_qs, parse_qsl, unquote, urlencode, urlsplit

from landingzones.monitoring import query_run_detail, query_run_summaries


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


def _page(title, content, queried_at_utc=None):
    queried_at_utc = queried_at_utc or _queried_at_utc()
    return """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="{1}">
<title>{0}</title>
<style>
body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #1f2937; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border-bottom: 1px solid #d1d5db; padding: .55rem; text-align: left; vertical-align: top; }}
th {{ background: #f3f4f6; }}
code {{ white-space: nowrap; }}
.muted {{ color: #6b7280; }}
.progress-complete {{ color: #166534; }}
.progress-failed {{ color: #b91c1c; }}
.progress-active {{ color: #92400e; }}
</style>
</head>
<body><h1>{0}</h1><p class="muted">Last queried: {2} · Auto-refreshes every {1} seconds.</p>{3}</body>
</html>""".format(
        escape(title),
        AUTO_REFRESH_SECONDS,
        escape(queried_at_utc),
        content,
    )


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
    if state in ("in progress", "delivered with cleanup pending", "waiting"):
        return "progress-active"
    return ""


def _render_runs(
    runs,
    query_string="",
    show_without_directory=False,
    queried_at_utc=None,
):
    hidden_runs = [run for run in runs if not run["directory"]]
    visible_runs = runs if show_without_directory else [
        run for run in runs if run["directory"]
    ]
    hidden_errors = [run for run in hidden_runs if "failed" in run["state"]]
    rows = []
    for run in visible_runs:
        run_id = run["run_id"]
        directory_value = escape(run["directory"] or "—")
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
        rows.append(
            "<tr>"
            "<td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td>"
            "<td>{4}</td><td>{5}</td><td>{6}</td><td>{7}</td>"
            "<td>{8}</td><td>{9}</td><td>{10}</td>"
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
            )
        )
    if not rows:
        rows.append('<tr><td colspan="11" class="muted">No matching runs.</td></tr>')
    table = (
        "<table><thead><tr><th>Directory</th><th>Runtime ID</th><th>System</th>"
        "<th>Execution user</th><th>Route</th><th>Enabled</th><th>Progress / step</th>"
        "<th>Last event</th><th>Age</th><th>Attempts</th><th>Latest failure</th>"
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
        visibility = "Showing transfers with directories."
    controls = '<p class="muted">{0}</p>'.format(visibility)
    return _page(
        "Landing Zones Transfer Runs",
        controls + table,
        queried_at_utc=queried_at_utc,
    )


def _render_detail(detail, queried_at_utc=None):
    rows = []
    for event in detail["timeline"]:
        diagnostic = event["reason_code"] or ""
        if event["exit_code"] is not None:
            diagnostic = "{0} exit={1}".format(diagnostic, event["exit_code"]).strip()
        if event["message"]:
            diagnostic = "{0} {1}".format(diagnostic, event["message"]).strip()
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
        '<p><a href="/">All runs</a></p>'
        "<table><thead><tr><th>Event time</th><th>Route</th><th>Status</th>"
        "<th>Phase</th><th>Attempt ID</th><th>Diagnostic</th></tr></thead>"
        "<tbody>{0}</tbody></table>"
    ).format("".join(rows))
    return _page(
        "Transfer Run {0}".format(detail["run_id"]),
        table,
        queried_at_utc=queried_at_utc,
    )


class MonitoringApplication:
    """Request boundary that queries current database state for every call."""

    def __init__(self, database_url):
        self.database_url = database_url

    def respond(self, path, query_string=""):
        """Return an HTTP-style status, headers, and text response body."""
        queried_at_utc = _queried_at_utc()
        if path == "/api/runs":
            runs = query_run_summaries(
                self.database_url,
                **_query_filters(query_string)
            )
            return _json_response(
                {"queried_at_utc": queried_at_utc, "runs": runs}
            )
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
                    queried_at_utc=queried_at_utc,
                ),
            )
        return _json_response({"error": "Not found"}, status=404)


def serve_monitoring(database_url, host="127.0.0.1", port=8080):
    """Serve live monitoring pages until interrupted."""
    application = MonitoringApplication(database_url)

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

    server = ThreadingHTTPServer((host, port), RequestHandler)
    server.serve_forever()
