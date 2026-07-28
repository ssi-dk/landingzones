#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Live HTTP and JSON views over the Transfer Event monitoring database."""

from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from urllib.parse import parse_qs, unquote, urlsplit

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


def _page(title, content):
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
<body><h1>{0}</h1><p class="muted">Auto-refreshes every {1} seconds.</p>{2}</body>
</html>""".format(escape(title), AUTO_REFRESH_SECONDS, content)


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


def _render_runs(runs):
    rows = []
    for run in runs:
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
    return _page("Landing Zones Transfer Runs", table)


def _render_detail(detail):
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
    return _page("Transfer Run {0}".format(detail["run_id"]), table)


class MonitoringApplication:
    """Request boundary that queries current database state for every call."""

    def __init__(self, database_url):
        self.database_url = database_url

    def respond(self, path, query_string=""):
        """Return an HTTP-style status, headers, and text response body."""
        if path == "/api/runs":
            runs = query_run_summaries(
                self.database_url,
                **_query_filters(query_string)
            )
            return _json_response({"runs": runs})
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
                    _page("Transfer Run not found", "<p>No matching event history.</p>"),
                )
            return (
                200,
                {"Content-Type": "text/html; charset=utf-8"},
                _render_detail(detail),
            )
        if path == "/":
            runs = query_run_summaries(
                self.database_url,
                **_query_filters(query_string)
            )
            return (
                200,
                {"Content-Type": "text/html; charset=utf-8"},
                _render_runs(runs),
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
