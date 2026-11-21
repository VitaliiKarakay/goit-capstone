"""Shared preflight for recovery demo scripts (no auto-cleanup)."""

from urllib.parse import urlencode


def assert_quiet_standalone_jobs(api, request, fail):
    for status in ("processing", "queued"):
        path = "/jobs?" + urlencode({"status": status, "limit": 5, "offset": 0})
        _, page = request("GET", path)
        items = page.get("items") or []
        if not items:
            continue
        ids = [item.get("id") for item in items]
        fail(
            "preflight: found standalone "
            + status
            + " job(s) "
            + ", ".join(ids)
            + ". Stop workers, cancel or finish stuck jobs, drain the JetStream backlog "
            "(see README crash demo), then re-run. This script does not mutate the queue."
        )