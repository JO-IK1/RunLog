#!/usr/bin/env python3
"""End-to-end checks against Compose. Requires Python 3, no pip packages."""

import json
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

BASE = "http://127.0.0.1:8080"


def compose(*args):
    """Runs Docker Compose and returns its standard output as text."""
    return subprocess.check_output(["docker", "compose", *args], text=True)


def project():
    """Creates a smoke-test project and returns its ID and API key."""
    result = compose("exec", "-T", "api", "runlog", "project-create",
                     "Smoke test")
    fields = dict(
        line.split("=", 1) for line in result.splitlines() if "=" in line)
    return fields["project_id"], fields["api_key"]


def request(method, path, key="", body=None):
    """Sends an API request and returns the HTTP status and decoded JSON."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path,
                                 data=data,
                                 method=method,
                                 headers={
                                     "Authorization": "Bearer " + key,
                                     "Content-Type": "application/json"
                                 })
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def wait_for(fn, seconds=90):
    """Waits for a predicate, raising AssertionError when the timeout expires."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            if fn():
                return
        except (OSError, ValueError):
            pass
        time.sleep(1)
    raise AssertionError("timed out waiting for expected state")


def main():
    """Checks event delivery, isolation, pagination, and outage recovery."""
    wait_for(lambda: request("GET", "/health/ready")[0] == 200)
    project_id, key = project()
    _, other_key = project()
    run = "smoke-" + uuid.uuid4().hex
    path = "/v1/runs/" + run + "/events"
    event = {
        "event_id": uuid.uuid4().hex,
        "run_id": run,
        "type": "run.queued",
        "occurred_at": "2026-11-14T10:00:00.123456Z",
        "message": "queued",
        "data": {
            "attempt": 1,
            "nested": {
                "ok": True,
                "items": [1, {
                    "name": "test"
                }]
            }
        }
    }
    assert request("POST", "/v1/events", "wrong", event)[0] == 401
    assert request("POST", "/v1/events", key,
                   dict(event, project_id="forged"))[0] == 400
    assert request("POST", "/v1/events", key,
                   dict(event, message="x" * 70000))[0] == 413
    assert request("POST", "/v1/events", key, event)[0] == 202
    assert request("POST", "/v1/events", key, event)[0] == 202
    wait_for(lambda: len(request("GET", path, key)[1].get("events", [])) == 1)
    assert request("GET", path, key)[1]["events"][0]["data"] == event["data"]
    assert request("GET", path, other_key)[1]["events"] == []
    assert request("POST", "/v1/events", other_key, event)[0] == 202
    wait_for(
        lambda: len(request("GET", path, other_key)[1].get("events", [])) == 1)
    assert request("POST", "/v1/events", key, dict(event,
                                                   message="changed"))[0] == 202
    time.sleep(1)
    assert request("GET", path, key)[1]["events"][0]["message"] == "queued"
    event2 = dict(event,
                  event_id=uuid.uuid4().hex,
                  type="run.finished",
                  message="done")
    assert request("POST", "/v1/events", key, event2)[0] == 202
    wait_for(lambda: len(request("GET", path, key)[1].get("events", [])) == 2)
    first = request("GET", path + "?limit=1", key)[1]
    cursor = urllib.parse.quote(first["next_cursor"])
    second = request("GET", path + "?limit=1&cursor=" + cursor, key)[1]
    assert len(second["events"]) == 1
    assert first["events"][0]["event_id"] != second["events"][0]["event_id"]
    assert request("GET", path + "?type=run.finished",
                   key)[1]["events"][0]["message"] == "done"
    # Restart the group from the beginning: already stored records remain unique.
    compose("stop", "consumer")
    compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", "kafka:9092", "--group", "runlog-storage-v1",
            "--topic", "runlog.events", "--reset-offsets", "--to-earliest",
            "--execute")
    compose("start", "consumer")
    time.sleep(3)
    assert len(request("GET", path, key)[1]["events"]) == 2
    compose("stop", "mongo")
    event3 = dict(event, event_id=uuid.uuid4().hex, type="build.started")
    assert request("POST", "/v1/events", key, event3)[0] == 202
    time.sleep(6)
    compose("start", "mongo")
    wait_for(lambda: len(request("GET", path, key)[1].get("events", [])) == 3)
    compose("stop", "consumer")
    event4 = dict(event, event_id=uuid.uuid4().hex, type="tests.finished")
    assert request("POST", "/v1/events", key, event4)[0] == 202
    assert len(request("GET", path, key)[1]["events"]) == 3
    compose("start", "consumer")
    wait_for(lambda: len(request("GET", path, key)[1].get("events", [])) == 4)
    compose("stop", "kafka")
    started = time.monotonic()
    assert request("POST", "/v1/events", key,
                   dict(event, event_id=uuid.uuid4().hex))[0] == 503
    assert time.monotonic() - started < 14, "publication exceeded HTTP deadline"
    compose("start", "kafka")
    wait_for(lambda: request("GET", "/health/ready")[0] == 200)
    compose("exec", "-T", "api", "runlog", "project-revoke", project_id)
    assert request("GET", path, key)[0] == 401
    print(
        "PASS: auth, validation, deduplication, pagination, replay, Mongo/Kafka outage, consumer restart, revocation"
    )


if __name__ == "__main__":
    try:
        main()
    finally:
        subprocess.run(
            ["docker", "compose", "start", "mongo", "kafka", "consumer"],
            check=False)
