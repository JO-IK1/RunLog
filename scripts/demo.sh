#!/usr/bin/env bash
# Publishes sample run events and prints the resulting history.
set -euo pipefail
: "${RUNLOG_KEY:?Set RUNLOG_KEY to the project API key}"
runlog_url="${RUNLOG_URL:-http://localhost:8080}"
run_id="demo-$(date +%s)"
for type in run.queued build.started tests.finished run.finished; do
  event_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  occurred_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  curl --fail-with-body -sS "${runlog_url}/v1/events" \
    -H "Authorization: Bearer $RUNLOG_KEY" -H 'Content-Type: application/json' \
    -d "{\"event_id\":\"$event_id\",\"run_id\":\"$run_id\",\"type\":\"$type\",\"occurred_at\":\"$occurred_at\",\"message\":\"$type\",\"data\":{}}"
  echo
  sleep 1
done
curl --fail-with-body -sS \
  -H "Authorization: Bearer ${RUNLOG_KEY}" \
  "${runlog_url}/v1/runs/${run_id}/events"
echo
