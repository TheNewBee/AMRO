#!/usr/bin/env bash
# E2E against the deployed Kafka+backend+frontend stack (not Testcontainers).
# Usage: scripts/e2e-stack.sh [compose|k8s]
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
TARGET=${1:-compose}
NS=amn-amro
CLIENT=$(printf '%04d' $((RANDOM % 9000 + 1000)))
TXN_TOPIC=amn-amro-transactions
DLQ_TOPIC=amn-amro-transactions-dead-letter

record315() {
  python3 - "$1" <<'PY'
import sys
client = sys.argv[1]
chars = [' '] * 176
def put(from1, value):
    chars[from1-1:from1-1+len(value)] = value
put(1, '315'); put(4, 'CL'); put(8, client); put(12, '0001'); put(16, '0001')
put(26, 'FU'); put(28, 'CME'); put(32, 'E2E'); put(38, '20200101')
chars[51] = '+'; put(53, '0000000007'); chars[62] = '+'; put(64, '0000000000')
sys.stdout.write(''.join(chars))
PY
}

json_ok() {
  python3 -c '
import json, sys
mode, extra = sys.argv[1], sys.argv[2]
raw = sys.stdin.read()
if not raw.strip():
    raise SystemExit(1)
try:
    rows = json.loads(raw)
except json.JSONDecodeError:
    raise SystemExit(1)
if mode == "oracle":
    oracle = {
        ("CL|1234|0002|0001", "SGX|FU|NK|20100910", "-52"),
        ("CL|1234|0003|0001", "CME|FU|N1|20100910", "285"),
        ("CL|1234|0003|0001", "CME|FU|NK.|20100910", "-215"),
        ("CL|4321|0002|0001", "SGX|FU|NK|20100910", "46"),
        ("CL|4321|0003|0001", "CME|FU|N1|20100910", "-79"),
    }
    got = {(r["clientInformation"], r["productInformation"], r["totalTransactionAmount"]) for r in rows}
    raise SystemExit(0 if oracle <= got else 1)
key = f"CL|{extra}|0001|0001"
raise SystemExit(0 if any(
    r["clientInformation"] == key
    and r["productInformation"] == "CME|FU|E2E|20200101"
    and r["totalTransactionAmount"] == "7"
    for r in rows
) else 1)
' "$1" "$2"
}

if [ "$TARGET" = compose ]; then
  kafka() { docker compose exec -T kafka "$@"; }
  append() { printf '%s\n' "$1" | docker compose exec -T backend sh -c 'cat >> /data/Input.txt'; }
  fetch() { curl -sf "$1"; }
  API=http://127.0.0.1:8080
  FE=http://127.0.0.1:8081
  BOOTSTRAP=localhost:9092
elif [ "$TARGET" = k8s ]; then
  kafka() { kubectl -n "$NS" exec -i deploy/kafka -- "$@"; }
  append() { printf '%s\n' "$1" | kubectl -n "$NS" exec -i deploy/amn-amro-backend -c backend -- sh -c 'cat >> /data/Input.txt'; }
  kubectl -n "$NS" delete pod e2e-curl --ignore-not-found >/dev/null
  kubectl -n "$NS" run e2e-curl --image=curlimages/curl:8.11.1 --restart=Never --command -- sleep 180 >/dev/null
  kubectl -n "$NS" wait --for=condition=Ready pod/e2e-curl --timeout=60s >/dev/null
  fetch() { kubectl -n "$NS" exec e2e-curl -- curl -sf "$1"; }
  API=http://amn-amro-backend:8080
  FE=http://amn-amro-frontend
  BOOTSTRAP=localhost:9092
  trap 'kubectl -n "$NS" delete pod e2e-curl --ignore-not-found >/dev/null' EXIT
else
  echo "usage: $0 [compose|k8s]" >&2
  exit 2
fi

offset() {
  kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP" --topic "$1" \
    | awk -F: '{print $NF+0; exit}'
}

wait_json() {
  local what=$1 mode=$2 extra=${3:-}
  local deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    if fetch "$API/api/summary" | json_ok "$mode" "$extra"; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for $what" >&2
  fetch "$API/api/summary" || true
  echo >&2
  return 1
}

echo "== $TARGET: fixture through deployed Kafka =="
wait_json oracle oracle
fetch "$FE/api/summary" | json_ok oracle ""
before_txn=$(offset "$TXN_TOPIC")
before_dlq=$(offset "$DLQ_TOPIC" || echo 0)
echo "offsets txn=$before_txn dlq=$before_dlq"

echo "== $TARGET: append valid CL|$CLIENT via Input.txt =="
append "$(record315 "$CLIENT")"
wait_json "client $CLIENT" client "$CLIENT"
fetch "$FE/api/summary" | json_ok client "$CLIENT"
after_txn=$(offset "$TXN_TOPIC")
test "$after_txn" -gt "$before_txn"

echo "== $TARGET: malformed line to DLQ, totals unchanged =="
append "$(python3 -c 'print("x"*176)')"
deadline=$((SECONDS + 90))
while (( SECONDS < deadline )); do
  now_dlq=$(offset "$DLQ_TOPIC" || echo 0)
  if [ "$now_dlq" -gt "$before_dlq" ]; then
    break
  fi
  sleep 1
done
test "$(offset "$DLQ_TOPIC")" -gt "$before_dlq"
fetch "$API/api/summary" | json_ok client "$CLIENT"
dlq=$(kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
  --topic "$DLQ_TOPIC" --from-beginning --max-messages 1 --timeout-ms 8000 2>/dev/null || true)
echo "$dlq" | grep -q 'record code must be 315'

echo "PASS $TARGET client=CL|$CLIENT txn $before_txn->$after_txn"
