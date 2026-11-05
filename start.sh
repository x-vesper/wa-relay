#!/usr/bin/env bash
# Idempotent start untuk wa-relay.
# Kalau systemd user service terpasang -> delegasi ke systemctl (lihat systemd/install.sh).
# Kalau tidak -> mode manual lama (setsid + nohup + pidfile).
set -euo pipefail
cd "$(dirname "$0")"

# bangun binary jika belum ada atau source lebih baru dari binary terakhir
if [ ! -x ./wa-relay ] \
  || [ -n "$(find ./cmd ./internal -name '*.go' -newer ./wa-relay -print -quit 2>/dev/null)" ] \
  || [ ./go.mod -nt ./wa-relay ] \
  || { [ -f ./go.sum ] && [ ./go.sum -nt ./wa-relay ]; }; then
  echo "building wa-relay..."
  go build -o wa-relay ./cmd/wa-relay
fi

PORT=$(python3 -c "import json;print(json.load(open('config.json')).get('port',8791))")

# ---- mode systemd ----
if systemctl --user cat wa-relay.service >/dev/null 2>&1; then
  echo "starting via systemd..."
  systemctl --user restart wa-relay.service
  systemctl --user restart wa-relay-sync.service
  systemctl --user --no-pager --lines=0 status wa-relay.service wa-relay-sync.service | grep -E "Active:" || true
  sleep 1
  echo "health: $(curl -s -m 5 "http://127.0.0.1:${PORT}/health" || echo 'belum siap')"
  exit 0
fi

# ---- mode manual (fallback) ----
SECRET=$(python3 -c "import json;print(json.load(open('config.json')).get('webhook_secret',''))")
LOG=${LOG_FILE:-relay.log}
SYNC_LOG=${SYNC_LOG:-sync.log}

# (a) hentikan wacli sync --follow lama untuk store ini
if [ -f sync.pid ]; then
  kill "$(cat sync.pid)" 2>/dev/null || true
  rm -f sync.pid
fi
pkill -f "wacli sync --follow" 2>/dev/null || true
sleep 0.5

# (b) hentikan relay lama (pidfile, lalu pemakai port)
if [ -f relay.pid ]; then
  kill "$(cat relay.pid)" 2>/dev/null || true
  rm -f relay.pid
fi
OLD=$(ss -ltnp 2>/dev/null | grep ":${PORT} " | grep -oP 'pid=\K[0-9]+' | sort -u || true)
for p in $OLD; do kill "$p" 2>/dev/null || true; done
sleep 0.5

# (c) jalankan relay
setsid nohup ./wa-relay >>"$LOG" 2>&1 </dev/null &
echo $! > relay.pid
echo "relay pid: $(cat relay.pid)"

# (d) jalankan wacli sync dengan webhook
setsid nohup wacli sync --follow --max-messages 3000 --presence-mode quiet \
  --webhook "http://127.0.0.1:${PORT}/hook" \
  --webhook-secret "$SECRET" \
  --webhook-allow-private \
  --webhook-events message \
  >>"$SYNC_LOG" 2>&1 </dev/null &
echo $! > sync.pid
echo "sync pid: $(cat sync.pid)"

sleep 1
echo "--- tail $LOG ---"
tail -n 10 "$LOG" || true
echo "--- tail $SYNC_LOG ---"
tail -n 10 "$SYNC_LOG" || true
