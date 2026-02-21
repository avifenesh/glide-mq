#!/bin/bash
# Stop the Valkey cluster

# Detect CLI binary
if command -v valkey-cli &>/dev/null; then
  CLI=valkey-cli
elif command -v redis-cli &>/dev/null; then
  CLI=redis-cli
else
  echo "[WARN] No CLI found, attempting kill by port"
  CLI=""
fi

for port in 7000 7001 7002 7003 7004 7005; do
  if [ -n "$CLI" ]; then
    timeout 2 $CLI -p $port shutdown nosave 2>/dev/null || true
  fi
  # Fallback: kill by port
  lsof -ti :$port 2>/dev/null | xargs kill 2>/dev/null || true
done
rm -rf /tmp/valkey-cluster
echo "Cluster stopped"
