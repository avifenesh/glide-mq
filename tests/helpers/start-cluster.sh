#!/bin/bash
# Start a 6-node Valkey cluster for integration testing
# Ports: 7000-7005 (3 primaries + 3 replicas)

set -e

CLUSTER_DIR=/tmp/valkey-cluster
PORTS="7000 7001 7002 7003 7004 7005"

# Detect CLI binary (valkey-cli or redis-cli)
if command -v valkey-cli &>/dev/null; then
  CLI=valkey-cli
  SERVER=valkey-server
elif command -v redis-cli &>/dev/null; then
  CLI=redis-cli
  SERVER=redis-server
else
  echo "[ERROR] Neither valkey-cli nor redis-cli found in PATH"
  exit 1
fi

echo "Using CLI: $CLI, Server: $SERVER"

# Kill any leftover nodes (with timeout to avoid hanging on dead ports)
for port in $PORTS; do
  timeout 2 $CLI -p $port shutdown nosave 2>/dev/null || true
done
sleep 1

# Also kill by pidfile or process name as a fallback
for port in $PORTS; do
  pidfile=$CLUSTER_DIR/$port/valkey.pid
  if [ -f "$pidfile" ]; then
    kill "$(cat "$pidfile")" 2>/dev/null || true
  fi
done
# Kill any leftover server processes on cluster ports
for port in $PORTS; do
  lsof -ti :$port 2>/dev/null | xargs kill 2>/dev/null || true
done
sleep 1

rm -rf $CLUSTER_DIR

# Create and start 6 nodes
for port in $PORTS; do
  dir=$CLUSTER_DIR/$port
  mkdir -p $dir
  cat > $dir/valkey.conf <<EOF
port $port
cluster-enabled yes
cluster-config-file $dir/nodes.conf
cluster-node-timeout 5000
appendonly no
save ""
maxmemory-policy noeviction
dir $dir
daemonize yes
pidfile $dir/valkey.pid
logfile $dir/valkey.log
bind 127.0.0.1
protected-mode no
EOF
  $SERVER $dir/valkey.conf
  echo "Started node on port $port"
done

sleep 2

# Verify all nodes are up
echo "--- Node check ---"
ALL_UP=true
for port in $PORTS; do
  result=$(timeout 3 $CLI -p $port ping 2>/dev/null || echo "FAIL")
  echo "Port $port: $result"
  if [ "$result" != "PONG" ]; then
    ALL_UP=false
  fi
done

if [ "$ALL_UP" != "true" ]; then
  echo "[ERROR] Not all nodes responded to PING"
  for port in $PORTS; do
    logfile=$CLUSTER_DIR/$port/valkey.log
    if [ -f "$logfile" ]; then
      echo "--- Log for port $port ---"
      tail -5 "$logfile"
    fi
  done
  exit 1
fi

# Create cluster (3 primaries + 3 replicas)
echo "--- Creating cluster ---"
echo "yes" | $CLI --cluster create \
  127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 \
  127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 \
  --cluster-replicas 1

sleep 2

# Wait for cluster to converge
echo "--- Waiting for cluster to stabilize ---"
for i in $(seq 1 10); do
  state=$(timeout 3 $CLI -p 7000 cluster info 2>/dev/null | grep cluster_state | tr -d '\r')
  if [ "$state" = "cluster_state:ok" ]; then
    echo "Cluster is healthy after ${i}s"
    break
  fi
  if [ "$i" -eq 10 ]; then
    echo "[ERROR] Cluster did not stabilize within 10s"
    timeout 3 $CLI -p 7000 cluster info 2>/dev/null || true
    exit 1
  fi
  sleep 1
done

echo "--- Cluster info ---"
timeout 3 $CLI -p 7000 cluster info | head -5
echo "--- Cluster nodes ---"
timeout 3 $CLI -p 7000 cluster nodes
echo "CLUSTER READY"
