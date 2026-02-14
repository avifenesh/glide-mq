#!/bin/bash
# Start a 6-node Valkey cluster for integration testing
# Usage: wsl -d Ubuntu-24.04 -- bash /mnt/c/Users/avife/glide-mq/tests/helpers/start-cluster.sh

set -e

CLUSTER_DIR=/tmp/valkey-cluster

# Kill any leftover nodes
for port in 7000 7001 7002 7003 7004 7005; do
  valkey-cli -p $port shutdown nosave 2>/dev/null || true
done
sleep 1
rm -rf $CLUSTER_DIR

# Create and start 6 nodes
for port in 7000 7001 7002 7003 7004 7005; do
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
bind 0.0.0.0
protected-mode no
EOF
  valkey-server $dir/valkey.conf
done

sleep 1

# Verify all nodes
echo "--- Node check ---"
for port in 7000 7001 7002 7003 7004 7005; do
  result=$(valkey-cli -p $port ping 2>/dev/null)
  echo "Port $port: $result"
done

# Create cluster (3 primaries + 3 replicas)
echo "--- Creating cluster ---"
echo "yes" | valkey-cli --cluster create \
  127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 \
  127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 \
  --cluster-replicas 1

sleep 2

echo "--- Cluster info ---"
valkey-cli -p 7000 cluster info | head -5
echo "--- Cluster nodes ---"
valkey-cli -p 7000 cluster nodes
echo "CLUSTER READY"
