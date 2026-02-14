#!/bin/bash
# Stop the Valkey cluster
for port in 7000 7001 7002 7003 7004 7005; do
  valkey-cli -p $port shutdown nosave 2>/dev/null || true
done
rm -rf /tmp/valkey-cluster
echo "Cluster stopped"
