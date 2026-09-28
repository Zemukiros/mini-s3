#!/usr/bin/env bash
# Failure-recovery demo (macOS / Linux / Git Bash / WSL).
# Run from the project root after:  docker compose up -d --build
set -euo pipefail

API=http://localhost:8080
KEY="X-Api-Key: dev-key"
cluster() { curl -s -H "$KEY" $API/admin/cluster; echo; }

echo "== Waiting for 4 storage nodes to register =="
until [ "$(curl -s -H "$KEY" $API/admin/cluster | grep -o '"status":"UP"' | wc -l)" -ge 4 ]; do sleep 2; done

echo "== Uploading 20 files + demo.bin (5 MB) =="
head -c 5242880 /dev/urandom > /tmp/demo.bin
ORIGINAL=$(sha256sum /tmp/demo.bin | cut -d' ' -f1)
curl -s -H "$KEY" -H "Content-Type: application/octet-stream" -T /tmp/demo.bin $API/buckets/demo/objects/demo.bin; echo
for i in $(seq 1 20); do
  curl -s -o /dev/null -H "$KEY" -H "Content-Type: text/plain" -X PUT --data-binary "hello from file $i" \
    $API/buckets/demo/objects/files/file-$i.txt
done
sleep 2; cluster

VICTIM=$(curl -s -H "$KEY" $API/admin/replicas/demo/demo.bin | grep -o '"id":"node-[0-9]*"' | head -1 | cut -d'"' -f4)
SERVICE=${VICTIM/node-/storage-}
echo "== Killing $SERVICE ($VICTIM) =="
docker compose stop "$SERVICE" >/dev/null
echo "Waiting ~15s: 6s heartbeat timeout + repair pass..."; sleep 15
cluster
curl -s -H "$KEY" $API/admin/replicas/demo/demo.bin; echo

echo "== Downloading demo.bin with $VICTIM still dead =="
curl -s -D /tmp/headers -H "$KEY" -o /tmp/demo-download.bin $API/buckets/demo/objects/demo.bin
DOWNLOADED=$(sha256sum /tmp/demo-download.bin | cut -d' ' -f1)
grep -i x-served-by /tmp/headers || true
echo "original  : $ORIGINAL"; echo "downloaded: $DOWNLOADED"
[ "$ORIGINAL" = "$DOWNLOADED" ] && echo "MATCH - no data lost" || { echo "MISMATCH"; exit 1; }

echo "== Bringing $SERVICE back =="
docker compose start "$SERVICE" >/dev/null
sleep 15; cluster
docker compose logs worker --tail 25
