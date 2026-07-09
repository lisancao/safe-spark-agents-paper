#!/usr/bin/env bash
# =============================================================================================
# run.sh — bring up the LOCAL config-shakeout stack and run the spike end-to-end.
#
#   ./run.sh          # build, up, provision, test; leaves the stack up
#   ./run.sh --down   # tear everything down (containers + volumes)
#
# Exit code == run_spike.py's: 0 iff every check passed. Artifacts land in ./out/.
# No AWS, no secrets, no network egress beyond image/jar pulls.
# =============================================================================================
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p out

# Flags: --real-image (swap driver+executor to the real project artifact), --down (tear down).
REAL=""
DOWN=0
for a in "$@"; do
  case "$a" in
    --real-image) REAL="-f docker-compose.real-image.yml" ;;
    --down) DOWN=1 ;;
    *) echo "unknown arg: $a"; exit 2 ;;
  esac
done
DC="docker compose -f docker-compose.yml ${REAL}"
[ -n "$REAL" ] && echo "== using the REAL project Spark Connect image for driver + executor =="

if [ "$DOWN" = "1" ]; then
  $DC --profile setup --profile run --profile trace down -v --remove-orphans
  echo "torn down."
  exit 0
fi

echo "== [1/6] build images (spark 4.1.2 + iceberg 1.11.0; runner) =="
$DC build spark-master runner

echo "== [2/6] start core services =="
$DC up -d db minio lakekeeper spark-master spark-worker spark-connect

echo "== [3/6] provision MinIO (bucket, prefixes, broad 'lakekeeper' user) =="
$DC run --rm minio-setup

echo "== [4/6] structural check: NO ambient/static S3 credential on the Spark path =="
{
  echo "### spark-defaults.conf grep for any static S3 key directive (expect: none)"
  # Strip comment lines first, so prose that MENTIONS these keys does not false-positive; then
  # look for an actual directive (key ending in .s3.access-key-id / .s3.secret-access-key, or a
  # spark.hadoop.fs.s3a.(access|secret).key).
  if grep -v '^[[:space:]]*#' config/spark-defaults.conf \
       | grep -Eiq '(\.s3\.(access-key-id|secret-access-key)|fs\.s3a\.(access|secret)\.key)[[:space:]]'; then
    echo "FAIL: a static S3 key directive is present in spark-defaults.conf"
  else
    echo "PASS: no static S3 key directive in spark-defaults.conf (only the vend can auth S3)"
  fi
  echo
  echo "### AWS_* env on the executor (spark-worker) and driver (spark-connect) (expect: NONE)"
  for svc in spark-worker spark-connect; do
    out="$($DC exec -T "$svc" bash -lc 'env | grep -i "AWS_\|S3_ACCESS\|S3_SECRET" || true' 2>/dev/null || true)"
    if [ -z "$out" ]; then echo "PASS: $svc has no AWS_* env"; else echo "FAIL: $svc has: $out"; fi
  done
} | tee out/structural.txt

echo "== [5/6] start best-effort S3 request trace =="
( $DC run --rm --name lkspike_trace -T minio-trace >out/minio_trace.jsonl 2>/dev/null & ) || true
sleep 2

echo "== [6/6] run the spike (provision warehouses + PASS/FAIL cases) =="
set +e
$DC run --rm runner
RC=$?
set -e

# stop trace + summarize evidence (non-fatal): show that the Iceberg DATA-file PUTs under
# tenant_a/ originated from the EXECUTOR container (spark-worker), corroborating executor-side
# FileIO with the vended credential.
docker kill lkspike_trace >/dev/null 2>&1 || true
if [ -s out/minio_trace.jsonl ]; then
  WORKER_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' lakekeeper-vending-spike-spark-worker-1 2>/dev/null || true)"
  DRIVER_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' lakekeeper-vending-spike-spark-connect-1 2>/dev/null || true)"
  echo "== trace: who wrote the Iceberg DATA files under tenant_a/ ? (worker=$WORKER_IP driver=$DRIVER_IP) =="
  WORKER_IP="$WORKER_IP" DRIVER_IP="$DRIVER_IP" python3 - <<'PY' 2>/dev/null || echo "  (trace parse skipped)"
import json,os
w=os.environ.get("WORKER_IP",""); d=os.environ.get("DRIVER_IP","")
data={}
for line in open("out/minio_trace.jsonl"):
    try: e=json.loads(line)
    except: continue
    if e.get("api")=="s3.PutObject" and "tenant_a/" in e.get("path","") and "/data/" in e.get("path",""):
        data.setdefault(e.get("client"),0); data[e["client"]]+=1
for ip,n in data.items():
    who="EXECUTOR (spark-worker)" if ip==w else ("driver (spark-connect)" if ip==d else "?")
    print(f"  {n} data-file PUT(s) from client {ip}  -> {who}")
if not data: print("  (no /data/ PUTs in trace window; see out/minio_trace.jsonl)")
PY
fi

echo
echo "== DONE (rc=$RC). Results: out/results.json ; structural: out/structural.txt =="
echo "   Tear down with: ./run.sh --down"
exit $RC
