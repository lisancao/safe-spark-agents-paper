#!/bin/sh
# =============================================================================================
# minio-setup.sh — one-shot MinIO provisioning for the LOCAL config-shakeout smoke.
# Runs inside the minio/mc container (entrypoint overridden to /bin/sh).
#
# Creates:
#   * bucket `warehouse` with two tenant prefixes tenant_a/ and tenant_b/ (each seeded with a
#     known object so the cross-tenant GET has a real target and returns AccessDenied, not 404).
#   * a dedicated MinIO user `lakekeeper` whose policy grants RW on the WHOLE bucket. This is the
#     deliberately-BROAD base identity Lakekeeper holds; it is the analog of today's fleet-wide
#     IRSA role. Isolation must come from the STS-downscoped VEND, not from this base policy.
#
# Why a dedicated user (not root `minioadmin`): MinIO refuses AssumeRole for the root/operator
# account, and Lakekeeper credential vending == calling MinIO STS AssumeRole with a prefix-scoped
# session policy. The vend downscopes THIS user's whole-bucket policy to one prefix.
# =============================================================================================
set -eu

MINIO_URL="${MINIO_URL:-http://minio:9000}"
ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
ROOT_PASS="${MINIO_ROOT_PASSWORD:-minioadmin}"
LK_USER="${LK_USER:-lakekeeper}"
LK_PASS="${LK_PASS:-lakekeeper-secret}"

echo "[minio-setup] waiting for MinIO at ${MINIO_URL} ..."
i=0
until mc alias set local "${MINIO_URL}" "${ROOT_USER}" "${ROOT_PASS}" >/dev/null 2>&1; do
  i=$((i+1)); [ "$i" -gt 60 ] && { echo "[minio-setup] MinIO never came up"; exit 1; }
  sleep 2
done
echo "[minio-setup] connected."

mc mb --ignore-existing local/warehouse

# Seed a known object under each tenant prefix (target for the cross-tenant GET test).
printf 'tenant_a seed\n' | mc pipe local/warehouse/tenant_a/_seed
printf 'tenant_b seed\n' | mc pipe local/warehouse/tenant_b/_seed

# Dedicated broad-access user for Lakekeeper's storage-credential.
mc admin user add local "${LK_USER}" "${LK_PASS}" || echo "[minio-setup] user exists, continuing"
mc admin policy create local warehouse-rw /config/minio-policy-warehouse.json || \
  echo "[minio-setup] policy exists, continuing"
mc admin policy attach local warehouse-rw --user "${LK_USER}" 2>/dev/null || \
  echo "[minio-setup] policy already attached, continuing"

echo "[minio-setup] DONE. bucket=warehouse prefixes=tenant_a/,tenant_b/ user=${LK_USER} (whole-bucket RW)"
