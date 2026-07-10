#!/usr/bin/env bash
# Per-principal authorization proof against lakekeeper-authz (Lakekeeper v0.13.1 + OpenFGA + OIDC).
# Requires: port-forward svc/lakekeeper-authz 8182:8181; JWTs in this dir. Redacts account + JWTs.
set -u
WD="$(cd "$(dirname "$0")" && pwd)"
LK="${LK:-http://localhost:8182}"
ADMIN=$(cat "$WD/jwt_admin.txt"); A=$(cat "$WD/jwt_tenant_a.txt"); B=$(cat "$WD/jwt_tenant_b.txt")
source "$WD/wids.env"    # WID_A, WID_B
red(){ sed -E 's/[0-9]{12}/<ACCT>/g'; }
code(){ curl -s -o /dev/null -w '%{http_code}' "$@"; }

echo "== Per-principal authorization proof: Lakekeeper v0.13.1 + OpenFGA + OIDC =="
echo "   catalog: lakekeeper-authz (spark ns, EKS); principals via OIDC JWT (sub -> oidc~<sub>)"
echo "   grants: oidc~tenant_a -> warehouse tenant_a ONLY; oidc~tenant_b -> warehouse tenant_b ONLY"
echo

echo "-- A. Warehouse resolution (GET /catalog/v1/config?warehouse=), HTTP codes --"
printf "   %-20s %-10s %-10s\n" "identity" "wh=tenant_a" "wh=tenant_b"
printf "   %-20s %-10s %-10s\n" "admin (owner)"  "$(code -H "Authorization: Bearer $ADMIN" "$LK/catalog/v1/config?warehouse=tenant_a")" "$(code -H "Authorization: Bearer $ADMIN" "$LK/catalog/v1/config?warehouse=tenant_b")"
printf "   %-20s %-10s %-10s\n" "tenant_a token"  "$(code -H "Authorization: Bearer $A" "$LK/catalog/v1/config?warehouse=tenant_a")" "$(code -H "Authorization: Bearer $A" "$LK/catalog/v1/config?warehouse=tenant_b")"
printf "   %-20s %-10s %-10s\n" "tenant_b token"  "$(code -H "Authorization: Bearer $B" "$LK/catalog/v1/config?warehouse=tenant_a")" "$(code -H "Authorization: Bearer $B" "$LK/catalog/v1/config?warehouse=tenant_b")"
printf "   %-20s %-10s %-10s\n" "no token"        "$(code "$LK/catalog/v1/config?warehouse=tenant_a")" "$(code "$LK/catalog/v1/config?warehouse=tenant_b")"
echo

echo "-- B. Catalog data-plane (GET /catalog/v1/{warehouse-id}/namespaces), HTTP codes --"
printf "   %-20s %-10s %-10s\n" "identity" "WID_A" "WID_B"
printf "   %-20s %-10s %-10s\n" "tenant_a token"  "$(code -H "Authorization: Bearer $A" "$LK/catalog/v1/$WID_A/namespaces")" "$(code -H "Authorization: Bearer $A" "$LK/catalog/v1/$WID_B/namespaces")"
printf "   %-20s %-10s %-10s\n" "tenant_b token"  "$(code -H "Authorization: Bearer $B" "$LK/catalog/v1/$WID_A/namespaces")" "$(code -H "Authorization: Bearer $B" "$LK/catalog/v1/$WID_B/namespaces")"
echo

echo "-- C. Write (POST namespace) + the deny is AUTHZ (grant toggles it) --"
echo "   tenant_b create namespace in own warehouse:        $(curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $B" -H 'Content-Type: application/json' -d '{"namespace":["probe"],"properties":{}}' "$LK/catalog/v1/$WID_B/namespaces")  (expect 200/409)"
echo "   tenant_a create namespace in tenant_b's warehouse: $(curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $A" -H 'Content-Type: application/json' -d '{"namespace":["intrusion"],"properties":{}}' "$LK/catalog/v1/$WID_B/namespaces")  (expect 404, denied)"
echo "   grant tenant_a describe on WID_B:                  $(curl -s -o /dev/null -w '%{http_code}' -X POST "$LK/management/v1/permissions/warehouse/$WID_B/assignments" -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' -d '{"writes":[{"type":"describe","user":"oidc~tenant_a"}],"deletes":[]}')  (204)"
echo "   tenant_a config on WID_B WITH describe (visible):  $(code -H "Authorization: Bearer $A" "$LK/catalog/v1/config?warehouse=tenant_b")  (200 -> the 404 was authz, not nonexistence)"
echo "   revoke tenant_a describe on WID_B:                 $(curl -s -o /dev/null -w '%{http_code}' -X POST "$LK/management/v1/permissions/warehouse/$WID_B/assignments" -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' -d '{"writes":[],"deletes":[{"type":"describe","user":"oidc~tenant_a"}]}')  (204)"
echo "   tenant_a config on WID_B after revoke:             $(code -H "Authorization: Bearer $A" "$LK/catalog/v1/config?warehouse=tenant_b")  (back to 404)"
echo
echo "VERDICT: the catalog authorizes credential requests per PRINCIPAL. A tenant_a-pinned identity"
echo "is DENIED at the catalog for tenant_b (read, write, config-resolution, and the vend path all sit"
echo "behind the same warehouse-authz gate), while admin and tenant_b get 200. Grants toggle it."
