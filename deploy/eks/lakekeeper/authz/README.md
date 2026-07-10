# Per-principal catalog authorization (Lakekeeper v0.13.1 + OpenFGA + OIDC)

Closes the isolation frontier's *authorization* half: a Spark/agent identity pinned to `tenant_a`
is **denied at the catalog** when it requests `tenant_b`'s warehouse or credential, while `tenant_b`
and the admin are allowed. This layers a catalog-plane per-principal gate on top of the S3
prefix-scoped **data-plane** isolation proven in `../spike/` (see `PLATFORM_LAB_NOTEBOOK.md`).

This is a **fresh, isolated** stack deployed alongside the allow-all Lakekeeper. Do NOT flip
`AUTHZ_BACKEND` in place: warehouses created under `allowall` have no OpenFGA ownership tuples and
become ungovernable.

## Components (namespace `spark`)
- `idp-authz` (nginx) -- static OIDC discovery doc + JWKS; serves hand-minted RS256 JWTs' verification.
- `openfga` (v1.14) -- the authorization backend (memory store for the demo; use Postgres to persist).
- `lakekeeper-authz` (v0.13.1) -- fresh instance, own DB `catalog_authz`, ServiceAccount `lakekeeper`
  (reuses the vending IRSA). `AUTHZ_BACKEND=openfga`, OIDC pointed at `idp-authz`.

## Secrets (NEVER committed -- kept in the session scratchpad)
- RS256 private key + the three JWTs (`admin`, `tenant_a`, `tenant_b`); mint with `mint_tokens.py`.
- `LAKEKEEPER__PG_ENCRYPTION_KEY` (fresh, any 32 bytes) and the warehouse `external-id`.

## Runbook
```bash
# 0. mint keys + JWTs + discovery/JWKS, create the configmap
python3 mint_tokens.py                       # writes jwks.json, openid-configuration, jwt_*.txt, priv.pem
kubectl -n spark create configmap idp-authz-files \
  --from-file=openid-configuration --from-file=jwks.json

# 1-2. deploy IdP + OpenFGA (IdP first: OIDC discovery is a Lakekeeper startup gate)
kubectl apply -f 00-idp-openfga.yaml
kubectl -n spark rollout status deploy/idp-authz deploy/openfga

# 3. fresh DB + migrate (installs the OpenFGA store + model v4.7)
kubectl -n spark exec deploy/lakekeeper-postgres -- psql -U postgres -c 'CREATE DATABASE catalog_authz'
#   fill PG_ENCRYPTION_KEY placeholders in 10-lakekeeper-authz.yaml first
kubectl apply -f 10-lakekeeper-authz.yaml    # migrate Job + serve Deployment + Service
kubectl -n spark wait --for=condition=complete job/lakekeeper-authz-migrate --timeout=150s
kubectl -n spark rollout status deploy/lakekeeper-authz
kubectl -n spark logs deploy/lakekeeper-authz | grep -iE 'openid|openfga'   # confirm both active

# 4. bootstrap + warehouses + grants  (LK via: kubectl -n spark port-forward svc/lakekeeper-authz 8182:8181)
LK=http://localhost:8182 ADMIN=$(cat jwt_admin.txt)
curl -sf -X POST $LK/management/v1/bootstrap -H "Authorization: Bearer $ADMIN" \
  -H 'content-type: application/json' -d '{"accept-terms-of-use":true,"is-operator":true}'
for s in tenant_a tenant_b; do
  curl -sf -X POST $LK/management/v1/user -H "Authorization: Bearer $ADMIN" -H 'content-type: application/json' \
    -d "{\"id\":\"oidc~$s\",\"name\":\"$s\",\"user-type\":\"application\",\"update-if-exists\":true}"
  curl -sf -X POST $LK/management/v1/warehouse -H "Authorization: Bearer $ADMIN" -H 'content-type: application/json' \
    -H 'x-project-id: 00000000-0000-0000-0000-000000000000' --data @../spike/eks/warehouse-$s.aws.json   # fill REPLACE-* first
done
# grant each tenant ONLY its own warehouse (WID from GET /management/v1/warehouse):
#   POST $LK/management/v1/permissions/warehouse/<WID>/assignments
#   {"writes":[{"type":"select","user":"oidc~tenant_a"},{"type":"describe",...},{"type":"modify",...},{"type":"create",...}],"deletes":[]}

# 5. prove it
./authz_proof.sh            # tenant_a -> own=200, other=404; admin=200/200; no-token=401; grant toggles the deny
```

## Expected result
```
identity        wh=tenant_a  wh=tenant_b
admin (owner)   200          200
tenant_a token  200          404   <- denied at the catalog
tenant_b token  404          200
no token        401          401
```
`404` = existence hidden for a zero-relation principal (grant a `describe` to see a clean `200`, i.e.
the deny is authorization, not nonexistence). Full evidence: `paper/notes/proof_2026-07-10_perprincipal_authz.log`.

## Multi-server Connect (token custody + per-tenant execution isolation)

Beyond catalog authorization, stand up ONE Spark Connect server per tenant, each injecting only that
tenant's catalog token server-side. A client never holds or chooses the token, and each tenant's
compute runs on its own executor pods.

```bash
# one server per tenant (JWT injected as a static Authorization header on catalog `lk`)
export CONNECT_IMAGE=<your-ecr>/ssa-spark/spark-connect:4.1.2-iceberg1.11.0
python3 gen_connect_server.py tenant_a jwt_tenant_a.txt | kubectl apply -f -
python3 gen_connect_server.py tenant_b jwt_tenant_b.txt | kubectl apply -f -

# connect (the image's principal-pinning interceptor requires the pinned principal + PSK):
#   sc://<svc>:15002/;x-connect-principal=tenant_a;token=<spark-connect-psk>;user_id=tenant_a
```

**Proves (see `paper/notes/proof_2026-07-10_multiserver.log`):**
- **Token custody** -- a session on tenant_a's server writes/reads tenant_a; reaching tenant_b is refused
  (`NotAuthorized: Missing Authorization Header`) because the client holds no tenant_b credential.
- **Execution isolation** -- tenant_a's executor pods (distinct IPs, own driver app) are disjoint from
  tenant_b's; the two tenants' compute never shares a JVM. Pods carry label `tenant=<t>`.

**Remaining seam:** per-principal ingress routing (bind each authenticated principal to its own server at
the Envoy mTLS gateway) -- a per-principal upstream config on the demonstrated cert-SAN pinning (P1).

## Per-principal ingress routing (gateway Envoy)

The last seam: bind each authenticated principal to its OWN Connect server so a client cannot connect to
another tenant's server. A gateway Envoy terminates client mTLS, derives the principal from the client-cert
URI-SAN, and routes on it.

```bash
# certs: reuse the Connect CA + per-tenant client certs (SAN spiffe://safe-spark-agents/<tenant>)
kubectl -n spark create secret generic connect-gateway-certs \
  --from-file=server.crt --from-file=server.key --from-file=connect-ca.crt
kubectl -n spark create configmap connect-gateway-config --from-file=envoy.yaml=gateway-envoy.yaml
kubectl apply -f 20-connect-gateway.yaml
```

**Proves (`paper/notes/proof_2026-07-10_ingress_routing.log`):**
- tenant_a client cert  -> routed to `spark-connect-tenant-a` (only)
- tenant_b client cert  -> routed to `spark-connect-tenant-b` (only)
- un-granted principal   -> `403` (no tenant route)
- no client cert         -> TLS handshake refused

There is no route that sends a tenant_a cert to tenant_b's server: the authenticated identity, not the
client's choice, selects the server. (Gotcha: forward the gateway on a local port other than 15009 if an old
`spark-connect-mtls` port-forward is still bound there, or probes will hit the wrong proxy.)
