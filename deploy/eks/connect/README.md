# Spark Connect server on EKS — mTLS-fronted, client-mode driver + k8s executors

This directory is the **capstone assembly**: the Kubernetes manifests that run the OSS Apache
Spark **Connect server** on EKS as a durable, **mTLS-fronted**, **client-mode driver** with Spark
launching **executor pods**, wired to **Hive Metastore + Iceberg + S3** — stitching together the
pieces built in the sibling PRs. It owns **no** image, **no** Terraform, **no** Envoy interceptor
source: it references those by path/value and assembles them into a running workload.

```
deploy/eks/connect/
├── base/                         # environment-agnostic kustomize base
│   ├── serviceaccount.yaml       # the Spark SA (IRSA-annotated → PR #5 role ARN)
│   ├── rbac.yaml                 # Role+RoleBinding: driver manages executor pods/svcs/cm
│   ├── configmap-env.yaml        # HMS/Iceberg/warehouse/image (rendered by the PR #6 image)
│   ├── deployment.yaml           # Connect-server (driver) + Envoy mTLS sidecar + PSK render init
│   ├── service-mtls.yaml         # internal NLB → Envoy mTLS port ONLY (the sole way in)
│   ├── service-headless.yaml     # driver ↔ executor RPC (cluster-internal, no 15002/15009)
│   ├── pdb.yaml                  # protect the singleton driver from casual eviction
│   ├── envoy/envoy.yaml          # k8s adaptation of PR #3's Envoy config (→ ConfigMap)
│   ├── pod-templates/executor.yaml  # executor placement: node group label + taint toleration
│   ├── secret.example.yaml       # SHAPE only of the 2 Secrets (never committed for real)
│   └── kustomization.yaml
└── overlays/example/             # one concrete environment; copy per cluster
    ├── namespace.yaml
    └── kustomization.yaml         # all REPLACE-* values from the sibling PRs' outputs
```

---

## 1. The end-to-end picture (diagram in words)

A single request, from an agent's laptop/sandbox to a row in Iceberg:

```
                            VPC-private (internal NLB; never internet-facing)
  ┌─────────────┐  client mTLS (h2)   ┌──────────────────────────────────────────────────────┐
  │ sandbox     │  sc://…:15009/        │  POD: spark-connect (one driver)                     │
  │ client      │  ;use_ssl=true        │                                                      │
  │ (PR #4)     │ ───────────────────► │  ┌───────────┐  strip+set x-connect-principal         │
  │  client     │   internal NLB        │  │  ENVOY    │  inject  authorization: Bearer <PSK>   │
  │  cert+key,  │   (TCP passthrough)   │  │ sidecar   │ ──── h2c ───►  127.0.0.1:15002         │
  │  Connect CA │                       │  │ :15009    │      (loopback ONLY)                   │
  └─────────────┘                       │  └───────────┘                  │                     │
                                        │   mTLS terminates here          ▼                     │
                                        │   (validates client cert     ┌─────────────────────┐  │
                                        │    vs Connect CA, pins        │ SPARK CONNECT SERVER│  │
                                        │    SPIFFE id → principal)     │  = client-mode      │  │
                                        │                               │  DRIVER             │  │
                                        │  PrincipalPinningInterceptor  │  (PR #6 image,      │  │
                                        │  (in image) enforces          │   role=connect-     │  │
                                        │  user_id == x-connect-principal│  server)           │  │
                                        │                               └─────────┬───────────┘  │
                                        └─────────────────────────────────────────┼─────────────┘
                                                                                  │ k8s API: create
                                  spark.driver.host = POD_IP (7078/7079)          │ executor pods
                              ┌───────────────────────────────────────────────────┼───────────┐
                              ▼                                                    ▼           │
                    ┌──────────────────┐   ┌──────────────────┐        spark-role=executor    │
                    │ EXECUTOR pod     │   │ EXECUTOR pod     │ …      :NoSchedule (PR #5      │
                    │ (same PR #6 img) │   │ (same PR #6 img) │        executor node group)    │
                    └────────┬─────────┘   └────────┬─────────┘                                │
                             │  Iceberg / HMS / S3 (same classpath + IRSA as the driver)       │
                             ▼                                                                  │
         thrift://hive-metastore.hive-metastore.svc:9083 (PR #8)   s3a://<bucket>/warehouse    │
                             │  catalog metadata                    (PR #5 bucket, IRSA RW)    │
                             └──────────────────────────────────────────────────────────────┘
```

Plain prose: **client mTLS → internal NLB → Envoy sidecar → loopback Connect server → the
interceptor pins `user_id` to the cert principal → Iceberg/HMS/S3**; executors run as pods on the
tainted executor node group, talking to the driver over the headless service and to Iceberg/HMS/S3
with the same image and the same IRSA identity.

**Raw 15002 is never exposed.** The Connect gRPC port binds `127.0.0.1` only
(`spark.connect.grpc.binding.address=127.0.0.1`); the only listener on the pod IP is Envoy's
`15009`; the only Service of type LoadBalancer is the internal NLB pointing at `15009`. There is no
path — Service, NLB, or NetworkPolicy hole — that reaches `15002`.

---

## 2. The identity flow (why a client can't lie about who it is)

Three independent checks, each catching a different forgery:

1. **mTLS at Envoy** (`require_client_certificate: true`, validated against the **Connect-layer
   CA** mounted from the `spark-connect-envoy-certs` Secret). No valid client cert ⇒ no
   connection. A `match_typed_subject_alt_names` rule additionally requires the cert's URI SAN to
   be under `spiffe://safe-spark-agents/`, bounding the blast radius of a misissued cert.
2. **Principal derivation** (Envoy Lua): Envoy **SANITIZE**s any client-supplied
   `x-connect-principal`, then **sets it itself** from the verified cert (URI SAN last segment,
   CN fallback). The client cannot inject this header — Envoy strips and replaces it.
3. **Principal pinning** (the `PrincipalPinningInterceptor` baked in the PR #6 image, FQCN
   `com.safesparkagents.connect.auth.PrincipalPinningInterceptor`, wired via
   `spark.connect.grpc.interceptor.classes`): on the server side it reads `x-connect-principal`
   (now trustworthy, set by Envoy) and the Spark Connect `user_id`, and **rejects the request if
   they differ**. So an agent that authenticates with cert `agent_42` cannot run a session as
   `user_id=agent_99`.

The **PSK** (`authorization: Bearer …`) is a *separate* secret on the loopback hop: it is the
server's `spark.connect.authenticate.token`, injected by Envoy and never seen by clients. It
proves "this request came through our Envoy", so nothing that bypasses Envoy (were 15002 ever
reachable) could talk to the server. The **same** PSK Secret feeds both ends — Envoy's bearer and
the server's token — so they always agree.

This is the cluster-level realization of `ARCHITECTURE.md`'s "each agent is a scoped principal":
the cert→principal→`user_id` chain is the identity that Unity Catalog / HMS grants then authorize.

---

## 3. How this references the sibling PRs (and what it does NOT vendor)

| PR | Dir (its branch) | What this assembly consumes | How referenced |
|----|------------------|------------------------------|----------------|
| **#6 IMAGE** | `deploy/eks/images/spark-connect` | the Iceberg Spark image; runs **both** the Connect server (`role=connect-server`) and executors; bakes the interceptor + spark-defaults template | `images:` transformer + `SPARK_KUBERNETES_CONTAINER_IMAGE` (overlay sets the pushed tag) |
| **#3 AUTH** | `deploy/auth` | Envoy mTLS contract + the `PrincipalPinningInterceptor` (jar in the #6 image) + the Connect-layer CA/cert tooling (`certs/`) | `base/envoy/envoy.yaml` is the **k8s adaptation** of `deploy/auth/envoy/envoy.yaml` (diff documented in-file); certs/PSK come from Secrets created with the #3 tooling |
| **#8 HMS** | `deploy/eks/hms` | the Hive Metastore at `thrift://hive-metastore:9083` | `configmap-env.yaml` `HMS_URIS` = the cross-namespace FQDN `thrift://hive-metastore.hive-metastore.svc.cluster.local:9083` |
| **#5 CLUSTER** | `deploy/eks/terraform` | EKS cluster, the executor node group (label `workload=spark-executor`, taint `spark-role=executor:NoSchedule`), the Spark IRSA role ARN + SA name, the warehouse bucket | overlay `patches`/`images` fill `irsa_spark.role_arn`, `warehouse_bucket`; the executor pod template targets the node group's label/taint |

**Nothing is copied.** The Envoy config is *adapted* (admin→unix socket; cert paths→Secret mount;
upstream→same-pod loopback) with its security behaviour preserved byte-for-byte and the diff
called out at the top of the file. The interceptor, the image, the metastore, the IAM roles, and
the CA all live in their owning PRs.

### Cross-namespace note (HMS)

PR #8 puts HMS in namespace `hive-metastore`; this puts Spark in `spark`. The bare service name
`hive-metastore` only resolves *within* HMS's namespace, so `HMS_URIS` uses the fully-qualified
`hive-metastore.hive-metastore.svc.cluster.local`. If you deploy HMS to a different namespace,
override `HMS_URIS` in the overlay (a commented patch is provided).

---

## 4. Catalog / storage wiring

Set by the **PR #6 image's** `spark-defaults.template.conf`, rendered at pod start from the env
this directory supplies (`configmap-env.yaml` + the PSK Secret). The acceptance-contract knobs:

- **Iceberg over HMS** — `spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog`,
  `.type=hive`, `.uri=$HMS_URIS`, `.warehouse=s3a://<bucket>/warehouse`;
  `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`;
  session catalog is HMS-backed (`spark.sql.catalogImplementation=hive`).
- **S3A via IRSA** — `fs.s3a.aws.credentials.provider=…WebIdentityTokenFileCredentialsProvider`;
  **no static keys**. The IRSA webhook injects `AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE` into
  every pod using the `spark` SA (driver and executors alike).
- **Connect auth** — `spark.connect.grpc.interceptor.classes=…PrincipalPinningInterceptor` and
  `spark.connect.authenticate.token=<PSK from the Secret>`.

The k8s-runtime knobs the template leaves open are passed as `--conf` args in `deployment.yaml`
(master, deploy mode, container image, executor pod template, driver host/ports, dynamic
allocation). See "client-mode specifics" below.

---

## 5. Client-mode Spark-on-Kubernetes specifics

- `spark.master=k8s://https://kubernetes.default.svc`, `spark.submit.deployMode=client` — the
  Connect server *is* the driver, running in the pod; Spark asks the API server to create
  executors.
- `spark.driver.host=$(POD_IP)` (downward API), `spark.driver.bindAddress=0.0.0.0`, fixed
  `spark.driver.port=7078` / `spark.blockManager.port=7079` — so executors can reach the driver
  and the **headless service** can publish those ports. (Note these are *separate* from the
  loopback-bound Connect gRPC `15002` — the driver's Spark RPC must be routable; the Connect
  endpoint must not.)
- `spark.kubernetes.container.image=$(SPARK_KUBERNETES_CONTAINER_IMAGE)` — executors use the
  **same PR #6 image** as the driver, so they share the Iceberg/S3A/Kafka classpath.
- `spark.kubernetes.authenticate.driver.serviceAccountName=spark` (from the image template) +
  `rbac.yaml` — the driver authenticates as the `spark` SA and is granted pods/services/configmaps
  `create,get,list,watch,delete` in its namespace.
- `spark.kubernetes.executor.podTemplateFile=/opt/spark/pod-templates/executor.yaml` — adds the
  `nodeSelector: workload=spark-executor` + the `spark-role=executor:NoSchedule` toleration so
  executors land on **PR #5's executor node group**. The driver has no toleration, so it stays on
  the untainted system pool.
- `spark.kubernetes.driver.pod.name=$(POD_NAME)` — executor pods get an ownerReference to the
  driver pod, so they are garbage-collected if the driver dies.
- **Dynamic allocation: ENABLED** (`minExecutors=0`, `maxExecutors=10`, `initialExecutors=0`,
  `shuffleTracking.enabled=true`). Justification: a long-lived, multi-tenant Connect server is idle
  between agent queries; scaling executors to **zero** lets the cluster-autoscaler drain the
  executor nodes when no one is querying, and burst on load. `shuffleTracking` replaces the
  external shuffle service, which isn't available on k8s. Turn it off (set a fixed
  `spark.executor.instances`) only if you want warm executors for latency.

---

## 6. Secret wiring (referenced, never committed)

Two Secrets, **not** listed in any `kustomization.yaml`, so `kustomize build` never emits a
placeholder credential. `.gitignore` blocks real `secret*.yaml` / `*.key` / `*.crt` / `*.token`.
Create them out-of-band (prefer **External Secrets Operator** / the **Secrets Store CSI driver**
so AWS rotation flows through). By hand, **`--from-file` for sensitive values, never
`--from-literal`** (a literal lands on the process argv and in shell history):

```bash
# 1) PSK — shared by Envoy (Bearer header) and the Connect server (authenticate.token).
openssl rand -base64 36 | tr '+/' '-_' | tr -d '=\n' > psk.token   # base64url, single line, no '|'
kubectl -n spark create secret generic spark-connect-psk --from-file=token=psk.token
shred -u psk.token

# 2) Envoy mTLS material — issued by the PR #3 tooling (deploy/auth/certs/issue-all.sh):
kubectl -n spark create secret generic spark-connect-envoy-certs \
  --from-file=server.crt=server.crt \
  --from-file=server.key=server.key \
  --from-file=connect-ca.crt=connect-ca.crt
```

`secret.example.yaml` documents the exact shape/keys. The cert volume mounts `defaultMode: 0444`
because Envoy runs as uid 101 and must read `server.key` — a stricter mode makes BoringSSL fail to
load the key (verified during the Envoy validate gate, see below).

---

## 7. How a sandbox client (PR #4) connects

The PR #4 thin client changes **one URL** (per `connect/client.py` and `ARCHITECTURE.md`). Against
this deployment it sets:

```
SPARK_REMOTE="sc://<internal-nlb-dns-or-name>:15009/;use_ssl=true;token=<unused-by-server>;user_id=agent_42"
```

plus its **client cert + key** and the **Connect CA** (the client must trust Envoy's server cert,
and present a cert whose URI SAN is `spiffe://safe-spark-agents/agent_42`). The cert's principal
(`agent_42`) is what Envoy pins and the interceptor enforces against `user_id` — so `user_id` in
the string must match the cert, or the request is rejected. The server's PSK is injected by Envoy,
so the client never needs the real PSK (any `token=` it sends is overwritten). Reads/writes are
then governed by HMS/UC grants on that principal exactly as in the local reference architecture.

---

## 8. Gates (run here; NOT deployed to a real cluster)

All four pass on the build host (tooling installed locally where missing):

| Gate | Command | Result |
|------|---------|--------|
| `kustomize build` | `kustomize build base` / `overlays/example` | **OK** (10 / 11 docs) |
| `kubeconform -strict` | `kustomize build overlays/example \| kubeconform -strict -summary` | **OK** — 11/11 valid, 0 invalid |
| `yamllint` | `yamllint -c .yamllint.yaml .` | **OK** (exit 0) |
| `envoy --mode validate` | `docker run --rm -v …/envoy.yaml:… -v <certs>:/etc/envoy/certs envoyproxy/envoy:v1.31.5 --mode validate -c …` | **`configuration OK`** |

Notes, honestly:
- **No host `envoy` binary**, so the Envoy gate ran via the `envoyproxy/envoy:v1.31.5` image (the
  same tag the sidecar uses). `--mode validate` *instantiates the listener*, so it actually loads
  the TLS cert files — it is not a syntax-only check. To exercise it we mounted **throwaway
  self-signed certs** (generated in scratch, gitignored, never committed) at `/etc/envoy/certs`;
  the real certs come from the Secret at runtime. This surfaced two real facts now baked into the
  manifests: the key must be **world/owner-readable by uid 101** (⇒ `defaultMode: 0444`), and the
  `__CONNECT_PSK__` placeholder validates fine pre-render (it's a valid string).
- `kubeconform` validates the **built kustomize output** (the API objects). The embedded
  `envoy.yaml` / `executor.yaml` are opaque ConfigMap strings to it — they are covered by the
  Envoy gate and by Spark at runtime respectively.
- `commonLabels` emits a deprecation warning (matching the PR #8 HMS base convention); harmless.
- **Not deployed.** No `kubectl apply`, no live cluster — per the task, gates only.

### Reproduce the gates

```bash
cd deploy/eks/connect
kustomize build base       >/dev/null && echo "base OK"
kustomize build overlays/example | kubeconform -strict -summary
yamllint -c .yamllint.yaml .
# Envoy (needs docker; certs are throwaway, for validation only):
D=$(mktemp -d); openssl req -x509 -newkey rsa:2048 -nodes -keyout "$D/connect-ca.key" \
  -out "$D/connect-ca.crt" -days 1 -subj "/CN=test-ca" 2>/dev/null
openssl ecparam -name prime256v1 -genkey -noout -out "$D/server.key"
openssl req -new -key "$D/server.key" -subj "/CN=test" -out "$D/s.csr" 2>/dev/null
openssl x509 -req -in "$D/s.csr" -CA "$D/connect-ca.crt" -CAkey "$D/connect-ca.key" \
  -CAcreateserial -days 1 -out "$D/server.crt" 2>/dev/null; chmod 644 "$D"/*
docker run --rm -v "$PWD/base/envoy/envoy.yaml:/c.yaml:ro" -v "$D:/etc/envoy/certs:ro" \
  envoyproxy/envoy:v1.31.5 --mode validate -c /c.yaml; rm -rf "$D"
```

---

## 9. Deploy (once the sibling PRs are applied to a real cluster)

```bash
# Prereqs (other PRs): EKS up (PR #5), AWS Load Balancer Controller installed, HMS running (PR #8),
# the PR #6 image pushed to ECR, the two Secrets created (§6).
cp -r overlays/example overlays/prod          # edit REPLACE-* from `terraform output`
kustomize build overlays/prod | kubectl apply -f -
kubectl -n spark rollout status deploy/spark-connect
kubectl -n spark get svc spark-connect-mtls -o wide   # the internal NLB DNS for clients
```

The overlay is the only file you edit per environment. Every `REPLACE-*` / `000000000000`
placeholder maps to a `terraform output` from PR #5 (role ARN, warehouse bucket, region/account)
or your ECR repo for the PR #6 image.

---

## 10. Version note: 4.1 → 4.2 / Tier-B (AUTO CDC)

This assembly is **format/engine-agnostic** — it only references the PR #6 image tag, so the
version story is entirely PR #6's (see `deploy/eks/images/README.md`):

- **Tier A (default):** `apache/spark:4.1.2` + released `iceberg-spark-runtime-4.1_2.13:1.11.0`.
  SCD is hand-rolled MERGE (Iceberg implements `SupportsRowLevelOperations`). This is what these
  manifests target.
- **Tier B (AUTO CDC):** Spark `master`/`5.0.0-SNAPSHOT` + the iceberg-port, unlocking native
  `create_auto_cdc_flow`. **No released Iceberg runtime exists for Spark 4.2/5.0 yet**, so Tier B
  is a source build.

To move tiers, change **only** the image reference: set the overlay's `images:` newTag (and the
matching `SPARK_KUBERNETES_CONTAINER_IMAGE`) to the Tier-B tag and re-apply. **No manifest in this
directory changes** — the driver/executor/Envoy/RBAC/Service topology is identical across tiers;
the interceptor is rebuilt against the new Spark Connect API as part of the PR #6 image, not here.
Track the two upstreams (Spark 4.2/5.0 GA, the first official `iceberg-spark-runtime-4.2`/`5.0`) to
retire Tier B for released bits.
