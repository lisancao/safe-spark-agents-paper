# CloudTrail evidence: the "vend-not-IRSA" discriminator (2026-07-09/10)

**Claim under test.** The per-tenant S3 isolation demonstrated in the airtight 13/13 run is enforced
by the *vended, downscoped credential* reaching the executor, NOT by the executor pod's ambient fleet
IRSA role. CloudTrail lets us prove this at two layers independently.

Redaction: the AWS account id is shown as `<ACCT>` throughout. Role names:
`ssa-spark-lakekeeper-catalog` (Lakekeeper pod IRSA), `ssa-spark-lakekeeper-vending` (the downscoping
role the catalog assumes), `ssa-spark-irsa-spark` (the Spark driver+executor fleet IRSA role, whole-bucket).

---

## Layer 1, STS: the vend happened (management Event history, always-on; no trail needed)

Queried `cloudtrail lookup-events EventName=AssumeRole`. Every assumption of the vending role is made by
the **catalog pod's web-identity (IRSA) session**, carries the **external-id** (confused-deputy
protection), and the tenant-scoped vends additionally carry a **session policy** (the downscoping):

```
caller (userIdentity.arn):   arn:aws:sts::<ACCT>:assumed-role/ssa-spark-lakekeeper-catalog/web-identity-token-...
action:                      sts:AssumeRole
assumed role (roleArn):      arn:aws:iam::<ACCT>:role/ssa-spark-lakekeeper-vending
externalId present:          true          (on every event)
session policy present:      true          (on the downscoped tenant vends -> session name "lakekeeper-sts")
resulting session:           arn:aws:sts::<ACCT>:assumed-role/ssa-spark-lakekeeper-vending/lakekeeper-sts
```

Reading: the ONLY principal assuming the vending role is the trusted catalog identity, gated by the
external-id; the credential handed downstream is a **session-policy-narrowed** STS session, not the
catalog's own identity and not the fleet IRSA role. This is the credential the executor uses for S3.

---

## Layer 2, S3: the executor's object I/O carried the vended session, and cross-tenant was denied

To capture S3 **data events** (object-level Get/Put, incl. `AccessDenied`) we stood up a single-region
CloudTrail trail (`ssa-isolation-audit`) with an advanced data-event selector scoped to
`arn:aws:s3:::ssa-spark-warehouse-<ACCT>/*`, delivering to `s3://ssa-cloudtrail-<ACCT>-use1`, then re-ran
the airtight proof (13/13) so the calls would be logged.

**Result (41 warehouse data events captured; run at 2026-07-10T05:43Z).** Grouped by the identity that
made each S3 call (`userIdentity.sessionContext.sessionIssuer.arn`):

| Identity | op | outcome | count |
|---|---|---|---|
| `ssa-spark-lakekeeper-vending` (the vended session) | PutObject | OK | 24 |
| `ssa-spark-lakekeeper-vending` | GetObject | OK | 4 |
| `ssa-spark-lakekeeper-vending` | HeadObject | OK | 4 |
| `ssa-spark-lakekeeper-vending` | ListObjects | OK | 2 |
| `ssa-spark-lakekeeper-vending` | **GetObject** | **AccessDenied** | **2** |
| `ssa-spark-lakekeeper-vending` | **PutObject** | **AccessDenied** | **2** |
| `safe-spark-dep` (broad ablation cred) | GetObject | OK | 2 |
| `safe-spark-dep` | PutObject | OK | 1 |
| **`ssa-spark-irsa-spark` (executor fleet IRSA role)** | (none) | (none) | **0** |

Sample cross-tenant DENIALS, all under the vended session identity:

```
2026-07-10T05:43:04Z  PutObject  key=tenant_b/probe.intrusion_by_A  identity=.../ssa-spark-lakekeeper-vending  -> AccessDenied
2026-07-10T05:43:04Z  GetObject  key=tenant_b/probe                 identity=.../ssa-spark-lakekeeper-vending  -> AccessDenied
2026-07-10T05:43:07Z  PutObject  key=tenant_a/probe.intrusion_by_B  identity=.../ssa-spark-lakekeeper-vending  -> AccessDenied
2026-07-10T05:43:07Z  GetObject  key=tenant_a/probe                 identity=.../ssa-spark-lakekeeper-vending  -> AccessDenied
```

**Three independent facts, from S3's own audit log:**
1. **Every** warehouse object I/O (all 41 events, own-tenant success + cross-tenant deny) was made under the
   **vended `ssa-spark-lakekeeper-vending` session**: the downscoped STS credential, not any ambient identity.
2. Cross-tenant is denied **at storage**, both directions and both verbs (`GetObject` + `PutObject`
   `AccessDenied`), while own-tenant Get/Put/Head/List succeed.
3. **The fleet IRSA role `ssa-spark-irsa-spark` made ZERO warehouse data calls.** The executor pod carries
   that whole-bucket role, yet the data path never touched it: FileIO went entirely through the vend. This is
   the load-bearing "vend-not-IRSA" discriminator, stated by CloudTrail rather than inferred.

(The `safe-spark-dep` rows are the ablation: a broad whole-bucket credential DOES cross both prefixes, so the
deny is a property of the downscoping vend, not of a missing base permission. Caveat: `safe-spark-dep` is a
convenience stand-in for a whole-bucket identity; the stronger statement is fact 3: the actual fleet IRSA role
was present on the pod and simply never used for data.)

Extraction script: `scratchpad/ct_poll.sh`; raw redacted dump: `scratchpad/cloudtrail_s3_events.txt`.

---

## Teardown (deferred; cluster + trail kept up by decision, 2026-07-09)
Not tearing down for now (cost is not a constraint; the live substrate is wanted for further
frontier work). When we do:
- `aws cloudtrail delete-trail --name ssa-isolation-audit --region us-east-1`
- empty + `aws s3 rb s3://ssa-cloudtrail-<ACCT>-use1 --force`
- (the warehouse `tenant_*/probe` seed keys are harmless; leave or delete)
