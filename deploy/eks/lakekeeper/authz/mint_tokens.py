#!/usr/bin/env python3
"""Mint the static OIDC artifacts for the per-principal authz demo:
  jwks.json, openid-configuration, priv.pem, and jwt_{admin,tenant_a,tenant_b}.txt

The IdP is a static nginx (00-idp-openfga.yaml) that just publishes discovery + JWKS so Lakekeeper
can verify these RS256 bearer JWTs. ISSUER must EXACTLY equal LAKEKEEPER__OPENID_PROVIDER_URI and the
`iss` in every token. Long TTL is a demo convenience (a security smell in production).

Deps:  pip install "pyjwt>=2.8" cryptography
Secrets: priv.pem and jwt_*.txt are secrets -- keep them out of git.
"""
import json, time, jwt
from jwt.algorithms import RSAAlgorithm
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

ISSUER = "http://idp-authz.spark.svc.cluster.local"   # == LAKEKEEPER__OPENID_PROVIDER_URI
AUDIENCE = "lakekeeper"
KID = "demo-key-1"
TTL = 10 * 365 * 24 * 3600
SUBJECTS = ("admin", "tenant_a", "tenant_b")           # Lakekeeper principal id = "oidc~<sub>"

key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
open("priv.pem", "wb").write(key.private_bytes(
    serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))

jwk = json.loads(RSAAlgorithm.to_jwk(key.public_key()))
jwk.update({"kid": KID, "use": "sig", "alg": "RS256"})
json.dump({"keys": [jwk]}, open("jwks.json", "w"))
json.dump({
    "issuer": ISSUER, "jwks_uri": ISSUER + "/jwks.json",
    "authorization_endpoint": ISSUER + "/authorize", "token_endpoint": ISSUER + "/token",
    "response_types_supported": ["id_token", "token"], "subject_types_supported": ["public"],
    "id_token_signing_alg_values_supported": ["RS256"],
}, open("openid-configuration", "w"))

now = int(time.time())
for sub in SUBJECTS:
    tok = jwt.encode({"iss": ISSUER, "sub": sub, "aud": AUDIENCE, "iat": now, "exp": now + TTL},
                     key, algorithm="RS256", headers={"kid": KID})
    open(f"jwt_{sub}.txt", "w").write(tok)
print("wrote: jwks.json openid-configuration priv.pem " + " ".join(f"jwt_{s}.txt" for s in SUBJECTS))
print("issuer =", ISSUER)
