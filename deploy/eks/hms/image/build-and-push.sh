#!/usr/bin/env bash
# Build the Postgres+S3A Hive Metastore image and (optionally) push it to ECR.
#
# Usage:
#   ./build-and-push.sh                       # build only, tag hive-metastore-pg:4.0.1
#   REGISTRY=<acct>.dkr.ecr.<region>.amazonaws.com ./build-and-push.sh --push
#
# REGISTRY comes from eks-cluster-iac output `ecr_repo_url` (minus the image name). The pushed
# reference must match overlays/<env>/kustomization.yaml `images[].newName`.
set -euo pipefail
cd "$(dirname "$0")"

IMAGE_NAME="${IMAGE_NAME:-hive-metastore-pg}"
HIVE_VERSION="${HIVE_VERSION:-4.0.1}"
TAG="${TAG:-${HIVE_VERSION}}"
REGISTRY="${REGISTRY:-}"

local_ref="${IMAGE_NAME}:${TAG}"
echo ">> building ${local_ref} (apache/hive:${HIVE_VERSION} base)"
docker build --build-arg HIVE_VERSION="${HIVE_VERSION}" -t "${local_ref}" .

if [[ "${1:-}" == "--push" ]]; then
  [[ -n "${REGISTRY}" ]] || { echo "REGISTRY env required for --push" >&2; exit 1; }
  remote_ref="${REGISTRY}/${IMAGE_NAME}:${TAG}"
  echo ">> tagging + pushing ${remote_ref}"
  # ECR login (no-op if already authed): aws ecr get-login-password | docker login ...
  docker tag "${local_ref}" "${remote_ref}"
  docker push "${remote_ref}"
  echo ">> pushed ${remote_ref}"
else
  echo ">> built ${local_ref} (not pushed; pass --push with REGISTRY set)"
fi
