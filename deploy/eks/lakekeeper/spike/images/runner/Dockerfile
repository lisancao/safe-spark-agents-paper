# syntax=docker/dockerfile:1.7
# Test runner for the spike: the Spark Connect PYTHON client (no local JVM) + boto3 for the
# storage-layer credential checks + urllib (stdlib) for the Lakekeeper REST provisioning/vend.
# Pinned to the same Spark line as the server (4.1.2) so the Connect client/server handshake matches.
FROM python:3.11-slim

RUN pip install --no-cache-dir \
      "pyspark[connect]==4.1.2" \
      "boto3>=1.34,<2"

WORKDIR /spike
ENTRYPOINT ["python", "run_spike.py"]
