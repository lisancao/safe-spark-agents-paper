#!/usr/bin/env python3
# Per-tenant Spark Connect server generator (multi-server Connect: token custody + execution
# isolation). Each server injects ONLY its tenant's catalog JWT server-side. Set CONNECT_IMAGE to the
# spark-connect image. usage: CONNECT_IMAGE=<img> gen_connect_server.py <tenant> <jwt_file> | kubectl apply -f -
# Emit (as JSON, which kubectl apply accepts) a per-tenant Spark Connect server that INJECTS that
# tenant's catalog token server-side. usage: gen_connect_server.py <tenant> <jwt_file>  > out.json
import json, sys
T = sys.argv[1]
JWT = open(sys.argv[2]).read().strip()
NAME = "spark-connect-" + T.replace("_", "-")   # k8s DNS-1123 name (no underscores)
IMG = os.environ.get("CONNECT_IMAGE", "<ECR>/ssa-spark/spark-connect:4.1.2-iceberg1.11.0")  # set CONNECT_IMAGE

conf = {
    "spark.connect.grpc.binding.address": "0.0.0.0",
    "spark.connect.grpc.binding.port": "15002",
    "spark.master": "k8s://https://kubernetes.default.svc",
    "spark.submit.deployMode": "client",
    "spark.kubernetes.namespace": "$(POD_NAMESPACE)",
    "spark.kubernetes.container.image": "$(SPARK_KUBERNETES_CONTAINER_IMAGE)",
    "spark.kubernetes.driver.pod.name": "$(POD_NAME)",
    "spark.kubernetes.executor.podTemplateFile": "/opt/spark/pod-templates/executor.yaml",
    "spark.kubernetes.executor.label.tenant": T,
    "spark.driver.host": "$(POD_IP)",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.port": "7078",
    "spark.blockManager.port": "7079",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.shuffleTracking.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.dynamicAllocation.maxExecutors": "4",
    "spark.dynamicAllocation.initialExecutors": "1",
    "spark.executor.cores": "1",
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g",
    "spark.sql.catalog.lk": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lk.type": "rest",
    "spark.sql.catalog.lk.uri": "http://lakekeeper-authz:8181/catalog",
    "spark.sql.catalog.lk.warehouse": T,
    "spark.sql.catalog.lk.header.Authorization": "Bearer " + JWT,
    "spark.sql.catalog.lk.header.X-Iceberg-Access-Delegation": "vended-credentials",
    "spark.sql.catalog.lk.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.defaultCatalog": "lk",
}
args = ["connect-server"]
for k, v in conf.items():
    args += ["--conf", f"{k}={v}"]

dep = {
    "apiVersion": "apps/v1", "kind": "Deployment",
    "metadata": {"name": NAME, "namespace": "spark", "labels": {"app": NAME, "tenant": T}},
    "spec": {
        "replicas": 1, "strategy": {"type": "Recreate"},
        "selector": {"matchLabels": {"app": NAME}},
        "template": {
            "metadata": {"labels": {"app": NAME, "tenant": T}},
            "spec": {
                "serviceAccountName": "spark",
                "containers": [{
                    "name": "connect", "image": IMG,
                    "envFrom": [{"configMapRef": {"name": "spark-connect-env"}}],
                    "env": [
                        {"name": "POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}},
                        {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
                        {"name": "POD_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}},
                        {"name": "CONNECT_AUTH_TOKEN", "valueFrom": {"secretKeyRef": {"name": "spark-connect-psk", "key": "token"}}},
                    ],
                    "args": args,
                    "ports": [{"containerPort": 15002, "name": "grpc"}],
                    "volumeMounts": [{"name": "executor-podtemplate", "mountPath": "/opt/spark/pod-templates", "readOnly": True}],
                    "readinessProbe": {"tcpSocket": {"port": 15002}, "initialDelaySeconds": 15, "periodSeconds": 10, "failureThreshold": 12},
                }],
                "volumes": [{"name": "executor-podtemplate", "configMap": {"name": "spark-connect-executor-podtemplate"}}],
            },
        },
    },
}
svc = {
    "apiVersion": "v1", "kind": "Service",
    "metadata": {"name": NAME, "namespace": "spark"},
    "spec": {"selector": {"app": NAME}, "ports": [{"port": 15002, "targetPort": 15002, "name": "grpc"}]},
}
print(json.dumps({"apiVersion": "v1", "kind": "List", "items": [dep, svc]}))
