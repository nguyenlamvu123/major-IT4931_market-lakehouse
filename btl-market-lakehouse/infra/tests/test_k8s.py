"""Static checks of the Kubernetes manifests against contracts/infra.yaml."""

from __future__ import annotations

from pathlib import Path

import yaml

from common.contracts import KAFKA_BOOTSTRAP_K8S, TOPICS
from infra.scripts.gen_kafka_topics import OUTPUT, render

INFRA = Path(__file__).resolve().parents[1]
K8S = INFRA / "k8s"
CONTRACT_INFRA = yaml.safe_load(
    (INFRA.parent / "contracts" / "infra.yaml").read_text(encoding="utf-8")
)


def _documents() -> list[tuple[Path, dict]]:
    found: list[tuple[Path, dict]] = []
    for path in sorted(K8S.rglob("*.yaml")):
        if path.name == "kustomization.yaml" or "helm" in path.parts:
            continue
        for document in yaml.safe_load_all(path.read_text(encoding="utf-8")):
            if document:
                found.append((path, document))
    return found


def _by_kind(kind: str) -> list[dict]:
    return [document for _, document in _documents() if document["kind"] == kind]


def _containers(document: dict) -> list[dict]:
    spec = document.get("spec", {})
    template = spec.get("template") or spec.get("jobTemplate", {}).get("spec", {}).get("template")
    if not template:
        return []
    return template["spec"].get("containers", [])


def test_generated_topics_match_contract() -> None:
    # Regression guard: the KafkaTopic file is generated, never hand-edited.
    assert OUTPUT.read_text(encoding="utf-8") == render()
    topics = _by_kind("KafkaTopic")
    assert {topic["spec"]["topicName"] for topic in topics} == set(TOPICS)
    for topic in topics:
        spec = TOPICS[topic["spec"]["topicName"]]
        assert topic["spec"]["partitions"] == spec["partitions"]
        assert topic["spec"]["config"]["retention.ms"] == spec["retention_ms"]
        assert topic["metadata"]["labels"]["strimzi.io/cluster"] == "market"


def test_namespaces_are_the_contract_namespaces() -> None:
    declared = {document["metadata"]["name"] for document in _by_kind("Namespace")}
    assert declared == set(CONTRACT_INFRA["namespaces"])
    for path, document in _documents():
        namespace = document["metadata"].get("namespace")
        if document["kind"] != "Namespace":
            assert namespace in declared, path


def test_kafka_cluster_matches_bootstrap_address() -> None:
    (kafka,) = _by_kind("Kafka")
    assert kafka["metadata"]["name"] == "market"
    assert kafka["metadata"]["namespace"] == "kafka"
    assert kafka["metadata"]["annotations"]["strimzi.io/kraft"] == "enabled"
    assert "kafkaExporter" in kafka["spec"]
    # Strimzi names the bootstrap service <cluster>-kafka-bootstrap.
    name, namespace = KAFKA_BOOTSTRAP_K8S.split(".")[:2]
    assert name == "market-kafka-bootstrap"
    assert namespace == "kafka"
    assert CONTRACT_INFRA["services"]["kafka_bootstrap"] == KAFKA_BOOTSTRAP_K8S


def test_cassandra_cluster_matches_service_address() -> None:
    (cluster,) = _by_kind("K8ssandraCluster")
    datacenter = cluster["spec"]["cassandra"]["datacenters"][0]["metadata"]["name"]
    service = f"{cluster['metadata']['name']}-{datacenter}-service.{cluster['metadata']['namespace']}.svc:9042"
    assert service == CONTRACT_INFRA["services"]["cassandra"]


def test_schema_comes_from_contract_file() -> None:
    kustomization = yaml.safe_load((K8S / "kustomization.yaml").read_text(encoding="utf-8"))
    generators = {item["name"]: item for item in kustomization["configMapGenerator"]}
    assert generators["cassandra-schema"]["files"] == ["cassandra.cql=../../contracts/cassandra.cql"]
    for _, document in _documents():
        assert "CREATE TABLE" not in yaml.safe_dump(document)


def test_btl_images_are_dev_tagged_and_not_pulled() -> None:
    images = set(CONTRACT_INFRA["images"])
    for path, document in _documents():
        for container in _containers(document):
            image = container["image"]
            assert ":latest" not in image or image.startswith("minio/mc"), path
            if image.startswith("btl/"):
                assert image in images, path
                assert container["imagePullPolicy"] == "IfNotPresent", path


def test_market_api_points_at_contract_services_without_demo_data() -> None:
    (deployment,) = [d for d in _by_kind("Deployment") if d["metadata"]["name"] == "market-api"]
    env = {item["name"]: item.get("value") for item in _containers(deployment)[0]["env"]}
    assert env["CASSANDRA_HOSTS"] + ":9042" == CONTRACT_INFRA["services"]["cassandra"]
    assert env["DEMO_FALLBACK"] == "false"


def test_spark_template_follows_infra_contract() -> None:
    template = yaml.safe_load((K8S / "spark" / "sparkapplication-template.yaml").read_text())
    assert template["metadata"]["namespace"] == "spark"
    service_account = CONTRACT_INFRA["namespaces"]["spark"]["service_account"]
    assert template["spec"]["driver"]["serviceAccount"] == service_account
    assert template["spec"]["imagePullPolicy"] == "IfNotPresent"
    conf = template["spec"]["sparkConf"]
    assert conf["spark.hadoop.fs.s3a.endpoint"] == CONTRACT_INFRA["services"]["minio"]


def test_helm_values_follow_contract() -> None:
    helm = K8S / "helm"
    airflow = yaml.safe_load((helm / "airflow-values.yaml").read_text())
    assert airflow["executor"] == "KubernetesExecutor"
    assert airflow["images"]["airflow"] == {
        "repository": "btl/airflow",
        "tag": "dev",
        "pullPolicy": "IfNotPresent",
    }
    assert airflow["postgresql"]["enabled"] is False
    spark = yaml.safe_load((helm / "spark-operator-values.yaml").read_text())
    assert spark["spark"]["jobNamespaces"] == ["spark"]
    assert spark["spark"]["serviceAccount"]["name"] == "spark-operator-spark"
    stack = yaml.safe_load((helm / "kube-prometheus-stack-values.yaml").read_text())
    assert "yesoreyeram-infinity-datasource" in stack["grafana"]["plugins"]
    infinity = stack["grafana"]["additionalDataSources"][0]
    assert infinity["uid"] == "infinity"
    assert infinity["url"] == "http://" + CONTRACT_INFRA["services"]["market_api"]


def test_airflow_rbac_binds_worker_service_account() -> None:
    (binding,) = _by_kind("RoleBinding")
    assert binding["metadata"]["namespace"] == "spark"
    assert binding["subjects"] == [
        {"kind": "ServiceAccount", "name": "airflow-worker", "namespace": "airflow"}
    ]


def test_kafka_version_is_the_same_on_compose_and_kubernetes() -> None:
    (kafka,) = _by_kind("Kafka")
    compose = yaml.safe_load((INFRA / "docker-compose.dev.yml").read_text(encoding="utf-8"))
    assert compose["services"]["kafka"]["image"] == f"apache/kafka:{kafka['spec']['kafka']['version']}"
    versions = (K8S / "helm" / "versions.env").read_text(encoding="utf-8")
    # Strimzi 0.45 is the operator line that runs Kafka 3.9.0 (role A's KafkaConnect).
    assert "STRIMZI_CHART_VERSION=0.45.0" in versions
    assert kafka["spec"]["kafka"]["version"] == "3.9.0"
