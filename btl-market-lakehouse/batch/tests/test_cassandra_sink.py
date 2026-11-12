"""Gold -> Cassandra must write exactly the contract columns with the contract types."""

from __future__ import annotations

from datetime import date

import pytest

from common.cql import load_tables

#: Spark DDL type -> CQL type, for the types the gold tables use.
DDL_TO_CQL = {
    "string": "text",
    "date": "date",
    "timestamp": "timestamp",
    "int": "int",
    "long": "bigint",
    "double": "double",
    "boolean": "boolean",
}


def _ddl_columns(ddl: str) -> dict[str, str]:
    return dict(item.split() for item in ddl.split(", "))


def test_gold_ddl_covers_every_contract_column() -> None:
    pytest.importorskip("pyspark")
    from batch.cassandra_sink import SINK_TABLES
    from batch.gold import GOLD_DDL

    tables = load_tables()
    for name in SINK_TABLES:
        gold = _ddl_columns(GOLD_DDL[name])
        for column, cql_type in tables[name].columns.items():
            assert column in gold, (name, column)
            assert DDL_TO_CQL[gold[column]] == cql_type, (name, column)


def test_prepared_gold_frames_match_cassandra_contract(spark, silver_lake) -> None:
    from batch.cassandra_sink import CQL_TO_SPARK, SINK_TABLES, prepare
    from batch.gold import GOLD_DDL, build_gold

    gold = build_gold(spark, silver_lake, [date(2026, 11, 25)])
    gold["asset_clusters"] = spark.createDataFrame([], GOLD_DDL["asset_clusters"])
    tables = load_tables()
    for name in SINK_TABLES:
        prepared = prepare(gold[name], name)
        contract = tables[name].columns
        assert prepared.columns == list(contract), name
        for field in prepared.schema.fields:
            assert field.dataType == CQL_TO_SPARK[contract[field.name]], (name, field.name)
        if name != "asset_clusters":
            assert prepared.count() > 0, name
        # Primary-key columns are never NULL, otherwise Cassandra rejects the write.
        for key in tables[name].primary_key:
            assert prepared.filter(prepared[key].isNull()).count() == 0, (name, key)


def test_prepare_rejects_frames_missing_contract_columns(spark) -> None:
    from batch.cassandra_sink import prepare

    frame = spark.createDataFrame([("UST", date(2026, 11, 25))], "curve string, day date")
    with pytest.raises(ValueError, match="lacks contract columns"):
        prepare(frame, "yield_curve")


def test_settings_add_auth_only_when_configured(monkeypatch) -> None:
    pytest.importorskip("pyspark")
    from batch.cassandra_sink import CassandraSettings

    monkeypatch.setenv("CASSANDRA_HOSTS", "market-dc1-service.cassandra.svc")
    monkeypatch.delenv("CASSANDRA_USERNAME", raising=False)
    conf = CassandraSettings.from_env().spark_conf()
    assert conf["spark.cassandra.connection.host"] == "market-dc1-service.cassandra.svc"
    assert "spark.cassandra.auth.username" not in conf

    monkeypatch.setenv("CASSANDRA_USERNAME", "market-superuser")
    monkeypatch.setenv("CASSANDRA_PASSWORD", "secret")
    assert CassandraSettings.from_env().spark_conf()["spark.cassandra.auth.username"] == "market-superuser"
