"""Every CQL statement of market-api must be valid against contracts/cassandra.cql."""

from __future__ import annotations

import pytest

from common.cql import load_tables
from serving.queries import QUERIES

TABLES = load_tables()


@pytest.mark.parametrize("name", sorted(QUERIES))
def test_selected_columns_exist_in_contract(name: str) -> None:
    query = QUERIES[name]
    table = TABLES[query.table]
    assert set(query.columns) <= set(table.columns), name


@pytest.mark.parametrize("name", sorted(QUERIES))
def test_where_clause_is_partition_key_plus_clustering_prefix(name: str) -> None:
    # Cassandra only serves equality on the full partition key followed by a clustering
    # prefix without ALLOW FILTERING; a full-table read is allowed only when deliberate.
    query = QUERIES[name]
    table = TABLES[query.table]
    if not query.where:
        assert name == "indicators_all"
        return
    partition = table.partition_key
    assert query.where[: len(partition)] == partition, name
    rest = query.where[len(partition) :]
    assert rest == table.clustering_key[: len(rest)], name
    assert "ALLOW FILTERING" not in query.cql


def test_realtime_query_matches_pdf_example() -> None:
    # PDF: "Bảng được thiết kế theo câu truy vấn, ví dụ giá BTC hôm nay theo từng phút".
    assert QUERIES["ohlcv_1m"].cql == (
        "SELECT symbol, day, window_start, window_end, asset_class, open, high, low, close, "
        "volume, vwap, trade_count, last_event_time, updated_at FROM market.ohlcv_1m "
        "WHERE symbol = %s AND day = %s LIMIT %s"
    )
