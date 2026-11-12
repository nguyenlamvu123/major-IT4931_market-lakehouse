"""Read the Cassandra contract (``contracts/cassandra.cql``) as Python data.

Writers (B streaming sink, C batch sink) and the reader (D market-api) use this in tests
to prove that the columns they write or select exist in the contract. That way a renamed
column fails in CI instead of failing at runtime on the cluster.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

CQL_PATH = Path(__file__).resolve().parents[1] / "contracts" / "cassandra.cql"

_TABLE_RE = re.compile(
    r"CREATE TABLE IF NOT EXISTS\s+(?:(\w+)\.)?(\w+)\s*\((.*?)\)\s*(?:WITH|;)",
    re.IGNORECASE | re.DOTALL,
)
_PK_RE = re.compile(r"PRIMARY KEY\s*\((.*)\)\s*$", re.IGNORECASE | re.DOTALL)


@dataclass(frozen=True)
class CqlTable:
    keyspace: str
    name: str
    #: Column name -> CQL type, in declaration order.
    columns: dict[str, str]
    partition_key: tuple[str, ...]
    clustering_key: tuple[str, ...] = field(default_factory=tuple)

    @property
    def primary_key(self) -> tuple[str, ...]:
        return self.partition_key + self.clustering_key


def _strip_comments(text: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in text.splitlines())


def _parse_primary_key(spec: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """``((a, b), c, d)`` -> ((a, b), (c, d)); ``a`` -> ((a,), ())."""
    spec = spec.strip()
    if spec.startswith("("):
        depth = 0
        for index, char in enumerate(spec):
            depth += {"(": 1, ")": -1}.get(char, 0)
            if depth == 0:
                partition = spec[1:index]
                rest = spec[index + 1 :]
                break
        else:  # pragma: no cover - malformed CQL
            raise ValueError(f"unbalanced PRIMARY KEY: {spec}")
    else:
        partition, _, rest = spec.partition(",")
        rest = "," + rest if rest else ""
    parts = tuple(item.strip() for item in partition.split(",") if item.strip())
    clustering = tuple(item.strip() for item in rest.split(",") if item.strip())
    return parts, clustering


def load_tables(path: str | Path = CQL_PATH) -> dict[str, CqlTable]:
    """Parse every ``CREATE TABLE IF NOT EXISTS`` statement of the contract file."""
    text = _strip_comments(Path(path).read_text(encoding="utf-8"))
    default_keyspace = "market"
    tables: dict[str, CqlTable] = {}
    for match in _TABLE_RE.finditer(text):
        keyspace = match.group(1) or default_keyspace
        name = match.group(2)
        body = match.group(3)
        columns: dict[str, str] = {}
        partition: tuple[str, ...] = ()
        clustering: tuple[str, ...] = ()
        # Split on commas that are not inside the PRIMARY KEY parentheses.
        depth = 0
        current = ""
        items: list[str] = []
        for char in body:
            depth += {"(": 1, ")": -1}.get(char, 0)
            if char == "," and depth == 0:
                items.append(current)
                current = ""
            else:
                current += char
        items.append(current)
        for item in (piece.strip() for piece in items):
            if not item:
                continue
            pk = _PK_RE.match(item)
            if pk:
                partition, clustering = _parse_primary_key(pk.group(1))
                continue
            words = item.split()
            column, cql_type = words[0], words[1]
            columns[column] = cql_type
            if len(words) > 2 and " ".join(words[2:]).upper() == "PRIMARY KEY":
                partition = (column,)
        tables[name] = CqlTable(keyspace, name, columns, partition, clustering)
    return tables
