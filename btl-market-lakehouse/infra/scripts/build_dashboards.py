"""Generate the two Grafana dashboards from code (PDF việc D.5).

- ``market-overview.json``: market dashboard for the jury (Infinity -> market-api).
- ``infra-health.json``: monitoring dashboard (Prometheus).

The JSON files are committed (Grafana provisioning and the K8s sidecar read them as-is);
``infra/tests/test_dashboards.py`` regenerates them and fails on drift, checks that every
Infinity URL is served by market-api and that every ``market_*`` metric is declared in
``contracts/infra.yaml``. Regenerate after editing:

    python -m infra.scripts.build_dashboards
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from common.contracts import INSTRUMENTS

ROOT = Path(__file__).resolve().parents[2]
DASHBOARDS = ROOT / "infra" / "monitoring" / "grafana" / "dashboards"

INFINITY = {"type": "yesoreyeram-infinity-datasource", "uid": "infinity"}
PROMETHEUS = {"type": "prometheus", "uid": "prometheus"}
#: Namespaces of the project; dashboards never chart kube-system noise.
PROJECT_NAMESPACES = "kafka|minio|cassandra|spark|airflow|market|monitoring"
COMPARE_SYMBOLS = "SPY,GLD,TLT,BTCUSDT,GC%3DF,DXY"  # GC=F url-encoded


def _infinity(url: str, columns: list[tuple[str, str, str]] | None = None, ref: str = "A") -> dict:
    """Infinity JSON query against market-api.

    URLs are relative: the datasource base URL (http://market-api:8000 in compose,
    http://market-api.market.svc:8000 in K8s) is prepended by the plugin.
    """
    target: dict[str, Any] = {
        "refId": ref,
        "datasource": INFINITY,
        "type": "json",
        "source": "url",
        "format": "table",
        "parser": "backend",
        "url": url,
        "url_options": {"method": "GET", "data": ""},
        "root_selector": "",
    }
    target["columns"] = [
        {"selector": selector, "text": text, "type": kind}
        for selector, text, kind in (columns or [])
    ]
    return target


def _prom(expr: str, legend: str, ref: str = "A") -> dict:
    return {"refId": ref, "datasource": PROMETHEUS, "expr": expr, "legendFormat": legend}


def _panel(
    panel_id: int,
    title: str,
    kind: str,
    grid: tuple[int, int, int, int],
    targets: list[dict],
    *,
    description: str = "",
    unit: str | None = None,
    options: dict | None = None,
    field_defaults: dict | None = None,
    transformations: list[dict] | None = None,
) -> dict:
    x, y, w, h = grid
    defaults: dict[str, Any] = dict(field_defaults or {})
    if unit:
        defaults["unit"] = unit
    return {
        "id": panel_id,
        "type": kind,
        "title": title,
        "description": description,
        "datasource": targets[0]["datasource"],
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": targets,
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": options or {},
        "transformations": transformations or [],
    }


def _dashboard(uid: str, title: str, panels: list[dict], variables: list[dict], refresh: str) -> dict:
    return {
        "uid": uid,
        "title": title,
        "tags": ["btl-it4931", "market-lakehouse"],
        "timezone": "utc",
        "schemaVersion": 39,
        "version": 1,
        "editable": True,
        "refresh": refresh,
        "time": {"from": "now-6h", "to": "now"},
        "templating": {"list": variables},
        "annotations": {"list": []},
        "panels": panels,
    }


BAR_COLUMNS = [
    ("window_start", "time", "timestamp"),
    ("open", "open", "number"),
    ("high", "high", "number"),
    ("low", "low", "number"),
    ("close", "close", "number"),
    ("volume", "volume", "number"),
    ("vwap", "vwap", "number"),
]
CANDLE_OPTIONS = {
    "mode": "candles+volume",
    "candleStyle": "candles",
    "colorStrategy": "open-close",
    # vwap is not part of OHLCV, so the candlestick panel draws it as an extra line.
    "includeAllFields": True,
    "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
}


def market_dashboard() -> dict:
    symbols = list(INSTRUMENTS)
    variables = [
        {
            "name": "symbol",
            "label": "Mã",
            "type": "custom",
            "query": ",".join(symbols),
            "current": {"text": "BTCUSDT", "value": "BTCUSDT"},
            "options": [
                {"text": symbol, "value": symbol, "selected": symbol == "BTCUSDT"}
                for symbol in symbols
            ],
        },
        {
            # Empty = today (UTC) on Cassandra; pick a past day for replayed sessions.
            "name": "day",
            "label": "Ngày (YYYY-MM-DD, trống = hôm nay)",
            "type": "textbox",
            "query": "",
            "current": {"text": "", "value": ""},
        },
    ]
    panels = [
        _panel(
            1,
            "Nguồn dữ liệu",
            "stat",
            (0, 0, 4, 4),
            [_infinity("/health", [("source", "source", "string"), ("cassandra", "cassandra", "string")])],
            description="cassandra = dữ liệu thật; demo = dữ liệu tổng hợp (chỉ ở laptop); none = Cassandra mất kết nối.",
            options={"reduceOptions": {"calcs": ["lastNotNull"], "fields": "/^source$/", "values": False},
                     "colorMode": "background", "textMode": "value"},
            field_defaults={"mappings": [{"type": "value", "options": {
                "cassandra": {"color": "green", "index": 0},
                "demo": {"color": "orange", "index": 1},
                "none": {"color": "red", "index": 2}}}]},
        ),
        _panel(
            2,
            "Chỉ báo mới nhất (mọi mã)",
            "table",
            (4, 0, 20, 8),
            [_infinity("/api/v1/indicators/latest", [
                ("symbol", "Mã", "string"),
                ("asset_class", "Lớp", "string"),
                ("close", "Giá", "number"),
                ("vwap", "VWAP", "number"),
                ("return_1m", "Lợi suất 1m", "number"),
                ("rsi_14", "RSI 14", "number"),
                ("macd", "MACD", "number"),
                ("zscore_30m", "Z-score 30m", "number"),
                ("volatility_30m", "Biến động 30m", "number"),
                ("window_start", "Nến", "timestamp"),
            ])],
            description="Bảng indicators_latest do speed layer (B) cập nhật mỗi micro-batch.",
            field_defaults={"custom": {"cellOptions": {"type": "auto"}}},
        ),
        _panel(
            3,
            "Nến 1 phút realtime + VWAP: ${symbol}",
            "candlestick",
            (0, 8, 16, 11),
            [_infinity("/api/v1/ohlcv?symbol=${symbol}&interval=1m&day=${day}&limit=390", BAR_COLUMNS)],
            description="ohlcv_1m, phân vùng (symbol, day). Độ trễ mục tiêu dưới 10 giây (PDF việc B).",
            options=CANDLE_OPTIONS,
        ),
        _panel(
            4,
            "Cảnh báo trong ngày",
            "table",
            (16, 8, 8, 11),
            [_infinity("/api/v1/alerts?day=${day}&limit=200", [
                ("alert_time", "Thời điểm", "timestamp"),
                ("symbol", "Mã", "string"),
                ("alert_type", "Loại", "string"),
                ("severity", "Mức", "string"),
                ("price", "Giá", "number"),
                ("zscore", "Z giá", "number"),
                ("volume_zscore", "Z khối lượng", "number"),
            ])],
            description="alerts_by_day: giá hoặc khối lượng lệch > 3 độ lệch chuẩn so với 30 phút trước.",
        ),
        _panel(
            5,
            "Nến 5 phút: ${symbol}",
            "candlestick",
            (0, 19, 12, 9),
            [_infinity("/api/v1/ohlcv?symbol=${symbol}&interval=5m&day=${day}&limit=288", BAR_COLUMNS)],
            options=CANDLE_OPTIONS,
        ),
        _panel(
            6,
            "So sánh tài sản theo ngày (đóng cửa = 100 ở ngày đầu)",
            "timeseries",
            (12, 19, 12, 9),
            [_infinity(f"/api/v1/daily/compare?symbols={COMPARE_SYMBOLS}&limit=120")],
            description="daily_summary của batch layer (C): cổ phiếu, vàng, trái phiếu, crypto, USD trên cùng thang.",
            transformations=[{"id": "convertFieldType", "options": {
                "conversions": [{"targetField": "day", "destinationType": "time"}]}}],
        ),
        _panel(
            7,
            "Ma trận tương quan 90 ngày (lợi suất ngày; US2Y/US10Y/REAL10Y là thay đổi lợi suất)",
            "table",
            (0, 28, 16, 12),
            [_infinity("/api/v1/correlation?window_days=90&format=matrix")],
            description="correlation_matrix, as_of mới nhất. Âm = ngược chiều (ví dụ GLD với REAL10Y, TLT với US10Y).",
            field_defaults={
                "min": -1,
                "max": 1,
                "decimals": 2,
                "color": {"mode": "continuous-RdYlGr"},
                "custom": {"cellOptions": {"type": "color-background", "mode": "gradient"}},
            },
        ),
        _panel(
            8,
            "Đường cong lợi suất mới nhất",
            "barchart",
            (16, 28, 8, 12),
            [_infinity("/api/v1/yield-curve/latest", [
                ("tenor", "Kỳ hạn", "string"),
                ("yield", "Lợi suất %", "number"),
            ])],
            unit="percent",
            options={"xField": "Kỳ hạn", "showValue": "always", "orientation": "vertical"},
        ),
        _panel(
            9,
            "Lợi suất 2Y, 10Y, thực 10Y và spread 10Y-2Y (âm = đảo ngược)",
            "timeseries",
            (0, 40, 16, 9),
            [_infinity("/api/v1/yield-curve?limit=365", [
                ("day", "time", "timestamp"),
                ("y_2y", "2Y", "number"),
                ("y_10y", "10Y", "number"),
                ("real_10y", "Thực 10Y", "number"),
                ("spread_10y_2y", "Spread 10Y-2Y", "number"),
            ])],
            unit="percent",
        ),
        _panel(
            10,
            "Phân cụm tài sản (MLlib KMeans)",
            "table",
            (16, 40, 8, 9),
            [_infinity("/api/v1/clusters", [
                ("symbol", "Mã", "string"),
                ("asset_class", "Lớp", "string"),
                ("cluster", "Cụm", "number"),
                ("volatility", "Biến động", "number"),
                ("corr_us10y", "Tương quan US10Y", "number"),
                ("model", "Mô hình", "string"),
            ])],
        ),
    ]
    return _dashboard("market-overview", "Market Lakehouse: thị trường", panels, variables, "10s")


def infra_dashboard() -> dict:
    ns = f'namespace=~"{PROJECT_NAMESPACES}"'
    panels = [
        _panel(1, "Target Prometheus đang lên", "stat", (0, 0, 24, 4),
               [_prom("sum by (job) (up)", "{{job}}")],
               options={"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background"},
               field_defaults={"thresholds": {"mode": "absolute", "steps": [
                   {"color": "red", "value": None}, {"color": "green", "value": 1}]}}),
        _panel(2, "Kafka consumer lag (Kafka Connect → MinIO bronze)", "timeseries", (0, 4, 12, 8),
               [_prom('sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup=~"connect-s3-sink-bronze.*"})',
                      "{{consumergroup}} {{topic}}")],
               description="Hai S3 sink: connect-s3-sink-bronze và connect-s3-sink-bronze-alerts. Lag tăng liên tục = "
                           "Connect/MinIO không theo kịp (PDF D.5: Kafka có bị dồn tin không)."),
        _panel(3, "Tin nhắn vào Kafka / giây theo topic", "timeseries", (12, 4, 12, 8),
               [_prom('sum by (topic) (rate(kafka_topic_partition_current_offset{topic!~"__.*|connect-.*"}[1m]))',
                      "{{topic}}")], unit="ops"),
        _panel(4, "Producer: tin nhắn / giây (A)", "timeseries", (0, 12, 12, 8),
               [_prom("sum by (source, topic) (rate(market_producer_messages_total[1m]))", "{{source}} → {{topic}}")],
               unit="ops"),
        _panel(5, "Producer: lỗi, kết nối lại, dead-letter, bản trùng bị bỏ (15 phút)", "timeseries",
               (12, 12, 12, 8),
               [_prom("sum by (source, kind) (increase(market_producer_errors_total[15m]))", "lỗi {{source}} {{kind}}"),
                _prom("sum by (source) (increase(market_producer_reconnects_total[15m]))", "reconnect {{source}}", "B"),
                _prom("sum by (source) (increase(market_producer_dead_letter_total[15m]))", "dead-letter {{source}}", "C"),
                _prom("sum by (source) (increase(market_producer_duplicates_total[15m]))", "trùng {{source}}", "D")],
               description="Bản trùng = tin gửi lại khi kết nối lại, producer đã bỏ theo trade_id "
                           "(slide Lessons learned 1/3: % trùng)."),
        _panel(6, "Spark streaming: dòng vào / xử lý mỗi giây (B)", "timeseries", (0, 20, 12, 8),
               [_prom("sum by (query) (market_speed_input_rows_per_second)", "vào {{query}}"),
                _prom("sum by (query) (market_speed_processed_rows_per_second)", "xử lý {{query}}", "B")],
               unit="rowsps",
               description="Xử lý < vào kéo dài = job không theo kịp, xem offsets behind latest."),
        _panel(7, "Spark streaming: offsets chưa đọc (lag của speed layer)", "timeseries", (12, 20, 12, 8),
               [_prom("max by (query) (market_speed_offsets_behind_latest)", "{{query}}")],
               description="Spark lưu offset trong checkpoint MinIO, không commit vào Kafka; đây là lag thật của luồng nhanh."),
        _panel(8, "Spark streaming: thời gian micro-batch", "timeseries", (0, 28, 12, 8),
               [_prom("max by (query) (market_speed_batch_duration_ms)", "{{query}}")], unit="ms"),
        _panel(9, "Độ trễ giao dịch → Cassandra (mục tiêu < 10 s)", "timeseries", (12, 28, 12, 8),
               [_prom("max by (query) (market_speed_event_to_sink_latency_seconds)", "{{query}}"),
                # B may export a histogram instead of a gauge; whichever series exists is drawn.
                _prom("histogram_quantile(0.95, sum by (le, query) "
                      "(rate(market_speed_event_to_sink_latency_seconds_bucket[5m])))", "p95 {{query}}", "B")],
               unit="s",
               field_defaults={"thresholds": {"mode": "absolute", "steps": [
                   {"color": "green", "value": None}, {"color": "red", "value": 10}]},
                   "custom": {"thresholdsStyle": {"mode": "line"}}}),
        _panel(10, "CPU theo pod", "timeseries", (0, 36, 12, 8),
               [_prom(f'sum by (namespace, pod) (rate(container_cpu_usage_seconds_total{{{ns},container!=""}}[5m]))',
                      "{{namespace}}/{{pod}}")], unit="cores"),
        _panel(11, "RAM theo pod", "timeseries", (12, 36, 12, 8),
               [_prom(f'sum by (namespace, pod) (container_memory_working_set_bytes{{{ns},container!=""}})',
                      "{{namespace}}/{{pod}}")], unit="bytes"),
        _panel(12, "RAM còn trống của máy (minikube node)", "stat", (0, 44, 6, 6),
               [_prom("sum(node_memory_MemAvailable_bytes)", "available")], unit="bytes"),
        _panel(13, "Pod khởi động lại (1 giờ)", "table", (6, 44, 9, 6),
               [_prom(f"sum by (namespace, pod) (increase(kube_pod_container_status_restarts_total{{{ns}}}[1h])) > 0",
                      "{{namespace}}/{{pod}}")],
               description="Dùng khi thử lỗi (docs/lessons/D-fault-injection.md)."),
        _panel(14, "market-api: p95 độ trễ và nguồn dữ liệu", "timeseries", (15, 44, 9, 6),
               [_prom("histogram_quantile(0.95, sum by (le, path) "
                      "(rate(market_api_request_duration_seconds_bucket[5m])))", "p95 {{path}}"),
                _prom("sum by (source) (rate(market_api_requests_total[5m]))", "req/s {{source}}", "B")]),
    ]
    return _dashboard("infra-health", "Market Lakehouse: giám sát hệ thống", panels, [], "30s")


def render() -> dict[str, str]:
    return {
        "market-overview.json": json.dumps(market_dashboard(), ensure_ascii=False, indent=2) + "\n",
        "infra-health.json": json.dumps(infra_dashboard(), ensure_ascii=False, indent=2) + "\n",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Grafana dashboards")
    parser.add_argument("--check", action="store_true", help="exit 1 if a file is stale")
    args = parser.parse_args()
    for name, text in render().items():
        path = DASHBOARDS / name
        if args.check:
            if path.read_text(encoding="utf-8") != text:
                sys.exit(f"{path} is stale; run python -m infra.scripts.build_dashboards")
            continue
        path.write_text(text, encoding="utf-8")
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
