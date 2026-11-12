# market-api (vai D)

API chỉ đọc. Nó chuyển các bảng Cassandra của hợp đồng v2 (keyspace `market`) thành
JSON để Grafana đọc qua plugin Infinity, vì Grafana không có datasource Cassandra chính
thức.

- Cassandra: `market-dc1-service.cassandra.svc:9042`
- Địa chỉ trong K8s: `market-api.market.svc:8000`

## Nên dùng / không nên dùng

- **Nên dùng** làm nguồn dữ liệu cho dashboard thị trường (giá realtime, nến, cảnh báo,
  tương quan, lợi suất), hoặc khi cần xem nhanh dữ liệu trong Cassandra bằng `curl`.
- **Không dùng** để ghi dữ liệu (B và C ghi thẳng vào Cassandra), và không dùng làm
  API công khai: API không có xác thực, chỉ phục vụ trong cụm.

## Endpoint

| Endpoint | Bảng Cassandra | Ghi chú |
|---|---|---|
| `GET /api/v1/ohlcv?symbol=BTCUSDT&day=YYYY-MM-DD&interval=1m\|5m&limit=390` | `ohlcv_1m` / `ohlcv_5m`, phân vùng `(symbol, day)` | Trả theo thời gian tăng dần; bỏ `day` thì lấy hôm nay (UTC) |
| `GET /api/v1/indicators/latest[?symbol=]` | `indicators_latest` | 1 dòng/mã |
| `GET /api/v1/alerts?day=&limit=200` | `alerts_by_day`, phân vùng `day` | Mới nhất trước |
| `GET /api/v1/daily?symbol=SPY&limit=90` | `daily_summary` | Tăng dần theo ngày |
| `GET /api/v1/daily/compare?symbols=SPY,GLD,TLT,BTCUSDT&limit=90` | `daily_summary` | Giá đóng cửa quy về 100 ở ngày đầu, mỗi mã một cột |
| `GET /api/v1/correlation?window_days=90&format=pairs\|matrix` | `correlation_matrix`, lấy `as_of` mới nhất | `matrix` = bảng đối xứng, đường chéo bằng 1 |
| `GET /api/v1/yield-curve?limit=90` | `yield_curve` (`curve='UST'`) | Lịch sử, kèm spread 10Y-2Y |
| `GET /api/v1/yield-curve/latest` | `yield_curve` | Các điểm (tenor, years, yield) để vẽ hình dạng đường cong |
| `GET /api/v1/clusters` | `asset_clusters` (`model='kmeans'`, `as_of` mới nhất) | Kết quả MLlib của C |
| `GET /api/v1/symbols` | — | Danh sách mã từ `common.contracts.INSTRUMENTS` |
| `GET /health` | — | Luôn trả 200, kèm trạng thái Cassandra và nguồn dữ liệu |
| `GET /metrics` | — | `market_api_requests_total`, `market_api_request_duration_seconds`, `process_*` |

Mọi câu CQL được khai báo dưới dạng dữ liệu trong `serving/queries.py`.
`serving/tests/test_queries.py` kiểm tra từng cột có trong `contracts/cassandra.cql`, và
mệnh đề WHERE luôn gồm đủ partition key cộng một prefix của clustering key (không cần
`ALLOW FILTERING`).

## Nguồn dữ liệu: thật hay demo

| Tình huống | `DEMO_FALLBACK=true` (mặc định ở laptop) | `DEMO_FALLBACK=false` (manifest K8s) |
|---|---|---|
| Cassandra có dữ liệu | dữ liệu thật | dữ liệu thật |
| Cassandra đang chạy nhưng bảng rỗng | `[]` (không thay bằng demo) | `[]` |
| Cassandra không kết nối được | dữ liệu tổng hợp từ `common` | HTTP 503 |

Mọi response đều có header `X-Data-Source: cassandra|demo|none`, nên khi chấm điểm không
thể nhầm số liệu giả với số liệu thật. Khi Cassandra mất kết nối, API thử kết nối lại
tối đa 5 giây một lần. Một query lỗi sẽ báo lỗi chứ không trả danh sách rỗng.

## Chạy

```bash
pip install -r serving/requirements.txt
uvicorn serving.market_api:app --reload --port 8000        # hoặc: make api
curl -i 'localhost:8000/api/v1/ohlcv?symbol=BTCUSDT&limit=5'
```

Docker (chạy từ thư mục `btl-market-lakehouse/`):

```bash
docker build -f serving/Dockerfile -t btl/market-api:dev .
docker run --rm -p 8000:8000 -e CASSANDRA_HOSTS=host.docker.internal btl/market-api:dev
```

## Test

```bash
python -m pytest serving/tests
```

Test dùng repository giả để chạy không cần mạng: Cassandra mất kết nối, Cassandra rỗng,
session lỗi, cùng với `DemoRepository` thật.
