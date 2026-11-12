# Ingest (vai A): thu thập dữ liệu và Kafka

Việc của A, gói trong một câu theo PDF: giá từ 4 nguồn chảy vào Kafka liên tục, đúng định
dạng, kể cả khi sàn Mỹ đóng cửa. Mọi producer chuẩn hoá dữ liệu về hợp đồng v2
(`contracts/`, `common/contracts.py`). Producer không tự ghi tên topic trong code: topic và
key đều lấy từ hợp đồng.

## Ma trận nguồn → topic

| `--source` | Nguồn | Symbol (`asset_class`) | Topic | Khóa API | K8s mặc định |
|---|---|---|---|---|---|
| `binance` | Binance WebSocket (combined stream) | BTCUSDT, ETHUSDT (`crypto`) | `crypto.trades` | không | bật |
| `alpaca` | Alpaca IEX WebSocket (REST dự phòng: `--alpaca-mode rest`) | AAPL, MSFT, NVDA, SPY (`equity`), GLD (`gold`), TLT, IEF, SHY (`bond`) | `stocks.trades` (trade) và `stocks.bars` (nến 1Min) | `ALPACA_API_KEY`, `ALPACA_SECRET_KEY` | bật |
| `finnhub` | Finnhub trade WebSocket | như Alpaca | `stocks.trades` | `FINNHUB_API_KEY` | tắt (dự phòng) |
| `yahoo` | yfinance, nến 1 phút mới nhất | GC=F (`gold`) | `gold.quotes` | không | bật |
| `yahoo` | như trên, ticker DX-Y.NYB | DXY (`fx`, volume = 0) | `fx.dxy` | không | bật |
| `treasury` | CSV home.treasury.gov (đường cong danh nghĩa + lợi suất thực) | 1M…30Y, REAL10Y (`rates`) | `rates.yields` (key = tenor) | không | bật |
| `fred` | FRED API (cùng `series_id`) | 1M…30Y, REAL10Y | `rates.yields` | `FRED_API_KEY` | tắt |
| `backfill` | yfinance nến ngày, nhiều năm | mọi mã trong registry | `stocks.bars` (`1Day`) | không | Job chạy 1 lần |
| `replay` | JSONL local hoặc bronze trên MinIO, 1x–100x | theo dữ liệu | theo `topic_for_record` | không | tắt (bật khi demo ban ngày) |
| `mock` | Thị trường giả lập tất định | mọi mã | 6 topic đầu vào | không | – |

Topic chọn theo **feed**, `asset_class` chọn theo **tài sản gốc**. Ví dụ GLD đi vào
`stocks.trades` nhưng mang `asset_class = gold`, để batch so được vàng với lợi suất thực.

## Chạy

```bash
pip install -r requirements.txt            # hoặc chỉ ingest/requirements.txt

# Không cần Kafka: in ra màn hình
python -m ingest.run_producer --source mock --rows 50 --dry-run --metrics-port 0
python -m ingest.run_producer --source replay --file contracts/samples --speed 100 --dry-run

# Với Kafka (Docker Compose dev: localhost:9092)
export KAFKA_BOOTSTRAP=localhost:9092
python -m ingest.run_producer --source binance
python -m ingest.run_producer --source alpaca            # cần key trong .env
python -m ingest.run_producer --source yahoo --interval 60
python -m ingest.run_producer --source treasury --interval 3600
python -m ingest.backfill_yfinance --years 3             # = --source backfill

# Demo ban ngày giờ Việt Nam: phát lại phiên thật từ bronze, tốc độ 10x, thời gian "hiện tại"
MINIO_ENDPOINT=http://localhost:9000 python -m ingest.run_producer --source replay \
  --s3-uri s3://lake/bronze/stocks.trades/dt=2026-11-10/ --speed 10 --rebase-time --loop
```

Replay giãn cách theo chênh lệch `event_time` chia cho `--speed` (giới hạn 1–100x). Khoảng
nghỉ dài như qua đêm bị chặn bởi `--max-gap` (mặc định 5 giây). `--rebase-time` gắn
`event_time` = thời điểm gửi, `source=replay` và id mới, còn giá trị gốc được giữ trong
`metadata`. Nhờ vậy speed layer coi đây là dữ liệu đang diễn ra và các vòng `--loop` không
bị lọc trùng.

## Đảm bảo giao nhận

| Rủi ro | Cách xử lý | Ở đâu |
|---|---|---|
| Mất tin khi broker chậm | `acks=all`, `enable.idempotence=true`, retry | `ingest/config.py` |
| Trùng tin khi kết nối lại | `trade_id` tất định (ưu tiên id của sàn) + bộ nhớ LRU 50.000 id trong tiến trình; silver bỏ trùng lần nữa | `common/contracts.py`, `ingest/dedupe.py` |
| Mất kết nối WebSocket | Kết nối lại với exponential backoff + jitter (1 s → tối đa 60 s), đặt lại sau một phiên khoẻ ≥ 60 s | `BaseProducer.run_with_reconnect` |
| API REST lỗi hoặc bị giới hạn tần suất | Circuit breaker closed/open/half-open; khi mạch mở thì không gọi API | `BaseProducer.poll_forever`, `ingest/resilience.py` |
| Bản ghi sai hợp đồng | Validate trước khi gửi, bản sai đi sang `dead-letter` dạng `{source, topic, error, payload, ingest_time}` | `BaseProducer.publish` |
| Thứ tự theo mã | Key = `symbol` (`rates.yields`: `tenor`), nên cùng mã luôn vào cùng partition | hợp đồng |
| Pod bị dừng | SIGTERM → `stop_event` → `close()` flush tin đang gửi | `run_producer.py` |

Alpaca REST trước đây chỉ gửi `limit=100`, nên lần poll nào cũng nhận lại 100 trade đầu
ngày. Nay mỗi mã giữ một con trỏ `start` = trade mới nhất đã gửi, thêm `feed=iex` và đi
theo `next_page_token`. Có test tái hiện lỗi này trong `ingest/tests/test_ingest_producers.py`.

## Metrics (Prometheus, cổng 8001)

| Series | Ý nghĩa |
|---|---|
| `market_producer_messages_total{topic,source}` | Số bản ghi đã giao cho Kafka client |
| `market_producer_errors_total{source,kind}` | `connection`, `poll`, `kafka_send` |
| `market_producer_reconnects_total{source}` | Số lần kết nối lại WebSocket |
| `market_producer_dead_letter_total{source}` | Số bản ghi bị chuyển sang dead-letter |
| `market_producer_duplicates_total{source}` | Số bản gửi lại bị bỏ trong tiến trình (thêm ngoài danh sách của hợp đồng) |

Mỗi 30 giây, producer ghi log một dòng `throughput crypto.trades=…/s`.

## Đo cho báo cáo

```bash
python -m ingest.tools.topic_rates --bootstrap localhost:9092 --interval 60 --samples 60   # tin/giây mỗi topic (broker)
python -m ingest.tools.feed_quality dump.jsonl                                              # % trùng, % sai thứ tự, % đến muộn, độ trễ thu thập
python -m ingest.tools.bench_publish --rows 100000                                          # trần của đường publish (không có Kafka)
```

Kết quả ghi vào `docs/metrics/A-ingest.md`, còn chương báo cáo nằm ở `docs/report/A-ingest.md`.

## Kafka Connect → bronze

Xem `ingest/kafka_connect/README.md`. Có 2 connector: `s3-sink-bronze` (6 topic đầu vào,
phân vùng theo `event_time`) và `s3-sink-bronze-alerts`. Đầu ra:
`s3a://lake/bronze/<topic>/dt=YYYY-MM-DD/`.

## Docker và Kubernetes

```bash
docker buildx build --platform linux/arm64 -f ingest/Dockerfile -t btl/ingest:dev .
minikube image load btl/ingest:dev
kubectl apply -k ingest/k8s            # producers (ns market) + Kafka Connect (ns kafka)
kubectl -n market get deploy,job -l app.kubernetes.io/component=producer
```

- Mặc định bật binance, alpaca, yahoo và treasury. Tắt sẵn finnhub, fred, replay (`replicas: 0`).
- Khi demo ban ngày: `kubectl -n market scale deploy/producer-replay --replicas=1`.
- Lợi suất chạy dạng Deployment poll mỗi giờ thay vì CronJob. Lý do: trạng thái "ngày đã
  gửi", circuit breaker và `/metrics` cần sống giữa các lần poll để Prometheus phát hiện
  được khi feed bị ngừng.
- Toàn bộ manifest qua `kubeconform -strict`.

## Test

```bash
python -m pytest ingest/tests -q
```

Test không gọi mạng. Payload của từng API là mẫu cố định, Kafka được thay bằng sink giả.

## Giới hạn đã biết

- Feed IEX miễn phí chỉ phản ánh một phần khối lượng toàn thị trường (đã nêu là ngoài phạm
  vi trong PDF ý tưởng).
- Finnhub không có id giao dịch: hai trade giống hệt nhau trong cùng một mili giây bị gộp
  làm một.
- Chưa chạy trên cụm thật. Các số liệu "4 nguồn chạy 1 giờ liền" và "tin/giây mỗi topic"
  phải đo trên minikube bằng các công cụ ở trên.
