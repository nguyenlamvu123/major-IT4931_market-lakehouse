# Hợp đồng dữ liệu v2 (`contracts/`)

Theo PDF *BTL-IT4931 Kiến trúc – Phân công* (09/11/2026), mục "Hợp đồng giữa các vai": tên
topic, định dạng tin nhắn, đường dẫn MinIO, bảng Cassandra và hạ tầng được chốt ở đây
từ tuần 1. Ai đổi phải mở Pull Request và được vai nhận dữ liệu đồng ý.

Thư mục này nằm trên `main` để cả 4 nhánh vai cùng đọc một bản. Bản thực thi bằng Python
nằm ở `common/contracts.py`, và `tests/test_contracts.py` kiểm tra hai bên luôn khớp nhau.

| File | Nội dung | Vai sở hữu | Vai dùng |
|---|---|---|---|
| `topics.yaml` | 7 topic theo PDF và `dead-letter` | A | B, C, D |
| `schemas/*.schema.json` | JSON Schema: trade, bar, rate_yield, alert | A (alert: B) | B, C |
| `samples/<topic>.jsonl` | 20 dòng mẫu mỗi topic | A | B, C |
| `paths.yaml` | Đường dẫn bronze/silver/gold/checkpoint trên MinIO | A, C | B, C |
| `cassandra.cql` | Keyspace `market`, bảng realtime và batch | B, C | D |
| `infra.yaml` | Namespace, service, secret, image, cổng metrics | D | A, B, C |

## Quy ước chung

- `event_time`: epoch **mili giây**, UTC, thời điểm sự kiện xảy ra trên thị trường.
- `ingest_time`: epoch mili giây lúc producer nhận được tin. Producer luôn gắn trường này
  để đo độ trễ.
- Khóa Kafka: `symbol`; riêng `rates.yields` dùng `tenor`. Mọi giao dịch của một mã vào
  cùng một partition nên giữ đúng thứ tự.
- Topic chọn theo **nguồn feed**, còn `asset_class` mô tả **tài sản gốc**. Ví dụ GLD đi
  qua `stocks.trades` (feed Alpaca) nhưng có `asset_class = "gold"`, để batch so sánh
  được vàng với lợi suất.
- `trade_id`, `bar_id`, `alert_id` là khóa tất định. Gửi lại cùng một sự kiện luôn ra
  cùng một id, nhờ vậy bỏ trùng được ở silver và upsert được ở Cassandra.
- Bản ghi sai hợp đồng được đẩy sang `dead-letter` dưới dạng
  `{"source", "topic", "error", "payload", "ingest_time"}`, không bị âm thầm bỏ đi.

## Sinh dữ liệu mẫu

```bash
python scripts/generate_samples.py      # ghi contracts/samples/*.jsonl + manifest.json
python -m pytest tests/test_contracts.py
```

## Quy tắc thay đổi

1. Trong cùng một version chỉ được **thêm** trường tuỳ chọn; không đổi tên, không đổi kiểu
   trường đã có.
2. Thêm trường bắt buộc hoặc đổi tên topic thì phải tăng `CONTRACT_VERSION` và ghi chú
   cách chuyển đổi trong `docs/thay-doi-theo-pdf.md`.
3. Sửa `cassandra.cql` thì đồng thời sửa sink của B/C và repository của `market-api` (D).
