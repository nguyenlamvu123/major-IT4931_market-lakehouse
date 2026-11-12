# Nhật ký sự cố và bài học (`docs/lessons/`)

PDF ý tưởng: "Ghi nhật ký sự cố vào docs/lessons/ ngay khi gặp, không để đến cuối kỳ."
Mỗi sự cố hoặc thí nghiệm là một file `<vai>-<chủ-đề>.md`. Ví dụ:
`A-binance-reconnect.md`, `B-watermark.md`, `C-skew-aqe.md`,
`D-fault-injection.md` (đã có sẵn).

## Mẫu (4 phần, giống chương báo cáo)

```markdown
# <Vai>: <tiêu đề ngắn>

- Ngày (UTC):
- Mảng (1–11, bảng dưới):
- Cấu hình: máy, số broker/executor, tốc độ replay…

## Vấn đề
Hiện tượng, cách phát hiện (panel Grafana, log, test), ảnh hưởng.

## Các cách đã thử
1. … → kết quả, vì sao bỏ.

## Giải pháp kèm số liệu
Thay đổi (commit/PR) và số đo trước/sau. Chép số sang docs/metrics.md.

## Bài học
1–2 câu dùng lại được cho slide.
```

## 11 mảng (slide Lessons learned trong PDF ý tưởng)

| # | Mảng | Tình huống trong đề tài | Vai |
|---|---|---|---|
| 1 | Thu thập dữ liệu | Nguồn lệch giá; tick đến muộn; trùng khi reconnect; giá đổi sau split | A |
| 2 | Xử lý Spark | Skew ở AAPL, NVDA, BTC; chọn số partition; OOM khi pivot | B, C |
| 3 | Xử lý luồng | Chọn watermark; state phình to; phục hồi khi pod bị kill | B |
| 4 | Lưu trữ | JSON hay Parquet; nhiều file nhỏ; dữ liệu nóng/lạnh (TTL 7 ngày) | C |
| 5 | Tích hợp hệ thống | API giới hạn tần suất; dịch vụ phụ thuộc chết; DNS trong K8s | D |
| 6 | Tối ưu hiệu năng | Join chậm; đọc thừa file; executor thiếu/dư | C |
| 7 | Giám sát, debug | Consumer lag tăng; producer im lặng; operator mất lease | D |
| 8 | Mở rộng | Replay 100x vượt sức xử lý; laptop thiếu RAM | D, A |
| 9 | Chất lượng, kiểm thử | Giá âm, high < low; UDF sai; luồng end-to-end gãy | C, D |
| 10 | Bảo mật, quản trị | Lộ API key; topic ai cũng đọc được; dữ liệu không mã hoá | D |
| 11 | Chịu lỗi | Broker/driver/node chết; mất dữ liệu trên MinIO | D, B |

## Đã ghi

| File | Nội dung |
|---|---|
| `D-fault-injection.md` | 7 kịch bản thử lỗi, bảng kết quả, mẫu bài học |
