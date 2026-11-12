# Dàn ý báo cáo

PDF mục 5: "mỗi người viết chương báo cáo của mình theo mẫu 4 phần (vấn đề, giải pháp,
số liệu, bài học), trình bày slide phần mình". D ghép báo cáo và slide tổng (việc D.7).

## Chương 1. Đặt vấn đề và phạm vi (D)

- Giá cổ phiếu, vàng, crypto, trái phiếu thay đổi từng giây. Cần hai thứ cùng lúc: cảnh
  báo trong vài giây và phân tích lịch sử nhiều năm.
- Phạm vi gồm 4 lớp tài sản cộng chỉ số USD:
  - cổ phiếu Mỹ (kèm ETF GLD, TLT/IEF/SHY);
  - vàng (GC=F);
  - crypto;
  - lợi suất trái phiếu Mỹ;
  - DXY.
- Ngoài phạm vi: không đặt lệnh thật, không tư vấn đầu tư; chỉ dùng nguồn miễn phí (feed
  IEX chỉ có một phần khối lượng).

## Chương 2. Kiến trúc Lambda (D)

- Sơ đồ và bảng thành phần: `docs/architecture.md`.
- Hợp đồng giữa các vai trong `contracts/`: topic, schema, đường dẫn MinIO, bảng Cassandra,
  hạ tầng.
- Giải thích lựa chọn: MinIO thay HDFS, Cassandra cho tầng phục vụ, operator trên Kubernetes.

## Chương 3–6. Mỗi vai một chương, cùng mẫu 4 phần

| Phần | Nội dung | Ví dụ ở vai D |
|---|---|---|
| 1. Vấn đề | Tình huống cụ thể gặp trong đề tài | Một máy 16 GB phải chạy Kafka, Cassandra, Spark, Airflow, Prometheus; CI không chạy; dashboard có thể hiện số giả |
| 2. Giải pháp và các cách đã thử | Cách làm, phương án đã bỏ và lý do | Operator thay manifest tự viết; `make up` chạy theo thứ tự; `DEMO_FALLBACK=false` + header `X-Data-Source` |
| 3. Số liệu | Bảng hoặc biểu đồ từ `docs/metrics.md` | Thời gian `make up`, thời gian phục hồi của 6 kịch bản lỗi, RAM toàn cụm |
| 4. Bài học | Điều rút ra, kèm số liệu | "Validate manifest trong CI (kustomize + kubeconform) bắt lỗi trước khi lên cụm" |

| Chương | Vai | Bằng chứng trong repo |
|---|---|---|
| 3 | A: Thu thập & Kafka | `ingest/`, `contracts/samples`, KafkaConnect S3 sink, số liệu msg/s mỗi topic |
| 4 | B: Speed layer | `streaming/`, watermark, nến 1m/5m, exactly-once, độ trễ < 10 s |
| 5 | C: Batch layer | `batch/`, bronze/silver/gold, tương quan với lợi suất, đường cong lợi suất, MLlib, Airflow, bảng tối ưu trước/sau |
| 6 | D: Hạ tầng & Dashboard | `infra/`, `serving/`, `.github/`, dashboard thị trường và giám sát, thử lỗi |

## Chương 7. Kết quả phân tích (C chủ trì, cả nhóm)

Trả lời các câu hỏi trên slide "So sánh với trái phiếu chính phủ Mỹ":

- Cổ phiếu và trái phiếu (SPY với TLT) đi cùng hay ngược chiều?
- Đường cong lợi suất đảo ngược (10Y−2Y < 0) báo hiệu gì?
- Vàng (GLD, GC=F) có ngược chiều lợi suất thực (REAL10Y) không?
- Vào ngày thị trường có cảnh báo, tiền chạy về đâu?

Số liệu lấy từ dashboard thị trường: ma trận tương quan và đường cong lợi suất.

## Chương 8. Lessons learned (cả nhóm)

Theo 11 mảng của PDF ý tưởng (slide Lessons learned 1/3–3/3). Mỗi mảng viết theo mẫu:
vấn đề → các cách đã thử → giải pháp kèm số liệu → bài học. Nhật ký thô nằm trong
`docs/lessons/`.

| Mảng | Vai chính |
|---|---|
| 1. Thu thập dữ liệu | A |
| 2. Xử lý Spark | B, C |
| 3. Xử lý luồng | B |
| 4. Lưu trữ | C |
| 5. Tích hợp hệ thống | D |
| 6. Tối ưu hiệu năng | C |
| 7. Giám sát, debug | D |
| 8. Mở rộng | D, A |
| 9. Chất lượng, kiểm thử | C, D |
| 10. Bảo mật, quản trị | D |
| 11. Chịu lỗi | D, B |

## Chương 9. Demo và kết luận (D)

- Kịch bản demo: `docs/demo_script.md`, kèm video dự phòng (việc D.7).
- Giới hạn: cấu hình dev chỉ có 1 broker và 1 node Cassandra; nguồn dữ liệu miễn phí.

## Phụ lục

- Runbook: `docs/runbook.md`
- Quy trình GitHub: `docs/github-workflow.md`
- Bản đồ nhánh: `docs/branching.md`
