## PR này làm gì

<!-- 1-3 câu: vấn đề, cách giải quyết. Ví dụ: "streaming: thêm watermark 30s cho nến 1 phút". -->

Closes #

## Cách chạy thử

<!-- Lệnh đã chạy và kết quả, ví dụ: -->

```bash
cd btl-market-lakehouse
python -m pytest
```

## Checklist (PDF mục 8: "Duyệt PR của người khác thế nào")

- [ ] Mô tả rõ PR làm gì và cách chạy thử
- [ ] Có đụng `contracts/` hoặc `common/`? Nếu có: đã tag cả 4 vai, cập nhật `docs/thay-doi-theo-pdf.md`, vai nhận dữ liệu đã đồng ý
- [ ] Không có API key, mật khẩu, file `.env` trong diff
- [ ] Không commit `data/`, checkpoint, file dữ liệu lớn
- [ ] CI xanh (ruff + pytest)
- [ ] Số liệu đo được (nếu có) đã ghi vào `docs/metrics.md`
