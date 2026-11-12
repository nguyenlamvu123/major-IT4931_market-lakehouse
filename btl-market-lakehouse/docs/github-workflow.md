# Quy trình làm việc trên GitHub

Theo PDF *Kiến trúc – Phân công*, mục 7 (thiết lập) và mục 8 (quy trình hằng ngày).
Thư mục `.github/` nằm ở **gốc git** (`major-IT4931_market-lakehouse/.github/`), không
nằm trong `btl-market-lakehouse/`. GitHub chỉ đọc workflow và CODEOWNERS ở gốc repo.
Bản CI cũ đặt trong thư mục con nên chưa bao giờ được chạy.

> **Tạm hoãn:** quy ước đặt tên nhánh của PDF (`<thư-mục>/<số-issue>-<mô-tả>`, PR vào
> `main`, squash merge) **chưa áp dụng**. Nhóm vẫn dùng nhánh vai `feature/<A|B|C|D>-...`
> theo `docs/branching.md` cho tới khi trưởng nhóm quyết định. Những phần dưới đây
> không phụ thuộc tên nhánh (CODEOWNERS, mẫu PR, CI, bí mật) đã có hiệu lực.

## Đã có trong repo

| File | Vai trò |
|---|---|
| `.github/workflows/ci.yml` | Job `python`: ruff, pytest (có Java 17 cho PySpark), kiểm tra mẫu dữ liệu khớp hợp đồng, demo local. Job `infra`: `bash -n`, `docker compose config`, file sinh tự động còn khớp, kustomize + kubeconform |
| `.github/CODEOWNERS` | `ingest/`→A, `streaming/`→B, `batch/`→C, `infra/`, `serving/`, `docs/`, `.github/`→D; `contracts/`, `common/`→cả 4 |
| `.github/pull_request_template.md` | Mô tả, cách chạy thử, `Closes #`, checklist duyệt theo PDF mục 8 |

## Trưởng nhóm làm một lần (PDF 7.1)

1. **Collaborators:** Settings → Collaborators → thêm 3 thành viên với quyền Write.
2. **CODEOWNERS:** thay `@ban_A`…`@ban_D` bằng username thật. Placeholder có dấu `_`,
   vốn không hợp lệ trong username GitHub, nên không thể vô tình gọi nhầm người lạ.
3. **Bảo vệ nhánh `main`:** bật Require a pull request (1 approval), Require review from
   Code Owners, Block force pushes. Repo private trên tài khoản free có thể không có tính
   năng này; khi đó chuyển repo sang Organization free hoặc dùng GitHub Student Pack.
4. **Projects (Board):** 4 cột Việc cần làm / Đang làm / Chờ duyệt / Xong. Mỗi việc trong
   thẻ vai A–D thành một Issue, gắn nhãn `A`…`D` và `tuan-1`…`tuan-6`.
5. **Bí mật:**
   - Key Alpaca/Finnhub/FRED chỉ nằm trong `.env` (đã có trong gitignore).
   - Trên K8s, `make secrets` tạo Secret `market-api-keys` và `minio-credentials`.
   - Trên Actions, dùng Settings → Secrets and variables. CI hiện không cần secret nào.

## Mỗi thành viên (PDF 7.2)

```bash
gh auth login
gh repo clone <owner>/major-IT4931_market-lakehouse
cd major-IT4931_market-lakehouse/btl-market-lakehouse
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env                     # điền API key của bạn
docker compose -f infra/docker-compose.dev.yml up -d && make topics
```

## Mỗi việc (PDF 8)

1. `git checkout <nhánh gốc> && git pull`
2. Tạo nhánh cho việc. Tên nhánh theo quy ước đang dùng, xem ghi chú "Tạm hoãn" ở trên.
3. Sửa code và chạy `python -m pytest`, `ruff check .`.
4. Commit nhỏ, thông điệp rõ ràng, có `(#issue)`.
5. `git push -u origin <nhánh>`, rồi `gh pr create --fill`, điền mẫu PR, ghi `Closes #<issue>`.
6. Có 1 người duyệt trong vòng 24 giờ và CI xanh thì merge.

PR đụng `contracts/` hoặc `common/` sẽ tự gọi cả 4 người duyệt qua CODEOWNERS, vì thay
đổi ở đó ảnh hưởng mọi vai (PDF: "nếu A đổi tên trường price thành p mà không báo, job
của B và C cùng hỏng").
