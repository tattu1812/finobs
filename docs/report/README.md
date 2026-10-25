# Khung báo cáo FinObs - IEEE two-column (Sprint 1)

## Phạm vi
Đây là **khung báo cáo**, không phải báo cáo nghiên cứu hoàn thiện. Các đoạn `TODO` là nơi nhóm bổ sung nội dung trong các sprint sau. Các tên tác giả và cơ quan phải được nhóm xác nhận, không tự điền.

## Cấu trúc
- `main.tex`: file chính, đặt chế độ IEEE conference hai cột và gọi các section.
- `sections/*.tex`: mỗi phần là một file riêng; thuận tiện cho các thành viên viết độc lập.
- `references.bib`: trích dẫn tài liệu sử dụng BibTeX (`IEEEtran`).
- `figures/`: lưu hình minh họa khi có, hiện chưa có hình chính thức.

## Cách biên dịch tại thư mục docs/report
Cần có một bản phân phối LaTeX như TeX Live hoặc MiKTeX và công cụ `latexmk`.

```bash
latexmk -pdf -interaction=nonstopmode -halt-on-error -outdir=build main.tex
```

Kết quả: `build/main.pdf`. Nếu không có `latexmk`, có thể dùng bộ lệnh pdflatex + bibtex, nhưng `latexmk` là cách ưu tiên.

Nếu chưa cài LaTeX trên Windows, có thể tải thư mục `docs/report/` lên Overleaf, đặt `main.tex` là main document và chọn trình biên dịch pdfLaTeX.

## Kiểm tra nghiệm thu Sprint 1
- [ ] `main.tex` sử dụng `\documentclass[conference]{IEEEtran}`.
- [ ] Các mục lớn nằm trong các file riêng.
- [ ] Tài liệu biên dịch không lỗi thành PDF hai cột.
- [ ] Có `references.bib` và trích dẫn hoạt động.
- [ ] Không có số liệu thực nghiệm hoặc kết quả hệ thống bị bịa/điền trước.
- [ ] Không chỉnh sửa ngoài `docs/report/` khi triển khai trên branch `s1-trung`.

## Git
Kiểm tra `git status` trước khi làm; chỉ `git add docs/report` và commit theo quy trình nhóm. Không tự động merge/push nếu chưa được yêu cầu.
