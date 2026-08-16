# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Đinh Văn Sinh  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Output ba lượt chạy cuối cùng</summary>

```
run 1/3 … 84.4s
run 2/3 … 92.8s
run 3/3 … 87.6s

BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG
gold_training_set     ✓ ok              12,480      12,480
gold_feature_daily    ✓ ok               9,100       9,100
gold_doc_chunks       ✓ ok              31,200      31,200
quarantine_tickets    ✓ ok                 312         312

CHECKSUM từng lượt
gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
gold_feature_daily    f9b805e1ac    f9b805e1ac    f9b805e1ac   ✓
gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

dbt test                                    ✓ 13/13 pass
silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
dashboard rows scanned                      ✓ 5,000,000 → 31,262 (159.9×)
  số file parquet                           ✓ 5,000 → 14
  kết quả truy vấn không đổi                ✓
DAG: catchup / max_active_runs              ✓ False / 1

TỔNG KẾT: 5/5 tiêu chí đạt
```

</details>

Tổng kết: **5 / 5 tiêu chí đạt**. Nhiệm vụ 5 cũng đạt thưởng: `make crash-test` báo `NHIỆM VỤ 5: ĐẠT ✓`.

---

## 1 · Bảng training phình lên sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau ba lượt chạy ban đầu, `gold_training_set` có 38.750 hàng thay vì 12.480; 12.480 ticket bị lặp và checksum thay đổi mỗi lượt. |
| **Nguyên nhân gốc** | Model `incremental` không khai báo khoá tự nhiên hay chiến lược upsert, nên dbt append các ticket thuộc cùng ngày `_ingested_at` khi retry. CDC có update nên một ticket hợp lệ có thể xuất hiện lại; retry vì vậy trở thành nhân bản dữ liệu. `catchup=True` và không giới hạn active run còn làm tăng nguy cơ chạy bù/chồng lấp. |
| **Cách sửa** | Trong `dbt/models/gold/gold_training_set.sql`: `unique_key='ticket_id'`, `incremental_strategy='delete+insert'`, `on_schema_change='fail'`; vẫn giữ filter theo `run_date`. Trong `dags/ai_training_pipeline.py`: `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | Trước: 38.750 hàng. Sau: 12.480 hàng, không ticket trùng; checksum ba lượt đều `8dd7c98653`; DAG `False / 1`. |

---

## 2 · Thiếu hàng, không ai biết

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` ổn định nhưng chỉ có 8.645 thay vì 9.100 tổ hợp `(event_date, customer_id)`; thiếu ở các ngày cũ. |
| **P99 độ trễ đo được** | **2,720104 ngày**; P50 = 0,127847, P95 = 1,811458 và max = 2,944688 ngày. |
| **Lookback đã chọn** | **3 ngày** = `ceil(P99)`. |
| **Nguyên nhân gốc** | Điều kiện `event_date > max(event_date)` chỉ xem ngày sự kiện mới hơn Gold. Event xảy ra trong quá khứ nhưng đến Bronze muộn có `event_date` nhỏ hơn max nên bị loại vĩnh viễn. |
| **Cách sửa** | Đặt `lookback_days: 3` trong `dbt_project.yml`. `gold_feature_daily` dùng `unique_key=['event_date', 'customer_id']`, `delete+insert`, và tính lại `event_date >= max(event_date) - interval 3 day`. |
| **Bằng chứng** | Trước: 8.645 hàng, thiếu 455 tổ hợp. Sau: 9.100 hàng, không còn tổ hợp thiếu; checksum ba lượt `f9b805e1ac`. |

Chọn P99 giúp cửa sổ tính lại phản ánh độ trễ vận hành điển hình thay vì bị kéo dài vô hạn bởi một outlier. Đổi lại, phần đuôi trên P99 cần được theo dõi/quét bù riêng. Với seed này, `ceil(max)` cũng là 3 nên chi phí hiện tại không khác, nhưng nguyên tắc chọn vẫn quan trọng khi dữ liệu thật có outlier lớn.

---

## 3 · Schema đổi giữa chừng

| | |
|---|---|
| **Triệu chứng** | `priority_raw` đổi từ số sang nhãn chuỗi; `try_cast` biến nhãn hợp lệ thành `NULL`. Silver ban đầu có 6.488 `NULL` cùng các giá trị ngoài miền như `-1`, `0`, `5`. |
| **Nguyên nhân gốc** | Pipeline chỉ ép kiểu số, không mô hình hoá schema evolution. Hơn nữa, nếu chọn bản ghi CDC mới nhất rồi mới loại lỗi, một update lỗi sẽ làm mất luôn ticket có bản ghi hợp lệ trước đó. |
| **Ba nhóm giá trị `priority` và cách xử lý** | `1..4`: giữ nguyên; `urgent/high/medium/low`: ánh xạ thành `1/2/3/4`; `P1`, `P2`, `unknown`, rỗng, `NULL`, `-1`, `0`, `5`: cách ly. |
| **Cách sửa** | Tạo macro `normalize_priority`; trong `silver_tickets.sql` chuẩn hoá, lọc bản ghi valid, rồi mới rank theo `(event_time, cdc_seq)`. Thêm model `quarantine_tickets` grain 1 hàng/1 CDC lỗi. Bật `contract.enforced: true`, thêm `not_null` và `accepted_values` cho `priority`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng; `dbt test` = 13/13 pass; `priority` chỉ còn 1..4 và không NULL; `silver_tickets` vẫn 12.480 ticket. |

Tôi chặn ở Silver: Bronze cần giữ payload nguồn để điều tra/audit, còn Silver là ranh giới chuẩn hoá cho downstream. Không để 312 bản ghi lỗi làm sập DAG vì các event và transcript lành vẫn cần tới người dùng; bản ghi lỗi được tách thành queue có lý do để xử lý sau.

---

## 4 · Dashboard chậm

| | |
|---|---|
| **Triệu chứng** | Dashboard đọc 5.000 Parquet nhỏ không phân vùng; predicate ngày dùng `strftime(event_time, ...)`, nên không thể pruning theo partition/statistics. |
| **Nguyên nhân gốc** | Small-file problem làm mỗi file nhỏ vẫn bị tính như một batch scan; tên đường dẫn không chứa ngày, và cột lọc bị bọc hàm. Vì vậy DuckDB phải mở toàn bộ bãi dữ liệu trước khi lọc. |
| **Cách sửa** | `tools/compact.py` ghi lại sang `data/gold_events_v2/`, `PARTITION_BY(event_date)`, `ORDER BY event_date, customer_name`, `ROW_GROUP_SIZE 8192`. Query dùng `hive_partitioning=true` và `event_date = DATE '2026-08-09'`. |

| Chỉ số | Trước | Sau | Tỷ lệ |
|---|---:|---:|---:|
| `rows scanned` | 5.000.000 | 31.262 | 159,9× giảm |
| số file | 5.000 | 14 | giảm |
| `result hash` | `17a8f70172a7` | `17a8f70172a7` | giống nhau |

Đo `rows scanned` thay vì thời gian vì thời gian phụ thuộc CPU, cache và tải máy; rows scanned đo trực tiếp lượng công việc mà engine buộc phải làm. Trên bộ dữ liệu nhỏ, partition theo ngày tạo phần lớn mức giảm; sort/row-group chuẩn bị cho partition lớn hơn.

---

## 5 · *(mở rộng)* Consumer và sự cố giữa lô

| | |
|---|---|
| **Kịch bản gốc cho ra** | Crash ở batch 7 sau khi offset đã commit 3.500: mất 500 hàng; không trùng vì batch đó bị bỏ qua. |
| **Nguyên nhân gốc** | Commit offset trước khi ghi là at-most-once. Crash trong khoảng giữa làm log quên batch trong khi sink chưa nhận batch. |
| **Cách sửa** | Đổi thứ tự thành `write_batch → maybe_crash → commit`; thêm `event_id PRIMARY KEY` và `ON CONFLICT(event_id) DO UPDATE` cho sink idempotent. |
| **Kết quả `make crash-test`** | Offset sau crash = 3.000; restart phát lại batch 7; cuối cùng 20.000 hàng / 20.000 event_id, không mất và không trùng: **ĐẠT ✓**. |

`DO NOTHING` giữ payload đầu tiên khi event được phát lại, nên có thể bỏ qua nội dung mới. `DO UPDATE` cập nhật payload theo message phát lại; cùng `event_id` vẫn chỉ có một hàng. Kết quả là at-least-once delivery cộng với idempotent write tạo hiệu ứng exactly-once tại sink.

---

## 6 · Một dòng cho mỗi bài học

| Nhiệm vụ | Nếu gặp lại hệ thống lạ, tôi sẽ kiểm tra điều này đầu tiên |
|---|---|
| 1 | Grain và natural key của bảng đã được khai báo ở tầng ghi chưa? |
| 2 | Dữ liệu về muộn được đo bằng distribution nào, và cửa sổ backfill có upsert theo đúng grain không? |
| 3 | Giá trị lạ là schema evolution hay dữ liệu bẩn; bản ghi lỗi bị lọc trước hay sau khi chọn trạng thái mới nhất? |
| 4 | Predicate có prune được partition/row group không, và số file nhỏ đang tạo chi phí scan bao nhiêu? |

---

## 7 · Tự chấm trước khi nộp

| Mục rubric | Bằng chứng | Tự đánh giá |
|---|---|---|
| A · Ổn định | Bốn checksum giống nhau qua ba lượt | 30/30 |
| B · Tính đúng | 12.480 / 9.100 / 31.200, không ticket trùng | 30/30 |
| C · Chất lượng dữ liệu | Contract bật, 13/13 test, quarantine 312, priority sạch | 15/15 |
| D · Hiệu năng | 5.000.000 → 31.262 rows scanned, hash không đổi, 5.000 → 14 file | 15/15 |
| E · Báo cáo | Bốn nguyên nhân gốc, P99 và before/after được nêu rõ | Tự đánh giá 10/10 |
| Thưởng · Nhiệm vụ 5 | `make crash-test`: ĐẠT | +5 |

Tự đánh giá: **105 điểm trước khi giảng viên chấm phần báo cáo**; không có thay đổi ở `expected/`, `seed/generate.py`, `tools/verify.py`, `tools/explain.py` hoặc `tools/common.py`.
