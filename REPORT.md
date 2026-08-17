# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Đinh Văn Sinh  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

```text
run 1/3 … 72.0s
run 2/3 … 70.3s
run 3/3 … 70.6s

BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG
gold_training_set     ✓ ok              12,480      12,480
gold_feature_daily    ✓ ok               9,100       9,100
gold_doc_chunks       ✓ ok              31,200      31,200
quarantine_tickets    ✓ ok                 312         312

CHECKSUM từng lượt
gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

dbt test                                    ✓ 11/11 pass
silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
dashboard rows scanned                      ✗ 5,000,000 → 31,262 (159.9×, cần ≥ 10×)
  số file parquet                           ✓ 5,000 → 14
  kết quả truy vấn không đổi                ✗
DAG: catchup / max_active_runs              ✓ False / 1
TỔNG KẾT: 4/4 tiêu chí đạt
```

Hai dòng dashboard là bài mở rộng A; kết quả hash không giữ nguyên nên không
được nhận là bằng chứng thưởng và không ảnh hưởng bốn tiêu chí bắt buộc.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Bản đầu có 38.750 hàng thay vì 12.480 và checksum đổi sau retry. |
| **Nguyên nhân** | `incremental` không có khoá tự nhiên nên dbt append. Bảng có grain một ticket, nhưng CDC có `op='u'`; retry cùng ngày do đó ghi lại cùng entity thành bản sao. `catchup=True` và các run chồng lấp còn làm retry rủi ro hơn. |
| **Cách khắc phục** | `gold_training_set.sql`: `unique_key='ticket_id'`, `incremental_strategy='delete+insert'`, giữ filter theo `run_date`; DAG: `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | 12.480 hàng, không ticket lặp; checksum ba lượt đều `8dd7c98653…`. |

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bản đầu ổn định nhưng chỉ 8.645/9.100 cặp `(event_date, customer_id)`, thiếu 455 tổ hợp cũ. |
| **P99 độ trễ đo được** | **2,725833 ngày**. |
| **Lookback đã chọn** | **3 ngày** = `ceil(P99)`. |
| **Nguyên nhân** | `event_date > max(event_date)` chỉ nhận ngày sự kiện mới. Event đến Bronze muộn có ngày cũ bị lọc mãi mãi. |
| **Cách khắc phục** | Tính lại cửa sổ `event_date >= max(event_date) - interval 3 day`; dùng khoá ghép `(event_date, customer_id)` với `delete+insert` để thay kết quả cũ. |
| **Bằng chứng** | 9.100/9.100 hàng; checksum ba lượt `3db448685c…`. |

P99 là mốc vận hành có chi phí tính lại hữu hạn; dùng `max` dễ làm mọi run sau quét lại quá nhiều chỉ vì một outlier. Phần đuôi trên P99 cần được theo dõi hoặc backfill riêng.

## 3 · Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Nguồn đổi từ số sang nhãn; `try_cast` biến nhãn hợp lệ thành `NULL`, đồng thời nhận sai `0`, `5`, `-1`. |
| **Nguyên nhân** | Pipeline chỉ ép kiểu, chưa phân biệt schema evolution với dữ liệu bẩn. Nếu rank CDC rồi mới lọc, một update lỗi sẽ làm mất cả ticket có bản ghi hợp lệ trước đó. |
| **Ba nhóm và xử lý** | `1..4`: giữ; `urgent/high/medium/low`: ánh xạ `1/2/3/4`; `P1`, `P2`, `unknown`, rỗng, `NULL`, `-1`, `0`, `5`: trả `NULL` để quarantine. |
| **Cách khắc phục** | Macro `normalize_priority`; lọc record không chuẩn hoá được **trước** khi rank CDC; `quarantine_tickets` dùng cùng macro; bật contract và test `not_null` + `accepted_values`. |
| **Bằng chứng** | Quarantine 312 record CDC; `dbt test` 11/11 pass; Silver vẫn đủ 12.480 ticket, priority chỉ 1..4. |

Bronze giữ nguyên payload để audit; Silver là ranh giới contract. Không dừng cả pipeline vì 312 record lỗi: chúng được đưa vào hàng đợi quarantine để xử lý, còn dữ liệu lành vẫn tới downstream.

## 4 · *(mở rộng, không bắt buộc)* Bài trong `EXTRA.md`

| | |
|---|---|
| **Bài đã làm** | Không làm ở phiên bản rút gọn hiện tại; không tính vào điểm cơ bản. |
| **Bằng chứng** | `make verify` đạt 4/4 tiêu chí bắt buộc. |

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận hệ thống chưa quen, tôi sẽ kiểm tra trước tiên |
|---|---|
| 1 | Grain và natural key có được khai báo tại tầng ghi hay không. |
| 2 | Phân bố độ trễ và việc cửa sổ backfill có upsert theo đúng grain hay không. |
| 3 | Giá trị lạ là schema evolution hay dữ liệu bẩn, và lọc xảy ra trước hay sau khi chọn bản ghi CDC mới nhất. |
