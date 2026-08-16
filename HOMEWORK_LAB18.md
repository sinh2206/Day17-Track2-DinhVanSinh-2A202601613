# Bài viết về nhà — nộp cùng Lab 18

**Họ tên:** Đinh Văn Sinh  
**Lớp:** AICB-P2T2  
**Ngày:** 17/08/2026

Nếu hệ thống tăng 50 lần, tức khoảng 20 triệu ticket mỗi ngày và sáu team cùng sử dụng, tôi sẽ thay ba lựa chọn hạ tầng dựa trên ràng buộc vận hành sau.

**DuckDB → Spark.** Ở quy mô lab, một file DuckDB trên một máy là hợp lý vì dữ liệu vừa bộ nhớ và chỉ có một luồng transform. Với 20 triệu ticket/ngày, lượng history, backfill và các truy vấn đồng thời của sáu team sẽ vượt khả năng I/O, RAM và thời gian batch của một máy duy nhất. Ràng buộc quyết định là SLA hoàn thành transform/backfill và năng lực của một node, không phải vì Spark có nhiều tính năng hơn. DuckDB vẫn phù hợp cho phân tích cục bộ, kiểm tra mẫu dữ liệu và phát triển SQL; Spark đảm nhận job production phân tán trên data lake.

**Airflow → Dagster.** Khi sáu team cùng thay đổi những Gold table dùng chung, rủi ro lớn là không biết ai sở hữu dữ liệu nào và một thay đổi upstream sẽ ảnh hưởng tới downstream nào. Ràng buộc ở đây là ownership, hợp đồng dữ liệu và khả năng đánh giá tác động giữa các team, thay vì đơn thuần là số lượng DAG. Tôi sẽ chuyển sang mô hình điều phối hướng asset của Dagster để mỗi bảng Gold là một đơn vị có owner, dependency và điều kiện materialize rõ ràng; những pipeline Airflow đang ổn định chỉ chuyển khi chúng trở thành một phần của chuỗi asset liên team.

**Kafka → WarpStream, nếu tổ chức đã có object storage và không có đội vận hành broker chuyên trách.** Ở lưu lượng lớn, việc tự chịu trách nhiệm capacity, replication, disk, network và nâng cấp nhiều Kafka broker có thể trở thành ràng buộc chi phí vận hành lớn hơn bản thân xử lý event. Khi độ trễ end-to-end cho phép và object storage đáp ứng được yêu cầu lưu giữ/replay, WarpStream giảm gánh nặng vận hành này. Ngược lại, nếu có yêu cầu latency rất thấp, connectivity đặc thù hoặc một đội platform đã vận hành Kafka hiệu quả, tôi sẽ giữ Kafka. Quyết định vì thế dựa vào latency SLO và năng lực vận hành, không dựa vào tên công nghệ.
