# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Thị Thanh Hiền
**Lớp:** E403
**Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

```text
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
DAG: catchup / max_active_runs              ✓ False / 1

TỔNG KẾT: 4/4 tiêu chí đạt
```

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

|                    |                                                                                                                                                                                                                                                                                                                                                                   |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**    | `gold_training_set` tăng số hàng sau mỗi lần pipeline được chạy lại. Baseline sau ba lượt có 38.750 hàng trong khi kỳ vọng chỉ có 12.480. Nhiều `ticket_id` xuất hiện lặp lại.                                                                                                                                                                                    |
| **Nguyên nhân**    | `gold_training_set` là incremental model có grain `1 hàng / 1 ticket` nhưng ban đầu không khai báo `unique_key` và chiến lược ghi phù hợp. Vì vậy dbt ghi thêm các row được xử lý lại thay vì đối chiếu theo `ticket_id` để cập nhật row cũ. Khi pipeline retry hoặc một ticket có bản ghi CDC `op='u'`, cùng một ticket tiếp tục được ghi thêm và tạo duplicate. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_training_set.sql`, khai báo `unique_key='ticket_id'` và `incremental_strategy='merge'`, đồng thời giữ nguyên điều kiện lọc theo `run_date`. Trong `dags/ai_training_pipeline.py`, đặt `catchup=False` và `max_active_runs=1` để hạn chế các run lịch sử hoặc run đồng thời ghi vào cùng bảng.                                         |
| **Bằng chứng**     | Trước: 38.750 hàng sau ba lượt verify, nhiều `ticket_id` bị lặp. Sau: **12.480 hàng**, đúng `1 hàng / 1 ticket`; checksum ba lượt đều bằng **`8dd7c98653`**.                                                                                                                                                                                                      |

**Kết luận:** lỗi nằm ở phép ghi incremental không idempotent, không phải ở dữ liệu nguồn. `silver_tickets` có 12.480 hàng và 12.480 `ticket_id` khác nhau, chứng minh nguồn Silver không bị duplicate.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

|                        |                                                                                                                                                                                                                                                                                                    |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**        | `gold_feature_daily` ổn định qua các lần chạy nhưng chỉ có 8.645 hàng thay vì 9.100. Các hàng thiếu thuộc những ngày cũ dù dữ liệu mới vẫn được xử lý bình thường.                                                                                                                                 |
| **P99 độ trễ đo được** | **2.725833 ngày**                                                                                                                                                                                                                                                                                  |
| **Lookback đã chọn**   | **3 ngày**, vì P99 ≈ 2,726 ngày nên làm tròn lên để pipeline tính lại đủ cửa sổ mà 99% dữ liệu tới muộn nằm trong đó.                                                                                                                                                                              |
| **Nguyên nhân**        | Điều kiện incremental ban đầu chỉ lấy `event_date > max(event_date)` của bảng đích. Một event xảy ra ở ngày cũ nhưng tới warehouse vài ngày sau sẽ có `event_date` nhỏ hơn ngày lớn nhất đã xử lý, nên bị bỏ qua vĩnh viễn. Bảng vì thế có thể ổn định qua nhiều lần chạy nhưng vẫn thiếu dữ liệu. |
| **Cách khắc phục**     | Trong `dbt/models/gold/gold_feature_daily.sql`, thay điều kiện chỉ xử lý ngày mới bằng lookback 3 ngày. Đồng thời khai báo `unique_key=['event_date', 'customer_id']` và `incremental_strategy='merge'` để các cặp ngày–khách hàng được tính lại nhưng không bị cộng dồn hoặc duplicate.           |
| **Bằng chứng**         | Trước: **8.645 hàng**. Sau: **9.100 hàng**. Checksum ba lượt đều bằng **`3db448685c`**.                                                                                                                                                                                                            |

### Vì sao dùng P99 thay vì `max`?

Phân bố độ trễ đo được:

* P50 ≈ 0,128 ngày
* P95 ≈ 1,814 ngày
* **P99 ≈ 2,726 ngày**
* Max ≈ 2,945 ngày
* Khoảng 5,05% event tới muộn hơn một ngày

P99 phản ánh cửa sổ bao phủ phần lớn dữ liệu tới muộn nhưng ít bị chi phối bởi một vài giá trị ngoại lệ như `max`. Mỗi ngày lookback thêm vào làm tăng lượng dữ liệu phải đọc, aggregate và merge ở **mọi lần chạy sau**, nên cửa sổ quá rộng sẽ tạo chi phí tính toán thường xuyên. Trong bộ dữ liệu này P99 và max đều làm tròn thành 3 ngày, nhưng nguyên tắc lựa chọn vẫn dựa trên phân bố đo được thay vì chọn một giá trị cực đại theo cảm tính.

---

## 3 · Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

|                                              |                                                                                                                                                                                                                                                                                                                                                                                                |
| -------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**                              | Pipeline vẫn chạy và 9 test ban đầu đều pass nhưng `silver_tickets.priority` có **6.606 hàng sai**, gồm NULL và các giá trị ngoài miền `1..4`. `quarantine_tickets` ban đầu có 0 hàng.                                                                                                                                                                                                         |
| **Nguyên nhân**                              | Nguồn thay đổi cách biểu diễn `priority` từ số `1..4` sang nhãn `urgent/high/medium/low`. Logic cũ không phân biệt schema evolution hợp lệ với dữ liệu thực sự hỏng, nên các nhãn chữ không được chuẩn hóa đúng. Đồng thời contract chưa được enforce và chưa có test cho miền giá trị, khiến pipeline vẫn chạy dù dữ liệu Silver vi phạm yêu cầu nghiệp vụ.                                   |
| **Ba nhóm giá trị `priority` và cách xử lý** | **Nhóm 1:** `1,2,3,4` → giữ nguyên. **Nhóm 2:** `urgent/high/medium/low` → map thành `1/2/3/4`. **Nhóm 3:** `P1`, `unknown`, `0`, `5`, `-1`, chuỗi rỗng, `NULL` → xem là bản ghi lỗi và đưa vào quarantine.                                                                                                                                                                                    |
| **Cách khắc phục**                           | Sửa `dbt/macros/normalize_priority.sql` để chuẩn hóa cả số và nhãn chữ hợp lệ, trả `NULL` cho nhóm lỗi. Trong `silver_tickets.sql`, lọc bản ghi lỗi **trước khi** `row_number()` chọn trạng thái mới nhất. Trong `quarantine_tickets.sql`, định tuyến đúng các row mà macro trả về `NULL`. Trong `schema.yml`, bật `contract: enforced: true`, thêm `not_null` và `accepted_values [1,2,3,4]`. |
| **Bằng chứng**                               | `quarantine_tickets` = **312 hàng**; `silver_tickets.priority` sạch và luôn thuộc `1..4`; `silver_tickets` vẫn đủ **12.480 ticket**; `dbt test` = **11/11 pass**.                                                                                                                                                                                                                              |

### Nên chặn ở Bronze hay Silver?

Nên giữ Bronze gần với dữ liệu nguồn nhất để còn đầy đủ bằng chứng phục vụ audit, replay và điều tra sự cố. Nếu Bronze từ chối ngay các row bất thường thì dữ liệu gốc có thể bị mất, khiến việc xác định nguồn phát sinh lỗi về sau khó khăn hơn.

Việc chuẩn hóa và phân loại nên thực hiện ở Silver: dữ liệu biểu diễn khác nhưng vẫn hợp lệ được normalize, còn row thực sự vi phạm quy tắc được đưa vào quarantine.

### Vì sao không dừng toàn bộ pipeline?

Chỉ có **312 bản ghi lỗi** trong khi phần lớn dữ liệu vẫn hợp lệ và cần tiếp tục phục vụ các downstream model. Dừng toàn bộ DAG vì một số ít row lỗi sẽ làm giảm availability của pipeline. Quarantine cho phép cô lập chính xác dữ liệu cần điều tra trong khi phần dữ liệu tốt vẫn được xử lý bình thường.

---

## 4 · Bài mở rộng

**Không thực hiện.**

Hai bài trong `EXTRA.md` là bài thưởng và không cần thiết để đạt đủ điểm cho ba nhiệm vụ chính.

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên                                                                               |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1**    | Xác định grain và natural key của bảng, sau đó kiểm tra cách incremental model ghi dữ liệu có idempotent khi retry hay không.                           |
| **2**    | So sánh thời điểm sự kiện xảy ra với thời điểm dữ liệu được ingest để xác định late-arriving data, sau đó đo percentile trước khi chọn lookback window. |
| **3**    | Kiểm tra phân bố giá trị và contract của dữ liệu để phân biệt schema evolution với dữ liệu hỏng, tránh loại nhầm dữ liệu vẫn còn ý nghĩa.               |

### Kết quả cuối cùng

* `gold_training_set`: **12.480** ✓
* `gold_feature_daily`: **9.100** ✓
* `gold_doc_chunks`: **31.200** ✓
* `quarantine_tickets`: **312** ✓
* `silver_tickets`: **12.480 ticket** ✓
* `dbt test`: **11/11 pass** ✓
* Checksum ba lượt chạy: **giống hệt nhau** ✓
* `make verify`: **4/4 tiêu chí đạt** ✓
