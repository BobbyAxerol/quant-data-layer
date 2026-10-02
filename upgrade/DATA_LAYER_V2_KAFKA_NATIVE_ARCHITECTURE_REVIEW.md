# Data Layer V2 — Đánh giá kiến trúc và đề xuất quay về đúng bản chất Kafka

> [!IMPORTANT]
> **CLAUDE OPUS 5.5: ĐỌC MỤC 18 TRƯỚC KHI IMPLEMENT.** Owner đã duyệt bản
> [thống nhất NĂM phase tại mục 18](#kn-v220-approved-guide) ngày 2026-09-23.
> Mục 0-17 phía dưới là lịch sử thảo luận, không phải năm kế hoạch cạnh tranh.
> Thiết kế hiện hành = mục 18; status/dev logs/evidence =
> [Unified Plan KN-1 đến KN-5](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#kn-v220-plan).
> Claude implement; Astra review từng phase. **Chưa có phase mới nào được thực thi
> hoặc certified chỉ vì tài liệu này đã được duyệt.** Các dòng "bản thảo/4 phase"
> dưới đây được giữ nguyên như lịch sử, không ghi đè quyết định mới.

> **Trạng thái:** bản thảo để thảo luận (2026-09-23). Chưa có quyết định. Không có thay đổi
> runtime hay code nào đi kèm tài liệu này.
> **Người đọc:** owner + Astra, dùng làm đầu vào để chốt 4 phase sửa kiến trúc.
> **Quy ước bằng chứng:** mọi con số đều ghi lệnh/nguồn tạo ra nó (Phụ lục A). Chỗ nào chưa
> kiểm được ghi rõ **[chưa kiểm]**, ước lượng ghi **[ước lượng]**. Đường dẫn code tính trong
> repo `/home/bobby/data_layer`, commit `83fa1bc`.

---

## 0. Tóm tắt một trang

1. **Kafka đã làm đúng việc, nhưng sau đó bị làm lại một lần nữa theo cách yếu hơn.**
   rust_core ghi `md.canonical.v2` theo transaction exactly-once, RF3, 6 partition, giữ
   thứ tự theo key. Sau Kafka, toàn bộ 6 partition lại bị dồn qua HTTP vào **một process
   Python duy nhất** để ghi thêm một bản log thứ hai (SQLite, fsync). Tính song song của
   Kafka bị triệt tiêu. Trần hệ thống là ~1.200 event/s, đã đo 4 lần.
2. **99,7% dữ liệu ghi vào SQLite là loại không consumer nào đọc lại làm lịch sử.**
   Tick (TRADE/QUOTE/BOOK/MARK) chiếm 99,7% số event trong spool. Trong **cả 6 manifest
   consumer**, mọi feed tick đều có `warmup_limit: 0`. Chỉ BAR cần lịch sử, và BAR chỉ
   chiếm **0,3%** lượng ghi. Hệ thống đang trả chi phí fsync + khoá + GIL cho dữ liệu
   không ai dùng theo cách đó.
3. **Kiến trúc này là kết quả bồi đắp, không phải một lựa chọn tổng thể.** SQLite (08-13)
   và gateway active/passive (08-14) có trước Kafka (08-15). Ở Phase B, Kafka được đặt
   *phía dưới* gateway cũ thay vì xây gateway mới *trên* Kafka. SQLite chỉ đổi nhãn thành
   "cache", còn thủ tục gỡ bỏ ghi trong ADR-0006 chưa bao giờ được thực hiện.
4. **Phần đáng giữ rất nhiều.** Hợp đồng với consumer (proto, SDK, cursor ký HMAC, giao
   thức phục hồi, manifest) **độc lập với backend**. Nhờ vậy có thể thay phần lõi mà TS và
   alpha gần như không phải sửa.
5. **Đề xuất (phương án C, "Kafka-native"):**
   - Kafka là nguồn sự thật duy nhất.
   - Luồng realtime đọc Kafka trực tiếp, song song, không cần lease.
   - Giá trị mới nhất (latest) nằm trong Redis như một cache thuần có thể dựng lại.
   - Chỉ BAR có kho lịch sử riêng (tốc độ ghi thấp, SQLite là đủ).
   - Bỏ chặng HTTP, bỏ writer đơn, bỏ lưu tick vào SQLite.
   - Làm trong 4 phase, chạy shadow song song với stack hiện tại, rollback bằng cách
     trỏ consumer về stack cũ. **[ước lượng]** 2–3 tuần.

---

## 1. Mục tiêu và ràng buộc của owner

| Hạng mục | Mục tiêu |
|---|---|
| Thông lượng | Chịu được **2.000–3.000 event/s** bền vững, thị trường sôi động hay không; có dư địa khi burst |
| Latency (p95 / p99) | QUOTE/TRADE ≤100/≤250 ms · MARK_INDEX ≤250/≤500 ms · L2 ≤300/≤750 ms · BAR ≤1000/≤2000 ms |
| Tải kiểm chứng | 50 workload alpha (20/15/10/5) + TS 60 route thật (`config/v2/v211-target-acceptance-budget.json`) |
| Tính đúng | Có thứ tự theo slice, không mất dữ liệu, replay được, phát hiện gap. Đây là lý do có Kafka |
| Tài nguyên | Tận dụng máy hiện có (16 vCPU / 30 GB), **không phung phí**; không thêm service khi service có sẵn làm được (rule 3b) |
| Vận hành | V2 là nguồn chính, V1 là fallback; rollback theo digest; không làm gãy TS/alpha |
| Thời gian | Không kéo dài thêm; mỗi phase có cổng thoát đo được |

---

## 2. Hiện trạng

### 2.1 Đường đi dữ liệu

```text
Sàn WS ─► ingestor (Rust) ─► Kafka md.raw.realtime.v2
        ─► rust_core ×3 (transaction EOS) ─► Kafka md.canonical.v2  (6 partition, RF3, 6h)
        ─► projector ×6 (Python, mỗi cái 1 partition)
              │  decode + kiểm tra lại + đọc SQLite để dedup + tính lại sha raw
              │  JSON + base64 + HMAC
              ▼
           HTTP POST ─► stream_v2_active  → 409 (không giữ lease)
                     ─► stream_v2_passive → 200 (đang giữ lease)   ← MỘT process Python
                            decode + kiểm tra lại lần nữa + sha lần nữa
                            SQLite append_many (1 file 3,3 GB, synchronous=FULL, fsync)
                            fan-out gRPC cho ~100 subscriber + live view MARK/INDEX
              │  (sau ACK) projector đọc lại SQLite để xác minh offset
              ▼
           Redis stable_redis: latest + key tương thích V1 + lease + cache identity
Query ×2 ──► đọc CHUNG file SQLite đó (snapshot, warmup, history)
```

Bằng chứng: `qdl/runtime/stable_projector.py:341-351,863-1031`,
`qdl/runtime/stable_ingest.py:141-368,495-578`, `qdl/stream/gateway.py:356-408`,
`qdl/transport/sqlite_spool.py:196-202,536-702`, `qdl/runtime/lease.py:186-326`,
`qdl/projection/stable.py:132-416`.

### 2.2 Số đo hôm nay (≈15:00–15:10 UTC, 2026-09-23)

| Chỉ số | Giá trị | Nguồn (Phụ lục A) |
|---|---|---|
| Lượng sản sinh canonical (thị trường đang yên) | **≈835 event/s** (53.431 event / 64 s) | A1 |
| Tốc độ writer tiêu thụ khi có backlog | **≈1.178 event/s** (75.418 / 64 s) | A1 |
| Trần writer đo trước đó | 1.240/s (09-16), ~1.600/s có backlog 48 s (07:44Z), 1.100–1.200/s (stage 35) | Plan `dl-v2-r1-outcome-20260916`, journal 09-23 |
| Lượng raw khi thị trường sôi động | 2.749 raw/s (09-16, backlog lên 2,4 triệu) | Plan:38913-38973 |
| Backlog hiện tại | p3 = 642.232 → 626.222, p5 = 317.506 → 305.777 trong 64 s (p3 giảm ~200/s, p5 ~130/s); 15:58Z p3 còn 35.292, các partition khác < 1.000 | A1, A10 |
| Lệch tải partition | p3 và p5 nhận **57%** lưu lượng; p3 ≈ 4,2 lần p1 | A1 |
| POST lãng phí | mỗi batch = 1 lần 409 + 1 lần 200 (vd. 85/85 mỗi phút) | A2 |
| CPU máy | bận ~87% (67% us, 13% sy, 4,6% wa); **V2 dùng ≈7,8 vCPU** | A3 |
| Kafka canonical / raw trên đĩa (1 broker) | 7.062 MB cho 6h (≈1,18 GB/h) so với 7.940 MB cho 8h (≈0,99 GB/h) | A4 |
| Spool SQLite | 3,30 GB + WAL 64 MB | A5 |
| Tỷ lệ event trong spool theo feed | trade 43,5% · book 24,9% · quote 23,4% · mark 7,8% · **bar 0,3%** | A6 |
| Số key | 188 key, trong đó **140 là BAR** | A6 |
| Feed cần warmup (`warmup_limit>0`) trong 6 manifest | **chỉ BAR** | A7 |
| Code đường lõi projector/stream | 13 module, 8.159 dòng; `qdl/` tổng 59.103 dòng | báo cáo đọc code |

Nhận xét: **topic canonical nặng hơn topic raw** tính theo giờ, dù dữ liệu canonical đã
chuẩn hoá lẽ ra phải nhỏ hơn. Nguyên nhân là mọi bản ghi canonical đều mang theo toàn bộ
raw envelope trong header `qdl-raw-provider-envelope`
(`rust/qdl-kafka/src/bin/qdl-realtime-core.rs:412`, `rust/qdl-kafka/src/lib.rs:836-839`).
Dữ liệu raw vì vậy được lưu 2 lần, rồi nhân 3 bản sao.

### 2.3 Giới hạn tài nguyên từng role (docker inspect)

| Role | CPU | RAM |
|---|---|---|
| kafka1 / kafka2 / kafka3 | 1,25 / 1,75 / 1,75 | 1,5 / 2 / 1,5 GiB |
| rust_core ×3 | 1,0 + 0,5 + 0,5 | 256 MiB mỗi cái |
| ingestor ×2 | 0,5 mỗi cái | 256 MiB |
| projector ×6 | 1,0 mỗi cái | 768 MiB |
| stream active + passive | 2,0 mỗi cái | 1 GiB |
| query ×2 | 1,5 mỗi cái | 1 GiB |
| stable_redis | 0,5 | 160 MiB |
| binance_bar_edge | 0,75 | 512 MiB |
| **Tổng trần** | **≈19,3 vCPU** trên máy 16 vCPU | |

---

## 3. Tại sao lại thành thiết kế SQLite spool?

Đây là phần quan trọng nhất để không lặp lại sai lầm. Nhìn lại lịch sử, **từng bước đều
hợp lý tại thời điểm của nó**. Cái sai nằm ở chỗ không bao giờ quay lại xem xét tổng thể.

### 3.1 Dòng thời gian

| Ngày | Quyết định | Lý do được ghi lại |
|---|---|---|
| 08-13 | Thêm SQLite WAL spool (`0ea4f52`) | ADR-0006 "Why Not Kafka Yet": chứng minh ngữ nghĩa replay/cursor cho **2 symbol** mà không phải vận hành một cụm Kafka. ADR ghi rõ: *"not the long-term canonical backbone"*, *"single-host failure-domain bridge… must never be promoted to broad-universe/book-delta authority"*, và có **Sunset Procedure 5 bước** |
| 08-13 | Guide §7.2 | Stage C: chuyển sang Kafka khi thông lượng, số consumer hoặc HA **vượt vùng an toàn của cầu tạm** |
| 08-14 | Phase 7.1 chọn gateway **active/passive** (`fa8cbb2`) | Plan:1263-1274 ghi 2 lựa chọn: (1) active/passive với lease, (2) **gateway theo partition dùng offset gốc của broker**. Chọn (1), **không ghi so sánh**. Lý do suy ra: ngày đó chưa có Kafka, nên (2) không làm được |
| 08-14 | Phase 7.2: Query và Stream dùng **chung một file spool** | Để snapshot và stream cursor chung một watermark (PHASE72 report:16-17) |
| 08-15 | Kafka RF3 ra đời (Phase 8.0) | Muộn hơn quyết định active/passive **một ngày** |
| 08-19 | Phase B: Kafka thành "replay authority" (`fcfd862`) | Giữ nguyên gateway, endpoint HTTP ingest và spool; thêm projector Python làm cầu Kafka→HTTP→gateway. SQLite được đổi nhãn thành *"rebuildable bounded cache"* (Plan:3992, 4038) |
| 08-19 | Redis + SQLite gộp thành "một đơn vị cache" có cache identity (`6af2f25`) | Để fence khi rebuild. Hệ quả: Redis mất thì projector từ chối chạy (`ProjectionCacheMismatch`) |
| 09-16 | Phát hiện trần ~1.240/s (R1) | Hai đòn bẩy: shard lease, hoặc gộp feed latest-state. Đều bị hoãn vì là *"a phase of its own"* |
| 09-17, 09-23 | Hoãn lần 2 và 3 | *"sharding the gateway lease… is new architecture and outside the three-phase scope"* |
| 09-23 | Owner chọn phương án A (tối ưu bên trong writer đơn) | Tôi làm theo; kết quả là thêm vá víu, không nâng được trần |

### 3.2 Năm nguyên nhân gốc

1. **Chứng minh ngữ nghĩa trước, hạ tầng sau.** Chọn SQLite ở 08-13 là đúng cho mục tiêu
   2 symbol. Sai ở chỗ **cầu tạm không bao giờ bị gỡ**: Sunset Procedure của ADR-0006
   không có dấu vết thực hiện, và sau 08-13 **không có ADR nào** cho stable edge, lease hay
   việc spool thành "cache".
2. **Kafka được lắp *phía dưới* gateway cũ, không phải gateway mới được xây *trên* Kafka.**
   Phase B cần giữ gateway đã chứng nhận và hợp đồng consumer, nên chèn projector làm cầu.
   Từ đó Kafka chỉ còn là **bộ đệm thượng nguồn**: consumer không bao giờ đọc Kafka
   (Plan:38891-38900 ghi đúng như vậy).
3. **Lựa chọn "theo partition" bị loại vì một lý do đã hết hiệu lực.** Nó cần offset gốc
   của broker, mà ngày 08-14 chưa có. Một ngày sau đã có Kafka, nhưng không ai xem lại.
   Invariant 37 (Plan:150-153) còn ghi rõ lối đi đúng: *"broker-native cursor/barrier or
   one active fenced gateway **per partition**"*. Chỉ có 1 shard từng được triển khai.
4. **Mọi feed bị đối xử như nhau.** Thiết kế chưa bao giờ hỏi "loại dữ liệu nào cần đảm bảo
   gì". Tick 1.000+/s chỉ cần latest + live + replay ngắn khi reconnect. BAR vài event/s
   mới cần lịch sử 10k dòng. Cả hai bị ép qua cùng một kho bền có fsync.
5. **Chứng nhận đóng băng invariant.** Mỗi phase ghi rõ *"No change to the active/passive
   lease or the single-writer invariant"* (Plan:38743). Đo ở mức thành phần (SQLite đạt
   40k/s) che mất cái phễu ở mức hệ thống (một process Python). Phiên làm việc hôm nay của
   tôi cũng rơi vào đúng khuôn này (xem §6).

### 3.3 So với thiết kế đích ban đầu (Guide)

| Guide nói | Thực tế |
|---|---|
| Rust sở hữu *"durable publication, projector và replay"* (Guide:33) | Projector và replay là Python |
| Log bền append-only đặt trước Redis; Redis chỉ là cache (Guide:39) | Có thêm một log bền thứ hai (SQLite) giữa Kafka và consumer |
| Kafka là *"partitioned durable replay log… consumer groups"* (Guide:461); *"consumer group có thể scale"* (Guide:1002-1006) | Consumer không đọc Kafka; 6 partition dồn về 1 writer |
| Target flow: Canonical Log → Redis latest / gRPC gateway / Historical materializer (Guide:50-61) | Không có tầng SQLite nào trong target flow |
| PostgreSQL cho lease/fencing (Guide:40) | Lease nằm trong Redis tạm (tmpfs, không AOF) |
| Canonical retention 7–30 ngày, raw 24–72h (Guide:1016-1026) | Canonical 6h, raw 8h; spool giữ lâu hơn Kafka. "Cache" đang là bản duy nhất của lịch sử BAR |

---

## 4. Ưu điểm khách quan của hệ thống hiện tại (phải giữ)

1. **Hợp đồng consumer tốt và độc lập backend.** Proto `query.proto` không lộ offset
   Kafka; cursor là token HMAC mờ (`qdl/replay/handoff.py:86-216`); SDK có giao thức phục
   hồi rõ ràng: `RECONNECTED`, `SNAPSHOT_REPLACED`, retry theo lỗi
   (`qdl_sdk/client.py:317-364`), và đã hỗ trợ **nhiều target gRPC**
   (`qdl_sdk/transport.py:464-502`). **Đây là tài sản lớn nhất**: vì hợp đồng này, thay
   lõi không kéo theo sửa TS và alpha.
2. **Tầng thượng nguồn đúng chuẩn.** Ingestor Rust idempotent; rust_core dùng transaction
   consume-transform-produce (`rust/qdl-kafka/src/lib.rs:826-898`), `read_committed`,
   RF3, min ISR 2, không cho bầu leader bẩn.
3. **Thứ tự theo slice và phát hiện gap có ở nhiều tầng**: `partition_sequence` do
   rust_core cấp một cách tất định từ offset raw (`rust/qdl-realtime-core/src/lib.rs:593-608`);
   sequence của sàn cho book (`sequence_verified`, `book_generation`).
4. **Manifest, quota, entitlement, catalog** là một mô hình khai báo tốt, dùng lại được.
5. **Tầng đọc đã đạt mục tiêu latency** ở stage 20 sau các bản sửa hôm nay (QUOTE p99
   105–111 ms, MARK_INDEX p99 148–156 ms, 0 lỗi).
6. **Kỷ luật vận hành**: build theo digest, script roll có kiểm hash và rollback, boot
   recovery, sổ chứng nhận, driver kiểm chứng tải 50 alpha với budget đóng băng. Driver
   này độc lập backend và **dùng lại nguyên được** để nghiệm thu kiến trúc mới.
7. **Tính đúng khi chuyển từ replay sang live**: barrier theo partition trong gateway
   (`gateway.py:273-349`) đảm bảo subscriber không mất event. Kiến trúc mới phải giữ tính
   chất này, dù bằng cơ chế khác.

---

## 5. Nhược điểm (phần quan trọng hơn)

### 5.1 Phễu writer đơn triệt tiêu song song của Kafka
- Chỉ một process giữ lease được ghi (`stable_ingest.py:164-167` trả 409 cho process còn lại).
  Mọi append đều đi qua một `threading.RLock` (`sqlite_spool.py:163`), trong một process
  Python bị GIL giới hạn ở một lõi.
- Trần ~1.200/s, trong khi mục tiêu là 2.000–3.000/s và raw đã từng lên 2.749/s. **Thêm
  CPU không giải quyết được**, vì vấn đề nằm ở số process có quyền ghi, không ở số lõi.
- Hệ quả hôm nay: backlog 1,5 triệu, TS còn 35/60.

### 5.2 Ghi bền 99,7% dữ liệu không ai đọc lại làm lịch sử
- Tick chiếm 99,7% số event trong spool (A6). Mọi manifest đặt `warmup_limit: 0` cho tick
  (A7). Consumer tick chỉ cần giá trị mới nhất, luồng live, và replay ngắn khi reconnect
  (TTL cursor là 1h, `stable.py:387`).
- Chính dữ liệu này tạo ra 2.273–4.933 IO/s (Plan:52433-52437), chiếm khoá và làm GIL
  tranh chấp với Query.

### 5.3 Ba kho dữ liệu phải giữ khớp nhau
- Kafka (authority) + SQLite ("cache", nhưng giữ BAR lâu hơn Kafka) + Redis (latest, lease,
  cache identity, quota).
- Chúng bị buộc vào nhau bằng `cache_id`: Redis mất thì projector dừng
  (`projection/stable.py:190-242`). Đó là lý do có runbook rebuild, unit boot-recovery, và
  lời dặn "không bao giờ recreate `stable_redis`".
- Retention bị đảo ngược: Kafka canonical 6h, còn spool giữ tới 12.064 dòng mỗi key (BAR
  1m ≈ 8 ngày). "Cache" thực tế là bản duy nhất của lịch sử BAR ngoài 6h.

### 5.4 Làm lại cùng một việc ở mỗi chặng, cho từng event
- Protobuf decode khoảng 5 lần (projector 4 lần, stream 1 lần). Sha256 raw tính lại 3 lần.
  JSON + base64 + HMAC chỉ để đi qua một chặng HTTP nội bộ.
- Projector đọc SQLite 2 lần mỗi batch (dedup trước khi gửi, xác minh sau ACK), cả hai lần
  **dưới khoá writer** (`sqlite_spool.py:1102-1132`).
- Mỗi batch gửi 2 POST (409 + 200) vì danh sách URL chỉ dùng để failover
  (`stable_ingest.py:504-529`).
- Fan-out quét tuyến tính mọi subscription cho mọi event (`gateway.py:400-407`).
- Topic canonical nhúng toàn bộ raw, nên nặng hơn cả topic raw (§2.2).

### 5.5 Đọc và ghi chung một file
- 11+ process mở cùng `canonical-cache.sqlite3` (compose, volume `stable_state`). Warmup
  lớn giữ khoá hoặc snapshot lâu. Query phải có lane, duty cycle cho việc lạnh, kết nối
  "hot" riêng, chỉnh GC và switch interval. **Toàn bộ các bản vá hôm nay là triệu chứng
  của việc dùng chung file này.**

### 5.6 Lease: failover là bàn giao cả đường ống
- Chỉ một shard (`stable-stream-v2`). Khi lease đổi chủ: mọi subscription bị đóng, mọi
  consumer reconnect, projector dò URL. TS mất 1–2,5 phút mỗi lần.
- Lease nằm trong Redis tmpfs không AOF, và healthcheck `/health/dependencies` luôn trả
  200 kể cả ở process bị fence.

### 5.7 Độ phức tạp so với giá trị mang lại
- 8.159 dòng cho đường lõi projector/stream; 76 commit trên 3 file lõi; 4 loại dedup
  (exact, semantic, recovery-overlap, duplicate-resolution).
- Để so sánh: V1 dùng 12.310 dòng cho **toàn bộ** hệ, chỉ WS → Redis SET + PUBLISH
  (`app/cache/redis_cache.py:183-208`). V1 không bền, nhưng cho thấy lõi realtime vốn có
  thể mỏng.

### 5.8 Tài nguyên
- V2 dùng ≈7,8 vCPU, máy bận 87% (A3). Tổng trần CPU các role (≈19,3) vượt số lõi thật (16).
- Phần lớn CPU đi vào việc lặp (§5.4) và vào Kafka phải chở raw hai lần.

### 5.9 Test bằng fake che lỗi thật (quy trình)
- Commit `83fa1bc` gọi `gateway.subscriber_count()` (`stable_ingest.py:118`), trong khi
  `subscriber_count` là `@property` (`qdl/stream/gateway.py:526-528`). Cứ ~10 s sẽ có một
  POST đã ghi bền nhưng trả 500.
- Test dùng fake có `staticmethod` nên không bắt được lỗi
  (`tests/test_stable_ingest_spans.py:15-18`).
- **Không ảnh hưởng runtime hiện tại**: Stream đang chạy `ae2d62a`. Nhưng không được đưa
  `83fa1bc` lên Stream khi chưa sửa.

---

## 6. Sai lầm phương pháp của tôi trong phiên này

- **Vá triệu chứng thay vì đặt mục tiêu công suất.** Tôi tối ưu từng điểm nghẽn (lane,
  khoá, GC, duty cycle, MARK) trong khi thông số quyết định là trần writer đơn. Con số này
  đã được ghi từ 09-16.
- **Áp tinh chỉnh chỉ dành cho Query sang Stream mà không đo trước** (ngưỡng GC trong
  `0070d74`). Hậu quả là Stream bị OOM hai lần, phải revert, và backlog 1,5 triệu.
- **Bản sửa MARK đầu tiên đẩy thêm tải vào Stream**, đúng process đang là nút cổ chai.
- **Test span bằng fake** nên lọt lỗi ở §5.9.

Nguyên nhân chung: tôi coi kiến trúc là cố định và chỉ tối ưu bên trong nó, thay vì hỏi
kiến trúc có phù hợp với mục tiêu không.

---

## 7. Nguyên tắc cho kiến trúc mới

1. **Kafka là nguồn sự thật duy nhất cho dữ liệu realtime.** Không có log bền thứ hai cho
   cùng dữ liệu đó.
2. **Mỗi loại dữ liệu nhận đúng đảm bảo nó cần:**

   | Loại | Cần gì | Nơi phục vụ |
   |---|---|---|
   | Tick lossless (TRADE, BOOK_DELTA) | live có thứ tự + replay ngắn khi reconnect | Kafka (seek theo offset) + bộ đệm RAM |
   | Tick latest-state (QUOTE, BOOK_SNAPSHOT, MARK, FUNDING…) | giá trị mới nhất + live | Redis latest / RAM |
   | BAR | lịch sử tới 10k dòng + live + handoff cursor | kho BAR riêng (tốc độ ghi thấp) |
3. **Song song theo partition từ đầu đến cuối**, không có process nào là phễu chung.
4. **Kiểm tra một lần, ở rust_core.** Các chặng sau tin bản ghi Kafka đã commit
   (`read_committed`). Truy vết lineage bằng `raw_capture_id` / hash, không chở raw trong
   canonical.
5. **Cache là cache thật:** mất thì tự dựng lại từ Kafka, không dừng hệ thống.
6. **Không lease cho đường đọc.** Replica độc lập, consumer failover bằng danh sách
   target mà SDK đã hỗ trợ.
7. **Tái sử dụng tối đa:** hợp đồng, SDK, catalog, manifest, codec cursor, route HTTP,
   rust_core, Kafka, driver nghiệm thu. Không thêm loại service mới nếu không có số đo
   chứng minh.

---

## 8. Các phương án

### A. Giữ writer đơn, tối ưu bên trong (hướng đang được duyệt)
- Bỏ POST 409, bỏ kiểm tra lặp ở Stream, lập chỉ mục fan-out, dùng batch `executemany`.
- **[ước lượng]** trần có thể lên 2–3 lần, nhưng **vẫn là một process, một lease**. Vẫn
  giữ 99,7% ghi thừa, giữ ba kho, giữ failover kiểu bàn giao cả đường ống.
- Phù hợp nhất như một biện pháp tạm, không phải lời giải.

### B. Shard writer theo partition Kafka (6 file SQLite)
- Mỗi projector ghi thẳng file của partition mình; Stream đọc theo cách tail file.
- Trần ×6, nhưng vẫn ghi bền tick không ai cần, vẫn ba kho, live phải poll file (thêm
  latency), lease thành 6 lease. **Độ phức tạp tăng.**

### C. Kafka-native (đề xuất)
- Stream đọc Kafka trực tiếp, không lease; Redis chỉ giữ latest; chỉ BAR có kho lịch sử.
- Bỏ HTTP hop, writer đơn và tick spool. Chi tiết ở §9.

### D. Đích đầy đủ của Guide (gateway Rust + Iceberg/Parquet + Postgres)
- Đúng về lâu dài, nhưng tốn thời gian và phải thêm hạ tầng (object storage, Postgres).
  Không phù hợp với yêu cầu "không tốn thêm nhiều thời gian". C là bước đi đúng hướng tới D
  mà không khoá đường lùi.

### So sánh

| Tiêu chí | A | B | C | D |
|---|---|---|---|---|
| Trần thông lượng | ~2–3× hiện tại **[ước lượng]**, vẫn 1 lõi | ~6× | Kafka + số replica; không còn phễu | Cao nhất |
| Ghi thừa tick | Giữ | Giữ | **Bỏ** | Bỏ |
| Số kho phải khớp | 3 | 3 (+6 file) | Kafka + Redis latest (cache thuần) + kho BAR nhỏ | Kafka + Redis + lakehouse |
| Failover | Lease 15 s + reconnect | 6 lease | Reconnect sang replica khác (SDK có sẵn) | Tương tự C |
| Tác động lên consumer | Không | Không | Một lần `SNAPSHOT_REPLACED` mỗi slice lúc cutover | Như C |
| Service mới | 0 | 0 | 0–1 (bar materializer, có thể gộp vào projector) | Nhiều |
| CPU | Như cũ | Tăng | **Giảm** **[ước lượng]** | Tăng hạ tầng |
| Thời gian | Vài ngày, nhưng không đạt mục tiêu | 1–2 tuần | **2–3 tuần [ước lượng]** | Hàng tháng |
| Rủi ro | Thấp, nhưng không giải quyết gốc | Trung bình | Trung bình, kiểm soát được bằng shadow | Cao |

---

## 9. Kiến trúc đề xuất chi tiết (phương án C)

### 9.1 Sơ đồ

```text
Sàn ─► ingestor (Rust, giữ nguyên) ─► Kafka raw
     ─► rust_core (giữ nguyên, bỏ nhúng raw vào header canonical)
     ─► Kafka md.canonical.v2  ◄═══ NGUỒN SỰ THẬT DUY NHẤT (RF3, read_committed)
          │
          ├─► Stream replica ×2 (không lease; mỗi replica đọc cả 6 partition)
          │     - RAM: latest theo key + ring buffer ngắn cho tick lossless
          │     - gRPC Subscribe: replay (ring → nếu thiếu thì seek Kafka) → live
          │     - cursor = vị trí Kafka của bản ghi (mờ, ký HMAC như cũ)
          │
          ├─► Latest projector (projector hiện có, rút gọn)
          │     - Redis: latest theo key + offset Kafka (không cache identity, không lease)
          │     - commit offset sau khi ghi Redis; mất Redis → tua lại vài phút là đủ
          │
          ├─► BAR materializer (lọc key BAR, ~0,3% lưu lượng)
          │     - kho BAR (SQLite, 1 writer ghi vài event/s) + offset Kafka của từng bar
          │
          └─► Query ×2 (route HTTP giữ nguyên)
                - snapshot / latest / MARK_INDEX: đọc Redis latest
                - BAR warmup / history: đọc kho BAR (read-only)
                - tick warmup: không có consumer nào dùng (warmup_limit 0)
```

### 9.2 Cursor và tính đúng: giữ hợp đồng, đổi nguồn offset
- **Offset cho consumer = offset Kafka của bản ghi.** Mỗi `partition_key` nằm trên đúng
  một partition Kafka (key hash), nên offset tăng nghiêm ngặt theo slice. Điều này thoả
  kiểm tra của SDK (`client.py:355-364`, cho phép khoảng trống vì bản ghi bị lọc).
- **Kiểm tra gap "offset liên tục" ở server** (`handoff.py:325-331`) được thay bằng
  **đọc partition liên tục**: replica đọc tuần tự từ offset X, nên không thể bỏ sót bản ghi
  của key. Gap ở nguồn vẫn được phát hiện bằng `partition_sequence` và sequence của sàn.
- **Mọi replica ra cùng một offset cho cùng một bản ghi**, nên Query cấp cursor và Stream
  nào cũng dùng được. Đây chính là thứ file dùng chung đang được dùng để đảm bảo.
- **`generation_id`** chuyển từ `spool.cache_id` sang danh tính ổn định: TopicId + số
  partition + `partition_plan_epoch`.
- **Replay sang live không mất event:** trong một replica, một luồng tiêu thụ duy nhất theo
  thứ tự. Subscriber đăng ký tại offset X, được đẩy phần ring từ X, rồi nhận tiếp các bản
  ghi mới. Không cần barrier giữa các process.
- **Cutover:** offset đổi hệ đếm, nên mỗi slice nhận một lần `CURSOR_EXPIRED` →
  `SNAPSHOT_REPLACED`. TS và alpha đã xử lý trường hợp này
  (`data_layer_v2.py:881,982-1004`).
- **Ràng buộc mới:** không đổi số partition khi đang chạy, vì key sẽ bị ánh xạ lại. Muốn
  đổi (ví dụ để sửa lệch p3/p5) thì làm cùng đợt cutover, khi cursor đằng nào cũng reset.

### 9.3 Cần xác minh trước khi cam kết (Phase 1 phải đo)

| Câu hỏi | Vì sao quan trọng | Cách đo |
|---|---|---|
| Một replica Python đọc 6 partition + decode + fan-out hết bao nhiêu CPU ở 3.000/s? | Quyết định dùng Python hay chuyển vòng tiêu thụ sang Rust | Replay topic thật bằng image có sẵn, container `--rm` |
| Ring buffer bao nhiêu phút cho TRADE/BOOK_DELTA, tốn bao nhiêu RAM? | Reconnect trong ring thì nhanh, ngoài ring thì phải seek Kafka | Đo kích thước bản ghi thật × tốc độ theo feed |
| Seek Kafka để replay dài (tối đa 10k bản ghi của một key) mất bao lâu? | Một key chỉ chiếm 1/30 partition nên phải quét nhiều bản ghi | Thử offset thật |
| Bỏ header raw khỏi canonical tiết kiệm bao nhiêu CPU/đĩa cho Kafka? | Dự kiến giảm mạnh, nhưng **[chưa kiểm]** | So bytes/s trước và sau trên stack shadow |
| bar_edge hiện ghi BAR vào đâu, và có phụ thuộc spool không? | Ảnh hưởng thiết kế kho BAR | **[chưa kiểm]**: đọc `qdl/runtime/stable_bar_edge.py` |
| Còn ai đọc key/pubsub tương thích V1 trong `stable_redis` không? | Nếu không thì bỏ được | **[chưa kiểm hết]**: chưa tìm thấy subscriber nào |

### 9.4 Giữ lại, bỏ đi, thêm mới

| Giữ nguyên | Rút gọn / sửa | Bỏ | Thêm |
|---|---|---|---|
| ingestor, rust_core, Kafka ×3, proto, SDK, manifest, catalog, codec cursor HMAC, route HTTP Query, driver nghiệm thu + budget | projector (chỉ ghi Redis latest), gateway gRPC (đọc Kafka thay vì spool), Query backend (Redis + kho BAR), rust_core (bỏ header raw) | HTTP ingest, lease Stream, tick spool, cache identity, 4 lớp dedup, POST 409 | BAR materializer (có thể là một chế độ của projector, không phải service mới) |

### 9.5 Ngân sách tài nguyên mục tiêu **[ước lượng, sẽ đo ở Phase 1]**
- Mục tiêu tổng V2 ≤ 5 vCPU ở 3.000 event/s (hiện ≈7,8 vCPU ở ~835–1.200/s).
- Tiết kiệm từ: bỏ 2 container Stream × 2 CPU (thay bằng 2 replica nhẹ); projector không
  còn decode/sha/SQLite/HTTP; Kafka không chở raw hai lần; SQLite chỉ nhận 0,3% lưu lượng.

---

## 10. Lộ trình 4 phase (đề xuất để thảo luận)

Nguyên tắc chung cho cả 4 phase:
- Stack hiện tại **tiếp tục chạy** cho tới hết Phase 3.
- Mọi thứ mới chạy **shadow** trên cùng Kafka, với consumer group riêng và port riêng.
- Rollback luôn là trỏ consumer về stack cũ.

### Phase 1 — Luồng realtime Kafka-native (shadow)
- **Bước đầu, bắt buộc: spike đo** các câu hỏi ở §9.3. Nếu Python không đạt mục tiêu CPU
  thì chuyển vòng tiêu thụ sang Rust trước khi làm tiếp.
- Stream replica đọc Kafka trực tiếp, không lease, RAM latest + ring, gRPC Subscribe với
  cursor theo offset Kafka.
- **Cổng thoát:**
  - Parity từng event với Stream cũ trên cùng slice (không thiếu, đúng thứ tự).
  - Chịu 3.000/s tải tổng hợp, không lag.
  - Latency live nằm trong budget.
  - Kill một replica: consumer reconnect sang replica kia mà không mất event.

### Phase 2 — Latest + BAR store + Query (shadow)
- Projector rút gọn chỉ ghi Redis latest. BAR materializer ghi kho BAR. Query mới đọc
  Redis và kho BAR. rust_core bỏ header raw (chỉ ở shadow, bằng một cờ cấu hình).
- **Cổng thoát:**
  - Parity snapshot/warmup/latest_bar/MARK với Query cũ.
  - Handoff BAR warmup → stream khớp watermark (`verify_bar_handoff` của alpha).
  - Mất Redis thì tự dựng lại trong vài phút, không dừng.

### Phase 3 — Cutover và nghiệm thu
- Chuyển TS trước (V1 fallback vẫn giữ), rồi alpha.
- Chạy driver đích stage 20 → 35 → 50 với budget đã đóng băng.
- **Cổng thoát:**
  - Stage 50 + TS 60/60 PASS.
  - Chịu burst thật (thị trường sôi động) không backlog.
  - Rollback đã diễn tập một lần.

### Phase 4 — Gỡ bỏ và phát hành
- Dừng Stream writer, lease, tick spool, HTTP ingest. Dọn image/volume theo digest, chỉnh
  lại trần CPU theo số đo. Cập nhật ledger, viết ADR mới (thay ADR-0006), phát hành
  **v2.2.0**.
- **Cổng thoát:** V2 ≤ ngân sách CPU đã chốt; không còn service thừa; tài liệu vận hành
  (boot recovery, rebuild) rút gọn tương ứng.

**Thời gian [ước lượng, chưa có số đo]:** P1 3–5 ngày · P2 3–5 ngày · P3 2–3 ngày ·
P4 1–2 ngày. Spike đầu Phase 1 là điểm dừng: nếu số đo xấu, ta biết ngay trong 1–2 ngày
thay vì sau 2 tuần.

---

## 11. Rủi ro và câu hỏi mở (để thảo luận với Astra)

1. **Python hay Rust cho Stream replica?** Guide muốn Rust. Python dùng lại được nhiều code
   (`grpc_service.py`, `handoff.py`). Đề xuất: quyết theo số đo của spike.
2. **Mỗi replica đọc cả 6 partition, hay chia partition giữa các replica?** Đọc tất cả thì
   đơn giản, và replica nào cũng phục vụ được mọi subscriber. Chia partition tiết kiệm CPU
   nhưng phải định tuyến subscriber. Đề xuất: đọc tất cả, trừ khi đo thấy quá tốn.
3. **Kho BAR là SQLite hay Kafka topic riêng có retention dài?**
   - (a) SQLite ghi bởi materializer: đơn giản, dùng lại code `read_final_bar_window`.
   - (b) rust_core ghi BAR thêm vào `md.bars.v2` với retention 14–30 ngày: Kafka giữ luôn
     lịch sử BAR, SQLite thành cache dựng lại được thật sự.
   - Đề xuất: (a) trước, (b) nếu cần HA cho lịch sử.
4. **Có sửa lệch partition (p3/p5 chiếm 57%) không?** Chỉ làm ở lúc cutover nếu cần.
5. **Retention canonical:** 6h đủ cho replay tick (TTL cursor 1h). Khi bỏ header raw, có
   thể tăng retention mà không tốn thêm đĩa.
6. **Key/pubsub tương thích V1 trong `stable_redis`:** giữ hay bỏ? (§9.3)
7. **Lease cho Redis latest:** projector theo partition thì mỗi key chỉ có một người ghi,
   nên không cần lease. Cần xác nhận không có người ghi thứ hai (bar_edge?).
8. **Rủi ro chính:** thay cơ chế cursor. Giảm rủi ro bằng parity shadow từng event và cho
   consumer cũ chạy song song đến hết Phase 3.

---

## 12. Việc trước mắt (không thuộc 4 phase, cần owner quyết)

1. **Sự cố backlog:** đang tự giảm ~343/s lúc thị trường yên. **[ước lượng]** khoảng
   45 phút nếu không có burst. Có thể để tự hết, hoặc duyệt phương án B cũ (dời offset).
2. **Không đưa `83fa1bc` lên Stream** khi chưa sửa §5.9.
3. **Ghi journal** các sự kiện chưa ghi: Stream OOM (12:29Z, 13:47Z), roll `83fa1bc` lên
   Stream thất bại và revert về `37d7f518`, roll Query `94f2db4`/`83fa1bc`, sự cố backlog.
4. **Dừng phương án A** (tối ưu bên trong writer đơn) nếu owner chọn C, để không tiếp tục
   đầu tư vào đường sẽ bị gỡ.

---

## Phụ lục A — Lệnh đã chạy để ra số liệu (2026-09-23, chỉ đọc)

| Mã | Lệnh / nguồn | Kết quả chính |
|---|---|---|
| A1 | `kafka-consumer-groups.sh --describe --group stable-projector-v1`, chạy 2 lần cách nhau 64 s, trong container `--rm` dùng image kafka1 và `admin.properties` | produced 53.431, consumed 75.418; lag p3 = 642.232 → 626.222, p5 = 317.506 → 305.777 (giảm; bản đầu ghi ngược chiều, đính chính ở §14.2) |
| A2 | `docker logs --since 60s projector_v2*`, đếm `200 OK` / `409 Conflict` | 85/85, 87/86, 74/77, 52/53, 67/68, 42/42 |
| A3 | `top -bn2`; tổng `docker stats` các container `qdl_v2*` | 67,4 us / 13,0 sy / 12,9 id; V2 ≈ 777% |
| A4 | `du` thư mục topic trên volume `kafka1_data` | canonical 7.062 MB (retention 6h), raw 7.940 MB (retention 8h) |
| A5 | `find` file > 10 MB trong volume `stable_state` | `canonical-cache.sqlite3` 3.301.470.208 B + WAL 67.108.864 B |
| A6 | Python `sqlite3` mode=ro, `query_only`, bảng `partitions` (`next_offset-1` gom theo feed), container `--rm --network none` | trade 147,2 triệu · book 84,3 triệu · quote 79,3 triệu · mark 26,4 triệu · bar 1,07 triệu; 188 key |
| A7 | Python `yaml`, duyệt `consumers/stable/*.yaml`, đếm `feed` × (`warmup_limit>0`) | chỉ BAR có warmup > 0 ở cả 6 manifest |
| A8 | `kafka-topics --describe` / `kafka-configs --describe --all` cho `md.canonical.v2` | 6 partition, RF3, min ISR 2, retention.ms 21.600.000, segment 16 MiB |
| A9 | `docker inspect` NanoCpus/Memory của từng role | bảng §2.3 |
| A10 | Như A1, chạy lại lúc 15:58:30Z | lag p0 212, p1 168, p2 251, p3 35.292, p4 955, p5 871 |

## Phụ lục B — Chỉ mục bằng chứng trong code và tài liệu

- ADR-0006: `docs/adr/0006-phase2-bounded-durable-bridge.md:10,25,42,47`
- Guide target: `upgrade/quant-data-layer-fund-grade-upgrade-architecture.md:33-61, 461, 1002-1006, 1016-1026`
- Lựa chọn active/passive và theo partition: Plan:1263-1274; invariant 37 Plan:150-153
- Phase B: Plan:3981-4039, 4298-4306, 5311-5316
- Trần writer đơn: Plan:38913-38973; hoãn: Plan:39065-39067, 52433-52437; phương án A: Plan:53033-53035
- Đọc Kafka chưa từng xảy ra ở consumer: Plan:38891-38900
- Lặp công việc: `stable_projector.py:341,863,953-966`; `stable_ingest.py:183-205,504-555`; `projection/stable.py:306-313`
- Offset / cursor: `sqlite_spool.py:607-631`; `handoff.py:86-216,295-332`; `stable_source.py:1508-1602`
- `partition_sequence` tất định: `rust/qdl-realtime-core/src/lib.rs:593-608`
- Header raw trong canonical: `rust/qdl-kafka/src/lib.rs:836-839`, `qdl-realtime-core.rs:412`
- Lỗi span: `qdl/runtime/stable_ingest.py:118`, `qdl/stream/gateway.py:526-528`, `tests/test_stable_ingest_spans.py:15-18`

---

<a id="astra-independent-addendum"></a>
## 13. Astra - Đánh giá độc lập và đề xuất triển khai bổ sung

> **Ngày:** 2026-09-23. **Trạng thái:** đề xuất để owner duyệt, chưa phải lệnh triển khai.
> **Phạm vi lượt này:** chỉ bổ sung tài liệu bên dưới bản Opus; giữ nguyên toàn bộ phần trên.
> **Source đã đối chiếu:** canonical `/home/bobby/data_layer`, branch
> `feat/consumer-endpoint-benchmark`, HEAD `83fa1bc56411e0dfb1f4d91b456e48ddd9b080b4`.
> **Evidence:** đọc source, ADR, guide và journal; không benchmark lại, không gọi provider,
> không kiểm kê runtime mới, không build/rollout hay thay dữ liệu trong lượt đánh giá này.
> Các số throughput/CPU/latency kế thừa phần Opus là số đo lịch sử tại cửa sổ ghi ở đó,
> không phải bảo đảm về trạng thái runtime hiện tại hay kết quả của kiến trúc đề xuất.

### 13.1 Kết luận và nguồn sự thật

**Đề xuất chọn phương án C có điều chỉnh: Kafka-native distribution, Rust sở hữu hot
data plane, Python giữ API/SDK và orchestration. Không rewrite toàn bộ Data Layer.**
Vấn đề không phải đã chọn Kafka sai; vấn đề là phía sau Kafka vẫn giữ bridge tạm,
single-writer spool và cơ chế phục hồi dành cho một mô hình nhỏ hơn mục tiêu hiện tại.

Các tài liệu phải đọc cùng phần bổ sung này:

- [Guide kiến trúc gốc](quant-data-layer-fund-grade-upgrade-architecture.md), các mục
  0-7 về trách nhiệm Rust/Python; 8-19 về canonical, replay, history, handoff, API/SDK.
- [ADR-0006](../docs/adr/0006-phase2-bounded-durable-bridge.md): bounded durable bridge
  có phạm vi nhỏ và điều kiện sunset; không được suy ra quyền mở rộng làm tick/L2 log chính.
- [Unified Plan](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md): source/runtime status,
  quyết định owner, evidence đã pass/fail, workload 50 alpha + TS và journal thực hiện.
- Phần Opus ở trên: lịch sử quyết định và số đo; phần Astra ở dưới: đánh giá độc lập,
  điều chỉnh giả định và quy trình đề xuất. Hai phần không tự động thay thế một approved plan.

Owner đã yêu cầu **ghi lại đề xuất**, chưa phê duyệt triển khai bốn phase này trong lượt
này. Tất cả phase dưới đây là `PROPOSED / NOT STARTED`. Khi được duyệt, cập nhật status,
execution scope và journal ở Unified Plan, link về các anchor dưới đây; không tách thêm
các phase mang tên mới sau mỗi lỗi. Nếu chọn C, phải ghi rõ nó thay phần capacity closure
nào của kế hoạch cũ, tránh tiếp tục song song cả hướng A và C mà không có điểm dừng.

### 13.2 Những gì đồng ý, những gì đã kiểm chứng

| Nhận định | Đánh giá Astra | Hệ quả triển khai |
|---|---|---|
| Kafka song song bị gom về một writer | Đồng ý. `StreamGateway.publish_many` chờ durable sink trước fan-out; lease giới hạn một active writer | Thay đường phân phối, không tiếp tục lấy tăng projector làm giải pháp chính |
| SQLite bridge đã vượt phạm vi ban đầu | Đồng ý theo ADR-0006 và lịch sử Phase B | Sunset tick spool có kiểm soát, không xóa lịch sử để che vấn đề |
| Nhiều tầng lặp decode/hash/lookup | Có trong projector/ingest/projection; chi phí cụ thể cần profile | Bỏ việc lặp được chứng minh dư thừa, không xóa mọi kiểm tra an toàn |
| Cache recovery bị coupling | BAR edge kiểm `cache_identity` và checkpoint; Redis projection cũng kiểm cache identity | Phải chuyển cả BAR recovery và Query, không chỉ thay gRPC Stream |
| Fan-out chưa hiệu quả theo số subscriber | Source duyệt subscriptions cho từng stored record | Dùng index theo stream key/binding, shared immutable payload và queue hữu hạn |
| Cold warmup ảnh hưởng hot read | Journal có probe 5.000-row warmup làm QUOTE tail tăng; duty-cycle chỉ giảm triệu chứng | Cô lập CPU-heavy decode/serialization khỏi hot executor |
| Test fake có thể che lỗi thật | `stable_ingest.py` gọi `subscriber_count()` trong khi gateway thật dùng property | Sửa/test với collaborator thật trước khi dùng source này làm baseline Stream |

Source đối chiếu: [gateway](../qdl/stream/gateway.py),
[handoff](../qdl/replay/handoff.py), [ingest](../qdl/runtime/stable_ingest.py),
[projector](../qdl/runtime/stable_projector.py),
[Redis projection](../qdl/projection/stable.py),
[BAR edge](../qdl/runtime/stable_bar_edge.py),
[Rust Kafka](../rust/qdl-kafka/src/lib.rs).

**Không kết luận rằng toàn bộ code/gate trước đây vô ích.** Instrument identity, decimal,
venue normalization, finality, L2 sequence/checksum, auth và provider admission vẫn có giá
trị. Sai sót phương pháp là dùng các pass riêng lẻ như bằng chứng cho sức tải end-to-end,
rồi tối ưu queue/timeout/resource quá lâu mà chưa thay chỗ tuần tự hóa toàn hệ thống.
Parity giữa hai implementation cùng sai cũng không đủ; oracle phải là contract và dữ liệu
provider/canonical đã commit, không mặc định output của stack cũ là chân lý.

### 13.3 Các giả định cần sửa trước khi chốt hướng C

1. **Không warmup tick không có nghĩa không cần replay tick.** TRADE và BOOK_DELTA vẫn
   cần reconnect/replay trong retention; loại bỏ bản sao SQLite, không loại bỏ Kafka
   event history. QUOTE/MARK latest có thể coalesce ở projection nếu contract cho phép,
   nhưng không âm thầm biến một subscription lossless thành latest-only.
2. **Không thể bỏ toàn bộ dedup/fencing.** Canonical domain validation nên tập trung ở
   Rust; sink vẫn phải idempotent. Reader vẫn kiểm auth, schema, identity, generation,
   source quality và execution eligibility. Consumer chậm hoặc reconnect không được
   nhận dữ liệu sai account/venue/instrument vì upstream đã kiểm một lần.
3. **Redis replay vài phút không bảo đảm đủ state.** Feed ít thay đổi có latest cũ hơn
   cửa sổ replay; BAR dài có history ngoài Kafka retention. Phải xác định bootstrap,
   checkpoint, durable backup và bounded provider recovery theo từng product.
4. **Hai replica đọc tất cả partition không nhân đôi ingest capacity miễn phí.** Hai
   replica cùng decode cùng dữ liệu, nhưng chia subscriber và cho failover. Chọn cách này
   trước vì đơn giản; đo CPU/bytes/fan-out trước khi thêm routing theo partition.
5. **Kafka offset không liên tiếp không tự chứng minh có gap.** Transaction/control
   records và lọc theo key tạo khoảng nhảy hợp lệ. Kiểm coverage bằng vị trí đọc partition
   và kiểm market gap bằng source sequence; không dùng `offset == previous + 1` cho slice.
6. **EOS Kafka không bao phủ Redis/SQLite.** Kafka transaction bảo vệ input offset và
   output record trong Kafka. Sink ngoài Kafka vẫn cần atomic state/checkpoint hoặc
   compare-and-apply idempotent, kể cả crash sau sink write nhưng trước offset commit.
7. **Không hứa recovery backlog từ số tổng.** A1 ghi lag p3 và p5 tăng dù aggregate
   consumed lớn hơn produced; partition khác có thể đang drain. Không suy ra mọi slice
   tự hồi phục trong 45 phút. Theo dõi lag/age/throughput riêng từng partition.
8. **99,7% là phân bố counter tích lũy, không phải số byte hiện giữ.** Không được suy ra
   tiết kiệm 99,7% RAM/disk/CPU. Raw/canonical có retention, compression và event mix khác;
   bỏ raw header tiết kiệm bao nhiêu phải đo serialized bytes/s thực tế.
9. **Tổng CPU cap vượt số core không tự là bug.** Phải xem usage, throttle, pressure và
   latency đồng thời. RF3 trên cùng host cũng không chứng minh independent-host HA.
10. **`3.000 event/s` và `<=5 vCPU` là mục tiêu thử, chưa phải certificate.** Cần nói rõ
    event canonical hay raw, bytes/event, L2 depth, fan-out và workload consumer; không
    benchmark bản ghi nhỏ rồi suy ra tất cả book/event mix đều chịu được.

Nguồn semantics Kafka: [Consumer API](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
về group/offset/replay; [Kafka design](https://kafka.apache.org/41/design/design/) về
transaction và external sinks. Chúng không thay thế phép đo trên deployment này.

<a id="astra-target-architecture"></a>
### 13.4 Kiến trúc đích và ranh giới trách nhiệm

```text
Approved real providers / durably captured provider replay
    -> existing Rust/Python venue adapters
    -> Kafka raw
    -> existing Rust canonical core / L2 / quality / admission
    -> Kafka canonical, committed records only
         |
         +-> Rust Stream replicas
         |     bounded RAM latest/ring, indexed fan-out
         |     isolated bounded replay readers -> existing gRPC public contract
         |
         +-> Rust latest materialization -> Redis latest + typed quality
         |                                      -> Python Query / SDK
         |
         +-> BAR materialization -> BAR history + atomic checkpoints
                                                -> Python Query / SDK

Historical/reference provider wrappers remain a bounded cold path.
Alpha/TS never need Kafka credentials or a Kafka client.
```

- **Rust:** consume/decode, hot projection, ordering, replay, L2 và bounded fan-out.
  Tái sử dụng `qdl-core`, `qdl-kafka`, generated protobuf và native normalization đã có.
  Không đặt một Python HTTP bridge per event trở lại giữa Rust và consumer.
- **Python:** REST/SDK compatibility, entitlement/control configuration, historical
  orchestration, reference wrappers, provider SDK phù hợp và admin tooling.
  Endpoint gRPC có thể do Rust phục vụ trực tiếp nhưng phải giữ auth/contract hiện hành.
- **Stream replicas:** không leader lease cho read delivery. Mỗi replica có group riêng
  hoặc cơ chế assignment rõ ràng để đọc đủ partition; hai replica cùng group sẽ chia
  partition và không còn tự phục vụ được mọi slice như thiết kế này yêu cầu.
- **Materializers:** ownership theo partition; Redis và BAR là projection khác nhau,
  không cần distributed transaction giữa chúng. Mỗi output có watermark riêng; Query
  không ghép các output lệch watermark rồi giả vờ là một atomic global snapshot.
- **History:** bars-only SQLite là lựa chọn khởi đầu hợp lý vì write rate thấp. Không
  tạo PostgreSQL/Iceberg/lakehouse mới trong bốn phase. Nếu history đã vượt retention
  Kafka, phải coi nó là dữ liệu cần backup/recovery, không đặt nhãn cache để miễn bảo vệ.
- **Redis:** latest cache có thể dựng lại; không nguồn authority cho event đã commit.
  Chỉ hỗ trợ `READY` khi state + checkpoint + quality của slice đã coherent.
- **Venue/product:** core chung cho Binance/OKX và adapter VN. DNSE/Spot đang deferred
  giữ nguyên certification/routing hiện tại; không tuyên bố được nâng cert qua test crypto.

Không tạo container theo symbol/interval. Dùng lại service boundary và deployment naming
chuẩn; số materializer replica cuối cùng quyết theo ownership, sink concurrency và số đo,
không mặc định phải giữ sáu Python projector hoặc gộp tất cả thành một process mới.

### 13.5 Contract theo từng loại dữ liệu

| Product | Nguồn phục vụ đề xuất | Bất biến phải giữ | Không được suy diễn |
|---|---|---|---|
| TRADE | Committed Kafka -> Stream; latest projection khi gọi snapshot | Native identity, price/qty/units, source/session quality, replay trong retention | Không có tick mới không tự là disconnect; quiet không đồng nghĩa giá luôn eligible |
| QUOTE/BBO | Latest projection và Stream | Bid/ask/sizes, venue, generation; coalescing chỉ theo delivery class đã công bố | Không suy ra market liveness từ một field age đơn lẻ |
| MARK_INDEX_PRICE | Typed latest state và Stream | Giữ timestamp riêng của mark/index và session/component quality | Không thay timestamp gốc bằng receive time hoặc refresh time để pass SLA |
| BOOK_SNAPSHOT/DELTA | Verified Rust book view + committed deltas | Snapshot sequence tương ứng delta, checksum/gap, depth, resync generation | Không lấy snapshot tùy ý rồi nối delta của generation khác |
| Final BAR | BAR history + canonical stream | Closed-only, interval/calendar alignment, OHLCV/units, correction, continuous warmup | Không dùng ingest offset mới nhất để suy ra toàn bộ history đã đủ |
| Funding/OI/long-short/taker/basis/metadata | Wrapper hiện có; materialize khi có demand/contract phù hợp | Missing không thành 0, native/derived lineage, applicability và unit | Reference input không tự thành execution/risk authority |
| VN/legacy product | Route/adapter đã được owner công bố | Session calendar và capability thực | Không tăng phạm vi cert khi chưa test provider/market-hours |

Bootstrap BAR từ provider vẫn đi qua validation/canonical path đã được định nghĩa,
không ghi trực tiếp fake rows vào Query để làm xanh matrix. Existing history đã chứng
minh phải được migrate có hash/count/identity và checkpoint; không bắt owner chờ history
đầy lại chỉ vì thay backend. Wrapper cold path không được tạo WS/subscription mới cho
mỗi request hoặc mỗi alpha.

<a id="astra-cursor-recovery"></a>
### 13.6 Cursor, replay, snapshot và recovery đúng domain

#### Cursor và compatibility

- Giữ token opaque, ký HMAC và ràng buộc identity/product theo contract. Payload nội bộ
  cần phân biệt transport version, topic identity/generation, physical partition,
  Kafka offset, logical slice và plan epoch; không đổi public response shape tùy tiện.
- Broker offset và provider sequence là hai khái niệm riêng. `read_committed` không được
  expose aborted/uncommitted output; boundary replay phải dựa vào readable committed
  progress, không lấy log-end offset làm bằng chứng mọi record đã có thể đọc.
- Query trả dữ liệu cùng watermark thực sự áp dụng cho chính view đó. Batch nhiều slice
  giữ cursor riêng hoặc vector semantics đã khai báo; không hứa atomic toàn thị trường.
- Cutover token SQLite cũ phải trả typed `CURSOR_EXPIRED`/resnapshot theo SDK hiện có,
  không diễn giải cùng số offset sang Kafka. Failover giữa replica cùng generation
  không được reset cursor liên tục hoặc phụ thuộc RAM của replica trước.
- ACK là tiến độ xử lý của subscription/consumer scope đúng hợp đồng, không phải vị trí
  fetch của server. Hai alpha dùng chung identity không được làm checkpoint của nhau
  tiến lên rồi bỏ qua dữ liệu chưa đọc của một alpha.

#### Replay sang live

1. Đăng ký subscription và chốt boundary của live buffer một cách atomic trong replica.
2. Replay từ cursor tới boundary qua ring hoặc reader Kafka riêng có giới hạn; không
   `seek` consumer live ngược về lịch sử vì một client reconnect.
3. Buffer phần mới trong lúc replay, merge theo transport identity, sau đó chuyển live.
   Queue/ring đầy phải trả typed recovery/reset, không silent drop.
4. Cursor ngoài retention hoặc book thiếu snapshot gốc phải resnapshot rõ ràng. Không
   nhảy đến latest rồi báo đã replay đủ; scan được cap theo bytes/time/records, không
   chỉ đếm số record matching vì một key có thể rất thưa trong partition.

#### Redis và materializer

- Reuse Lua/atomic compare-and-apply hiện có: giá trị, typed quality, generation và
  applied offset được cập nhật nhất quán; chỉ commit Kafka sau khi sink xác nhận.
- Crash sau sink write/trước offset commit sẽ replay và phải idempotent. Rebalance có
  thể để worker cũ hoàn tất write muộn: ownership/fence hoặc generation+offset CAS phải
  chặn rollback state. Không giữ global reader lease nhưng không bỏ write correctness.
- Rebuild dùng namespace/generation riêng và publish readiness sau khi state/checkpoint
  đúng. Mất Redis có thể làm slice tạm unavailable; không hứa zero downtime nếu không
  có replica/storage recovery bảo đảm, cũng không dừng toàn bộ ingestion vì cache mất.
- Chọn recovery artifact nhỏ nhất đáp ứng RTO: durable checkpointed latest snapshot
  hoặc approved compacted-state projection nếu thực sự cần. Không bật compaction trên
  lossless TRADE/BOOK_DELTA topic. Existing state storage được ưu tiên, không thêm stack.
- Một provider refresh có giới hạn chỉ phục hồi latest được contract cho phép; nó không
  chứng minh đã replay đủ trade/delta bị mất. Cache rỗng không được tái sử dụng checkpoint
  cũ rồi đánh dấu recovered khi state chưa được dựng lại.

#### BAR history và L2

- BAR upsert và materializer offset/checkpoint nằm trong cùng transaction. Crash không
  được tạo offset đi trước rows hoặc rows nhận hai lần thành hai candle.
- Repair nến cũ phải giữ event/revision lineage; không làm `latest_final_bar` lùi về
  timestamp cũ. Checkpoint ingest cao không thay thế continuity check theo open-time.
- BAR edge chuyển sang interface history/checkpoint mới thay vì gắn vào tick spool
  `cache_id`; finality, bounded bootstrap và count-fenced repair hiện có được tái sử dụng.
- 5.000 nến 1m đã dài hơn 6 giờ; 5.000 nến 1h dài hơn nhiều nữa. Khai báo rõ maxlen,
  coverage và provider pagination, trả typed insufficient history nếu provider không có.
- Book rebuild phải có snapshot/checkpoint tương ứng delta sequence và generation.
  Retention còn delta không đồng nghĩa còn snapshot cần thiết. SDK nhận resync typed,
  không coi reconstructed partial book là execution-ready.

### 13.7 Tối ưu tài nguyên mà không chuyển lỗi sang chỗ khác

- Fan-out theo index binding -> subscribers, tránh `events x all_subscribers`; payload
  immutable dùng chung khi có thể, không stringify/base64/hash lại cho từng consumer.
- Ring/queue/replay concurrency bị giới hạn theo **bytes và số item**, có metrics đầy,
  slow-consumer policy và cancellation. L2 message lớn không được chỉ tính như một tick.
- Materializer latest được coalesce theo semantics product; lossless stream và book
  delta không được drop để cải thiện throughput. Session/quality update phải được giữ.
- Hot Query có bounded executor riêng; heavy warmup decode/serialize không giữ GIL
  hoặc chiếm toàn bộ slot của hot path. Ưu tiên native batch work hoặc bounded cold
  worker thích hợp, không dùng sleep/duty-cycle như lời giải dài hạn cho CPU contention.
- Không chuyển heavy warmup từ spool sang Redis rồi quét toàn cache trong request nóng.
  Warmup có pagination/limit/cancellation, lịch sử được index theo instrument/interval/time.
- Raw header chỉ được rút gọn sau khi kiểm tất cả downstream dependency. Lineage mới
  phải có capture reference/hash và transport coordinates cần thiết; raw hết retention
  phải được diễn đạt đúng, hash không có nghĩa payload vẫn tải lại được mãi mãi.
- Chỉ bỏ header bằng thay đổi backward-compatible đã test, không sửa business event ID
  hoặc làm replay cũ không đọc được. Đây là tối ưu trong Phase 2 nếu an toàn; không bắt
  dựng cả raw/canonical stack thứ hai chỉ để chạy A/B headers trên production.
- Bắt đầu với partition topology hiện có; đo skew p3/p5. Đổi partition làm đổi mapping
  key/cursor nên không coi là config tweak vô hại. Chỉ làm nếu số đo chứng minh cần,
  trong cutover đã version hóa; không tự thêm phase sharding hoặc routing mesh.
- Không double-run toàn stack trên host đang bận. Shadow dùng group riêng, caps, duration
  và stop condition theo ảnh hưởng lên runtime hiện tại; fault injection dùng scope test.

<a id="astra-four-phase-plan"></a>
### 13.8 Bốn phase đề xuất: goal, scope, test, exit và rollback

Các checklist dưới đây là work items bên trong đúng **bốn phase**, không phải phê duyệt
các phase con vô hạn. Hoàn thành scope thì dừng, ghi journal và chuyển phase đã được duyệt.
Bug do implementation mới phải sửa tại phase sở hữu; missing implementation không được
đổi tên thành technical debt. External limitation phải ghi rõ ảnh hưởng và owner decision.

<a id="astra-phase-1-stream"></a>
#### Phase 1 - Kafka-Native Stream And Replay Foundation

**Status:** `PROPOSED / NOT STARTED`.
**Goal:** bỏ phụ thuộc singleton durable writer khỏi đường realtime, giữ protocol và
proof replay; xác nhận hướng mới giảm bottleneck trước khi chuyển toàn bộ Query.
**Đọc:** §13.3-13.7; guide canonical/replay/SDK; Opus §9.2-9.3; ADR-0006.

**Phải làm:**

1. Freeze source/image/config baseline, source topic/partition identity và consumer
   manifest đang dùng. Record các fail có sẵn để không đổ lỗi nhầm cho bản mới.
2. Kiểm inventory source tái sử dụng. Rust Kafka và protobuf đã có; native gateway,
   auth interceptors, cancellation và replay scheduler không được giả định đã viết xong.
3. Prototype nhỏ dùng dữ liệu đã capture để đo native consume/decode/fan-out cost,
   event bytes và memory. Đây là một checkpoint trong phase, không một train riêng.
4. Thay Stream backend bằng Kafka committed read, bounded ring và replay workers;
   giữ port/protocol/schema/entitlement của public API theo migration config.
5. Hai replica đọc đủ partition độc lập; implement subscription-index, flow control,
   per-client cancellation và cursor thế hệ mới theo §13.6.
6. Shadow với group/port/identity test rõ ràng; không commit/reset group cũ, không đưa
   load-test event vào topic production. Không chỉ test BTC mà dùng captured/live scope
   Binance/OKX hiện có, gồm bar, trade, quote, mark và L2.

**Test bắt buộc:** unit/property/golden cho token/ordering; integration Kafka thật với
commit/abort/rebalance; reconnect ring-hit/ring-miss/expired cursor; duplicate và source
gap riêng; hai subscriber chung identity; kill một test replica; slow/abandoned client;
auth cross-venue/symbol denial; byte cap/queue cap; malformed payload fail-closed.

**Exit:** canonical oracle và output mới khớp về event identity/value/order theo delivery
class; zero unexplained loss/cross-mix; failover không cần writer lease handoff; đo được
capacity/cost ở baseline và target canonical event mix. Target 3.000/s là benchmark có
provenance, không là permission publish 3.000 synthetic events/s lên production.
Nếu chưa đạt, sửa bottleneck thuộc phase hoặc báo số đo và quyết định thiết kế trước
khi đi tiếp; không giấu bằng tăng freshness budget.

**Rollback/cleanup:** không chuyển consumer production trong phase; stop đúng shadow
replica/client, giữ offsets cũ. Dọn image/cache test không referenced; giữ artifact cần
Phase 2 và evidence compact. Không xóa source history/volume.
**Dừng scope:** chưa đổi Query backend, BAR authority, Kafka partition count hoặc TS.

<a id="astra-phase-2-materialization"></a>
#### Phase 2 - Latest, BAR History And Query Convergence

**Status:** `PROPOSED / REQUIRES PHASE 1 EXIT`.
**Goal:** Query nóng không chờ tick spool; history và rebuild đúng domain, không mất
warmup đã có. Chuẩn bị backend mới đầy đủ cho consumer mà không đổi alpha business logic.
**Đọc:** §13.4-13.7; guide warmup/history/quality; existing BAR edge, Redis projection,
Query adapters và provider reference wrappers.

**Phải làm:**

1. Materialize latest bằng native bounded path, atomic generation/offset/state; retain
   legacy Redis keys/pubsub chỉ nếu inventory xác nhận consumer còn dùng.
2. Tách BAR history khỏi tick log; migrate verified rows/checkpoint, validate uniqueness,
   correction và calendar semantics. Backup/restore có bounded rehearsal trước cutover.
3. Chuyển BAR edge checkpoint/recovery sang interface mới; không đổi thuật toán provider
   finality và không bổ sung artificial close delay để làm test dễ hơn.
4. Query snapshot/latest/mark đọc projection mới; BAR history đọc store mới. Book snapshot
   chỉ từ verified native view; warmup cursor là watermark thật, không watermark Kafka
   global mới nhất. Reference cold wrappers và public SDK shape được giữ nguyên.
5. Cô lập hot-read/cold-work scheduling; cancellation phải giải phóng work budget thực,
   không để request timeout nhưng background job tiếp tục làm nghẽn Query.
6. Chọn/implement recovery snapshot + replay cho latest và durable BAR recovery; readiness
   per slice, không global cache identity khiến mọi product cùng ngừng không cần thiết.
7. Rút raw header có kiểm chứng dependency nếu đáp ứng §13.7; nếu chưa thể bỏ an toàn,
   giữ nguyên và report chi phí, không che lineage loss bằng một hash không đủ ngữ cảnh.

**Test bắt buộc:** crash trước/sau sink commit; duplicate/rebalance/zombie write;
Redis empty/partial rebuild; interrupted restore; BAR old-open repair/correction;
warmup 2.500/5.000 và maxlen công bố; interval/native calendar alignment;
strict batch 1/8/16/32/50; two-replica snapshot/cursor parity tại cùng watermark;
quiet/disconnect/session generation cho từng feed; simultaneous cold warmup + hot reads.

**Exit:** full read-plane matrix qua hai replica pass; history đủ theo manifest, không
âm thầm giảm maxlen; cache restart recovery có RTO đo được; hot p95/p99 không bị cold
work làm vượt ngân sách đã freeze; invalid/stale/gap vẫn bị chặn đúng. Không yêu cầu
byte-equal latest tại hai thời điểm khác nhau nếu update đang tiếp tục; so đúng identity,
watermark/progress và bounded replica lag.

**Rollback/cleanup:** Query cũ và spool cũ giữ nguyên; shadow history/Redis namespace
riêng. Rollback backend/config về baseline đã ghi, không reset Kafka/flush cache chung.
Xóa đúng test namespace được cho phép, không xóa bản history duy nhất.
**Dừng scope:** chưa promote toàn bộ consumer, chưa gỡ stack cũ, chưa release.

<a id="astra-phase-3-acceptance"></a>
#### Phase 3 - Real Consumer Load, Handoff And Recovery Acceptance

**Status:** `PROPOSED / REQUIRES PHASE 2 EXIT`.
**Goal:** chứng minh sức tải workload mục tiêu, latency consumer thực và phục hồi trước
khi chuyển authority rộng. Không dùng health-up hoặc số container làm chứng nhận tải.
**Đọc:** §13.9-13.11; workload/budget trong Unified Plan; SDK/TS adapter và manifest thực.

**Phải làm:**

1. Freeze workload, per-identity quotas, route set, source/image/config và budget trước
   run. Driver phải phân biệt offered, admitted, completed, rejected, timed-out,
   in-flight và missed; không chặn load generation rồi gọi đó là zero miss.
2. Chạy fast exact matrix toàn affected product trên hai replica: warmup, latest,
   reference, typed status, snapshot/cursor và gap diagnostics; không stream/order.
3. Chạy targeted protocol/fault matrix của backend mới; lỗi nào xác định layer/binding
   thì sửa/test đúng layer trước, không chạy lại full C2 để tìm lỗi.
4. Dùng no-order consumers thực qua SDK với staged workload 20 -> 35 -> 50 alpha +
   TS 60 route, bắt đầu bằng smoke nhỏ nếu cần. Không cần dựng 50 strategy engine để
   benchmark Data Layer, nhưng phải giữ đúng 50 logical clients, quota và data requests.
5. Canary consumer routing theo manifest/versioned config: TS data consumer trước,
   sau đó representative alpha read clients. Không đổi signal/sizing/order path.
6. Đo burst bằng real capture replay trong scope test và lưu lượng provider thật; kiểm
   lag từng partition, replay/catch-up và cold-warmup interference. Kafka fault drills
   không được kill broker production chỉ để lấy evidence.
7. Diễn tập rollback/return và chỉ khi các matrix xanh mới chạy acceptance cuối 300s.
   SDK snapshot/reconnect/allowed fallback phải đúng, product BLOCKED không lén fallback.

**Exit:** đạt tải 50 + TS theo denominator đã freeze; no unexplained data loss/duplicate
application/cross-mix; latency và recovery đúng budget; không backlog tăng không giới hạn,
OOM hoặc resource pressure chưa giải thích. Được có backlog hữu hạn khi burst nếu
catch-up/RTO đã chốt và dữ liệu quá hạn không được coi eligible.
TS report đủ từng route và classification; exception OKX DOGE QUOTE đã được owner ghi
chỉ áp đúng scope đó, không âm thầm biến nó thành nới SLA cho mọi feed.

**Rollback/cleanup:** route về V2 baseline cho product V1 không support; V1 chỉ fallback
khi policy cho phép. Restore exact image/config và SDK route revision, không dời offset.
Stop/delete read clients sau run; giữ một rollback set và evidence, không để test client
chạy qua đêm không chủ đích. Mọi order mutation ngoài scope là test failure.
**Dừng scope:** chưa tuyên bố cross-host HA, all-venue production certification hoặc
unbounded alpha capacity; chưa tag khi source/runtime/certificate chưa hội tụ.

<a id="astra-phase-4-release"></a>
#### Phase 4 - Final Cutover, Retirement And Stable Release

**Status:** `PROPOSED / REQUIRES PHASE 3 EXIT`.
**Goal:** một runtime rõ ràng, không giữ hai kiến trúc không thời hạn; phát hành candidate
`v2.2.0` đã chứng minh trong scope, cleanup an toàn và đủ hướng dẫn vận hành.
**Đọc:** §13.11-13.13; workspace/project AGENTS; release/rollback procedures hiện có.

**Phải làm:**

1. Hoàn tất versioned route handoff cho scope được duyệt; mỗi role có source SHA,
   image digest, config/manifest revision, mount/volume và rollback entry chính xác.
2. Đối chiếu group progress, history backup, replay retention và readiness. Sau đó
   dừng writer/HTTP ingest/lease/tick-spool path cũ; không gọi stop container là đã
   được phép xóa volume. Lập retention/expiry cho rollback và archive cần giữ.
3. Gỡ code/flags không còn caller sau inventory; không xóa compatibility contract vì
   không nhìn thấy traffic trong một cửa sổ ngắn. Cập nhật ADR sunset và runbook recovery.
4. Source -> feature PR -> dev/CI -> release PR -> main theo workflow repo. Build/attest
   immutable image từ revision release; receipt liên kết exact runtime/config và
   source tested. Nếu merge SHA khác, xác minh tree/provenance và chạy affected smoke;
   không ghi image cũ là build từ SHA mới, không force-push để giả hội tụ.
5. Publish release khi gate đạt, cùng benchmark consumer-call-to-usable, capacity
   envelope, unsupported/deferred products và known external limitations rõ ràng.
6. Cleanup từng artifact theo reference: active set + named rollback, xóa test image
   và unused build cache trong scope được duyệt; remove merged worktree/branch sau khi
   xác nhận code đã nằm ở dev/main. Disk before/after và restart check ghi vào plan.

**Exit:** CI/test/receipt/source/image/config cùng được trace; endpoints/SDK cũ tương
thích; target workload đã pass; không writer cũ âm thầm tiếp tục ghi; old-state removal
có approval và restore path; canonical checkout, dev, main/release và runtime rõ ràng.

**Rollback:** còn hiệu lực trong cửa sổ đã công bố, có image **và** config/data recovery
cần thiết; chỉ giữ image nhưng bỏ state bắt buộc không phải rollback có thể chạy.
**Dừng scope:** không mở hạ tầng mới, trading-system upgrade, order test, P19 hay migrate
DNSE/Spot ngoài phạm vi chỉ để đóng release này.

<a id="astra-test-and-evidence"></a>
### 13.9 Matrix test tập trung và cách dùng lại evidence

| Nhóm | Case tối thiểu phải cover | Evidence / phase sở hữu |
|---|---|---|
| Canonical input | committed/aborted transaction, corrupt schema, wrong identity, out-of-order source | Rust/golden + real Kafka integration, P1 |
| Stream delivery | ring hit/miss, filtered offsets, sparse key, expiry, reconnect replica, overlapping subscribers, slow client, cancellation | Event ID/value/order comparison và memory limits, P1 |
| Auth/SDK | JWT/manifest revision, cross-slice denial, cursor tamper, expired token, correct renewal/reconnect | Existing SDK dùng server thật, P1-P3 |
| Latest projection | crash/retry, rebalanced old worker, generation switch, empty/partial Redis, quiet state older than short replay | State + checkpoint + typed readiness, P2 |
| L2 | snapshot/delta boundary, duplicate, sequence gap, checksum failure, resync, missing snapshot, depth preservation | Verified book oracle, P1-P2 |
| BAR | finality, calendar intervals, 5k history, missing old opens, late correction, repair không lùi latest, history-to-live | Provider/canonical/history tie-out, P2 |
| Query | strict batches, replica progress, hot/cold fairness, canceled cold work, partial vs require_all semantics, diagnostics unavailable | Typed item results + queue/latency, P2 |
| Reference | native/derived basis lineage, missing/unit, MARK/INDEX component quality, unchanged provider limits | Reuse adapter cert + changed read-path tests, P2 |
| Recovery/load | 20/35/50 clients + TS, burst, per-partition lag, one replica failure, cache recovery và rollback | Timelines/metrics/route outcomes, P3 |
| Release | exact provenance, cold boot, allowed rollback, resource inventory và cleanup | Release receipt/CI/disk inventory, P4 |

- **Không chạy lại toàn bộ venue research/normalization đã certified nếu source/domain
  đó không đổi.** Ghi evidence ID/source scope được kế thừa, không chỉ nói "đã test rồi".
- **Phải test lại affected read/replay path của toàn demanded scope** vì backend/cursor
  thay đổi. Reuse 299-product inventory/fixtures nếu vẫn là inventory hiện hành; không
  mặc định con số 299 là bất biến hoặc một BTC smoke bao phủ mọi product.
- Unit dùng synthetic có test provenance; integration dùng captured/provider data đúng
  nguồn. Replay tăng tốc không được sửa event timestamp rồi gọi đó là live freshness.
- Khi fake cần thiết, khóa protocol/spec của collaborator; ít nhất một test dùng class
  thật để bắt lỗi property/method như `subscriber_count`. Existing five suite errors
  phải được phân loại; không gọi toàn suite xanh khi chúng vẫn lỗi.
- Test fail ghi `identity + endpoint + replica + typed code + watermark/generation +
  quality hash + latency components`, không chỉ `DataLayerError` chung chung.
- Một final C2 300s là bước certification sau matrix, không công cụ dò bug mỗi vòng.
  Nếu binary/routing đổi sau certificate, đánh giá affected scope và cập nhật receipt
  trung thực; không tái dùng receipt cũ cho đường thực thi mới chưa kiểm.

<a id="astra-benchmark-contract"></a>
### 13.10 Benchmark: đo đúng thứ consumer cần và đúng sức tải

**Bốn loại thời gian bắt buộc tách riêng:**

1. `provider_event -> host_receive`: network + provider timing, dùng clock có kiểm tra skew.
2. `host_receive -> canonical_commit -> served_view`: pipeline/projection age và lag.
3. `consumer_call -> usable validated result`: đo trong container client qua SDK thật,
   gồm DNS/TLS/auth/network/queue/decode/quality check; không chỉ thời gian server handler.
4. `final_bar_close -> consumer_signal_input_ready`: BAR reaction delay; khác dropout
   budget của một history window. Record rejected/not-ready observations, không chỉ các
   response thành công được chọn lọc.

Stream còn report inter-arrival/progress, replay catch-up, reconnect recovery và
source/session liveness. Event age của quiet feed không đồng nghĩa transport chết;
session live cũng không tự làm stale price đủ điều kiện execution.

**Workload đích kế thừa và phải xác minh bằng manifest thực trước run:**

| Class | Số client | Traffic mỗi client |
|---|---:|---|
| Candle/signal | 20 | BAR 1m stream + QUOTE poll 1 Hz |
| Realtime | 15 | TRADE + QUOTE streams + MARK_INDEX poll 1 Hz |
| Grid/L2 | 10 | BAR + QUOTE + BOOK_DELTA streams, snapshot bootstrap/recovery, MARK_INDEX 1 Hz |
| Multi-symbol | 5 | Hai QUOTE streams + hai-item MARK batch 1 Hz + reference một lần/phút |
| Trading System | Consumer thật | 60 route hiện hành, giữ freshness/policy và frequency thực |

Đây là khoảng 90 alpha subscriptions và 50 hot HTTP requests/s, chưa tính TS,
bootstrap/recovery/reference. Phải đếm thêm item/s, messages/s và bytes/s: batch hai item
không bằng một item, và một canonical event có thể fan-out đến nhiều subscriber.
Không tự chia cùng identity quota nhỏ cho 50 clients rồi benchmark throughput bị kìm.

Budget consumer-call-to-usable dùng mốc đã thảo luận, cần freeze theo đúng operation
trước test, không diễn giải thành provider-age SLA:

| Operation | Mốc p95 / p99 | Điều kiện |
|---|---|---|
| Hot QUOTE/TRADE read | 100 / 250 ms | Warm connection và cold connection report riêng |
| MARK_INDEX read | 250 / 500 ms | Component freshness và eligibility vẫn kiểm riêng |
| L2 read | 300 / 750 ms | Snapshot size/depth và replay không trộn một histogram |
| BAR latest read | 1.000 / 2.000 ms | Không phải toàn bộ warmup 5k hoặc close-to-signal SLA |

Nếu budget hiện hành khác, agent phải đối chiếu/ghi quyết định trước run, không chọn
ngưỡng có lợi sau khi thấy kết quả. Warmup 2.500/5.000, reference batch, cursor replay,
diagnostics và stream-open có histogram/error/recovery riêng, không bị giấu khỏi báo cáo.

**Báo cáo mỗi stage:** actual source rate/mix/bytes; per-partition lag và oldest age;
offered/admitted/completed/error/timeout/in-flight; p50/p95/p99/max và sample count theo
venue/feed/operation/replica; CPU usage/throttle, RSS/working set, queue/ring peaks,
Kafka/Redis/SQLite disk và I/O. Freshness rejection đúng domain vẫn là unavailable
observation cần báo, không được bỏ mẫu để làm latency đẹp.

Target `<=5 vCPU @3.000 canonical events/s` ở Opus chỉ là hypothesis. Chốt resource
envelope sau P1 rồi xác nhận lại với fan-out/cold load ở P3; không cam kết trước kết quả.
Tăng cap chỉ khi profile chứng minh throughput/tail cải thiện tương xứng, và không
đẩy áp lực sang TS/DB/consumer. Single-host RF3 và 300s acceptance không chứng minh
regional DR, nhiều ngày ổn định hoặc chịu được mọi tương lai không giới hạn.

### 13.11 Scope, rollout, rollback và cleanup thống nhất

- **Giữ:** venue adapters/domain contracts/provider quotas; SDK/public endpoint shape;
  data truth và source timestamps; no-order boundary; existing authority policies.
- **Cho phép trong đề xuất:** thay internal Stream/projector/Query/BAR-state dependency,
  typed cursor version, scoped materialization/recovery và affected tests.
- **Không bao gồm:** order/risk/alpha strategy changes; V1 overhaul; move/reset Kafka
  offsets để bỏ backlog; broker topology change tự phát; flush/delete shared state;
  bật DNSE/Spot/Deribit production entitlement; lakehouse/Kubernetes/new message bus.
- Approval execution sau này phải bao gồm đúng roles/images/configs/state transitions
  trong scope. Không hỏi lại cho từng retry đã nằm trong approval, nhưng việc viết tài
  liệu hôm nay không cấp quyền restart/xóa dữ liệu hoặc đổi authority.
- Không xóa old spool khi chưa có migrated history + replay/restore proof. Dừng group
  cũ không đồng nghĩa phải reset nó; giữ rollback checkpoints với retention hữu hạn.
- V1 fallback chỉ cho sản phẩm policy cho phép; advanced V2 rollback cần old V2 binary
  và state/config tương ứng. Không giữ old stack chạy mãi chỉ vì chưa định nghĩa expiry.
- Mỗi phase inventory test artifacts, dọn đúng scope sau test, ghi retention và disk
  trước/sau khi có cleanup. Không tạo image mới khi chỉ thay harness/config có thể chạy
  bằng candidate hiện có; build khi binary/dependency thực thay đổi.
- Một canonical checkout, một feature train cho thay đổi này; không mở worktree/branch
  cho từng symbol, lần test hay lần retry. Feature -> dev -> main/release, không push
  hoặc merge ngoài quyền owner đã cấp. Giữ các branch khác có code riêng chưa merge.

### 13.12 Thời gian, decision boundary và cách tránh lặp lại vòng cũ

Đây là thay backend có state/cursor, **không phải một config hotfix vài giờ**. Mốc 2-3
tuần ở Opus là ước lượng chưa đo, không deadline đã được chứng minh. Trong P1 cần một
prototype nhỏ/timebox được ghi trước để biết native path có cải thiện thật; sau đó mới
chốt lịch theo khối lượng auth/SDK/replay/materializer thực, không hứa hoàn thành 1-2 ngày.

Để làm nhanh đúng cách:

1. Bỏ hướng tối ưu vô hạn single writer sau khi owner chọn C; chỉ vá lỗi an toàn cấp bách
   của runtime cũ nếu được chấp thuận, không chạy song song hai chương trình nâng cấp.
2. Giữ contract/domain và test fixture đã tốt; thay backend qua adapter/interface có sẵn.
   Không thêm abstraction tổng quát cho một tương lai chưa có requirement.
3. Rust-first cho hot path, nhưng không rewrite SDK/control plane/provider lịch sử chỉ
   để đồng nhất ngôn ngữ. Không làm hai gateway hoàn chỉnh Python rồi Rust liên tiếp.
4. Freeze workload/evidence schema trước implementation để cùng một bug được phát hiện
   bằng matrix ngắn; full acceptance ở cuối, không tạo ceremony mới sau từng lỗi.
5. Commit mỗi coherent tested slice với identity owner; journal scope/results/remaining
   work tại Unified Plan. Source pass, shadow pass, runtime pass và release là bốn trạng
   thái khác nhau, không đổi tên chúng để tạo cảm giác đã xong.
6. Nếu benchmark không đạt, chỉ ra bottleneck/correctness failure cụ thể và sửa ở phase
   hiện hành. Chỉ xin quyết định thiết kế khi thật sự vượt scope/semantics/resources đã
   duyệt; không gọi code chưa viết xong là external technical debt.

### 13.13 Quyết định đề nghị owner chốt và trạng thái tài liệu

| Quyết định | Khuyến nghị Astra | Trạng thái |
|---|---|---|
| Hướng kiến trúc | C có các điều chỉnh tại §13; không A dài hạn, không D toàn bộ | Đề xuất, chưa triển khai |
| Hot runtime | Rust consume/replay/materialization/fan-out; Python API/SDK/control | Đề xuất, prototype P1 phải đo |
| Stream replica | Hai replica độc lập đọc đủ partition, bounded buffers/replay | Đề xuất; xác minh CPU/fan-out |
| History | BAR-only durable store với checkpoint/backup; provider backfill có giới hạn | Đề xuất; migration P2 |
| Topology | Giữ Kafka hiện có; chưa tăng partition/service theo phỏng đoán | Đề xuất mặc định |
| Target acceptance | 50 logical alpha theo workload thật + TS, matrix trước C2 | Không hạ mục tiêu cũ |
| Release | Candidate v2.2.0 sau source/runtime/consumer/provenance gates | Chưa certified/published |

**Kết luận:** giữ Kafka và những contract/domain đã đúng, thay điểm ghi tuần tự dư thừa
và ràng buộc cache đang cản toàn pipeline. Kafka giữ durable replay, Redis giữ latest,
BAR store giữ history, Stream phục vụ từ committed log mà không buộc mỗi tick phải
fsync thêm một lần vào SQLite. Tốc độ đến từ bỏ công việc/đợi không cần thiết, không từ
bỏ finality, nới freshness, đổi timestamp hay im lặng mất event.

**Receipt của lần bổ sung này:** documentation-only; phần Opus nguyên vẹn; bốn phase
vẫn pending approval. Chưa có code/runtime/resource cleanup mới và không tự phát hành
release. Journal tương ứng nằm ở
[Unified Plan - Astra review addendum](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#kafka-native-astra-review-addendum).

---

<a id="opus-astra-merged-plan"></a>
## 14. Opus: đánh giá §13 và bản hợp nhất để triển khai

> **Ngày:** 2026-09-23. **Trạng thái:** đề xuất hợp nhất, chờ owner duyệt. Chưa có
> code/runtime nào thay đổi.
> **Kiểm thêm trong lượt này (chỉ đọc):**
> - lag Kafka lúc 15:58Z;
> - Rust workspace (`Cargo.toml`, `rust/*`);
> - coupling của `qdl/runtime/stable_bar_edge.py`;
> - checkpoint phía server (`qdl/replay/handoff.py`) và phía SDK (`qdl_sdk/client.py`);
> - ACL Kafka (`scripts/phaseb_bootstrap_stable_broker.py`);
> - network alias trong `docker-compose.v2-stable.yml` và biến target gRPC của TS.
>
> **Đọc cùng:** §9 (Opus) và §13 (Astra). Khi hai phần khác nhau, **§14.3 là bản chốt đề
> xuất**.

### 14.1 Đánh giá §13 của Astra

Nhìn tổng thể, §13 mạnh hơn §0–12 ở **tính đúng**: ranh giới EOS, idempotency của sink,
replay không được seek consumer live, BAR phải có checkpoint cùng transaction, và 4 đại
lượng latency (khớp rule owner 09-18). Điểm yếu của §13 là **nhiều điều cấm nhưng ít
việc cụ thể** (module, interface, số đo cổng thoát), và có hai khuyến nghị đắt mà chưa
kiểm chi phí.

| # | Điểm của Astra | Đánh giá | Bằng chứng / lý do |
|---|---|---|---|
| 1 | Không warmup tick ≠ không replay tick (§13.3-1) | **Đồng ý.** §9 đã có ring + seek Kafka; Astra nói rõ hơn | Đưa vào WI P1.4 |
| 2 | Sink vẫn phải idempotent, có fencing (§13.3-2, 13.6) | **Đồng ý.** Bản hợp nhất còn **bỏ hẳn sink ngoài cho latest** (§14.3 D3) nên vấn đề chỉ còn ở kho BAR, nơi nó được giải bằng một transaction | D3, D4 |
| 3 | Redis replay vài phút không đủ cho feed ít cập nhật (§13.3-3) | **Đồng ý, và câu "tua lại vài phút" ở §9.1 của tôi sai.** Sửa bằng checkpoint latest cục bộ + đọc lại toàn bộ retention khi khởi động mới | D3 |
| 4 | Hai replica đọc toàn bộ không nhân đôi capacity (§13.3-4) | Đồng ý | — |
| 5 | Offset không liên tiếp không có nghĩa là gap (§13.3-5) | Đồng ý | WI P1.3 |
| 6 | EOS không bao phủ Redis/SQLite (§13.3-6) | Đồng ý | D4 |
| 7 | "Lag p3/p5 tăng, không hứa 45 phút" (§13.3-7) | **Kết luận không đúng, nhưng do lỗi của tôi.** Phụ lục A1 bản đầu ghi ngược chiều số. Thực tế lag **giảm**: p3 642.232 → 626.222 trong 64 s, và lúc 15:58Z còn 35.292. Nguyên tắc "theo dõi từng partition" của Astra vẫn đúng | A1, A10 (đã sửa) |
| 8 | 99,7% là số event, không phải byte (§13.3-8) | **Đồng ý về chữ.** Chi phí lock/fsync/GIL tỉ lệ với **số lần ghi**, nên kết luận "bỏ ghi tick" vẫn đứng. Không suy ra tiết kiệm 99,7% RAM/đĩa | — |
| 9 | Tổng CPU cap vượt số lõi không phải bug (§13.3-9) | Đồng ý | — |
| 10 | 3.000/s và ≤5 vCPU là giả thuyết (§13.3-10) | Đồng ý. Cổng thoát ở §14.5 dùng số đo | — |
| 11 | **Rust-first cho toàn bộ hot data plane** (§13.4, 13.12-3) | **Chưa đồng ý cam kết trước.** Workspace Rust hiện **không có gRPC server (tonic), HTTP server hay JWT** (`Cargo.toml` members: contracts, core, kafka, provider-envelope, realtime-core, venue-core). Gateway Rust phải port: auth RS256/ES256 + manifest revision (`qdl/security/data_plane.py` 363 dòng, `grpc.py` 130, `policy.py` 240), matching/freshness (`qdl/stream/grpc_service.py` 576), codec cursor. Đây là hạng mục đắt nhất cả kế hoạch. Nguyên nhân trần là **writer đơn có trạng thái**, không phải bản thân Python: replica mới không có trạng thái chung nên scale ngang được | D2 |
| 12 | Rust latest materialization → Redis (§13.4) | **Đề xuất đơn giản hơn:** bỏ sink Redis cho latest; mỗi replica tự giữ latest trong RAM từ Kafka | D3 |
| 13 | Materializer theo partition (§13.4) | Với BAR (0,3% lưu lượng) **một** materializer là đủ; một file, một writer, không tranh chấp | D4 |
| 14 | BAR edge gắn `cache_identity` (§13.2, 13.6) | **Đúng, đã kiểm:** `stable_bar_edge.py:102-109` đọc `cache_identity`; `:922` `_assert_canonical_cache_identity` | WI P2.3 |
| 15 | ACK không được để hai alpha cùng identity đẩy checkpoint của nhau (§13.6) | **Rủi ro này không xảy ra trong thực tế:** `GapFreeHandoff.acknowledge` (`handoff.py:333-338`) không có caller trong `qdl/stream`; ACK của SDK chỉ lưu cục bộ (`qdl_sdk/client.py:368-375`). Bản hợp nhất **bỏ hẳn checkpoint phía server**: resume chỉ bằng token | D5 |
| 16 | Không double-run cả stack trên host đang bận (§13.7) | Đồng ý. §14.6 có ngân sách shadow và điều kiện dừng | §14.6 |
| 17 | Bỏ header raw chỉ khi đã kiểm dependency (§13.7) | Đồng ý. Không nằm trên đường găng; để ở P2, **tùy chọn** | WI P2.7 |
| 18 | Test matrix §13.9, benchmark §13.10, scope §13.11 | Đồng ý, dùng nguyên làm checklist | §14.5 |

### 14.2 Đính chính phần Opus (§0–12)

1. **A1 ghi ngược chiều lag** (đã sửa ở §2.2, Phụ lục A1, thêm A10). Backlog đã tự giảm:
   15:58Z p3 = 35.292, các partition khác < 1.000.
2. **§9.1 "mất Redis → tua lại vài phút là đủ" là sai** với feed ít cập nhật. Thay bằng
   D3.
3. **§9.1 nói Redis latest "không cache identity, không lease" là chưa đủ**: nếu còn sink
   ngoài thì phải có CAS theo offset. Bản hợp nhất bỏ sink đó (D3).
4. **§0 "2–3 tuần" quá lạc quan.** Ước lượng mới ở §14.7, vẫn là **[ước lượng]**.
5. Sửa ở trên làm SHA-256 của phần Opus mà journal Astra ghi (`617d75aa…`) không còn khớp.
   Chỉ đổi 2 dòng số liệu và thêm 1 dòng A10; không đổi nội dung nào khác.

<a id="opus-astra-decisions"></a>
### 14.3 Các quyết định hợp nhất (cần owner chốt)

| ID | Chủ đề | Opus (§9) | Astra (§13) | **Hợp nhất** | Vì sao |
|---|---|---|---|---|---|
| D1 | Hướng | C | C có điều chỉnh | **C hợp nhất.** Không tiếp tục A, không làm D | Cả hai đồng ý |
| D2 | Ngôn ngữ Stream | Python, quyết bằng spike | Rust-first | **Python stateless replica trước, kèm luật chuyển Rust định sẵn trước khi đo** (§14.5 P1.1). Không viết hai gateway hoàn chỉnh | Tái dùng ~1.800 dòng auth/contract đã chứng nhận; replica không có trạng thái chung nên scale ngang. Nếu đo không đạt thì chuyển Rust ngay ở P1, không đợi sau |
| D3 | Latest (QUOTE, MARK, BOOK_SNAPSHOT, funding…) | Projector → Redis | Rust materializer → Redis + CAS | **Không có sink ngoài.** Mỗi replica Stream/Query có `LatestView` trong RAM, đọc Kafka (lọc theo key, chỉ decode bản ghi cuối mỗi key trong mỗi batch), kèm **checkpoint cục bộ** (SQLite nhỏ ~200 dòng, ghi mỗi 5 s) | Bỏ 6 projector khỏi hot path; không còn Redis coherence/CAS/fencing; trạng thái mỗi replica là hàm thuần của log Kafka nó đã đọc. Feed ít cập nhật được giữ nhờ checkpoint |
| D4 | Lịch sử BAR | SQLite BAR | BAR store + checkpoint atomic | **Một `bar_materializer`** ghi `bars.sqlite3`: upsert bar và offset Kafka **trong cùng transaction**; lưu cột OHLCV đã decode | BAR chỉ 0,3% lưu lượng; một writer không tranh chấp; warmup đọc cột thay vì decode protobuf từng dòng |
| D5 | Checkpoint consumer | — | ACK đúng scope | **Chỉ token phía client** (SDK đã làm vậy). Bỏ `consumer_checkpoints` phía server | Không có caller thật (§14.1 #15) |
| D6 | Cursor | Offset Kafka | Tách transport/offset/slice/epoch | **Token v3** (§14.4.1) | Kết hợp cả hai |
| D7 | Cutover | Trỏ consumer | Route theo manifest/config | **Đổi network alias** `qdl-v2-query`, `qdl-v2-stream-a/b` trên `executor_network` | TS đã trỏ vào alias (`.env.example:192-193`, compose `:316,348,380-382,409-411`). **Không recreate TS** (tránh coupling #8 ở CLAUDE.md); rollback = đổi alias ngược lại |
| D8 | Header raw trong canonical | Bỏ | Chỉ khi an toàn | **Tùy chọn ở P2**, có kiểm dependency | Không nằm trên đường găng |
| D9 | Topology Kafka | Không đổi | Không đổi | **Giữ 6 partition**, không đổi ACL nếu không bắt buộc (§14.4.6) | Đổi partition làm đổi mapping cursor |

### 14.4 Thiết kế kỹ thuật chốt

```text
ingestor ─► Kafka raw ─► rust_core ×3 ─► Kafka md.canonical.v2  (không đổi)
                                            │  assign() cả 6 partition, read_committed, không commit group
          ┌─────────────────────────────────┼──────────────────────────────┐
          ▼                                 ▼                              ▼
  stream_v3 ×2 (Python)            query_v3 ×2 (Python)           bar_materializer ×1 (Python)
  - LatestView + checkpoint        - LatestView + checkpoint      - lọc key "/bar/"
  - Ring theo key (tick lossless)  - snapshot/latest/MARK         - upsert bars + offset (1 txn)
  - Subscription index             - BAR warmup/history ◄──────── bars.sqlite3 (đọc read-only)
  - Replay reader riêng            - route HTTP giữ nguyên
  - gRPC contract giữ nguyên
  alias qdl-v2-stream-a / -b       alias qdl-v2-query
Redis stable_redis: chỉ còn quota JWT / provider admission (+ key tương thích V1 nếu kiểm kê còn cần)
```

#### 14.4.1 Token cursor v3
- Codec giữ nguyên (`SignedHandoffCursorCodec`, HMAC, xoay key). Payload thêm
  `version=3`, `topic_id`, `kafka_partition`, `kafka_offset`, `partition_key`,
  `plan_epoch`. `watermark_offset = kafka_offset` của bản ghi cuối mà view đã áp dụng
  cho slice đó.
- `generation_id = "{topic_id}:{partition_count}:{plan_epoch}"`. Topic id hiện tại:
  `ljfjPYApRpWQd79McfTtZg` (A8).
- `StreamRecord.logical_offset = kafka_offset`. Offset này tăng nghiêm ngặt theo slice,
  nên kiểm tra của SDK (`client.py:355-364`) vẫn đúng.
- Token cũ (SQLite `cache_id`) → `CURSOR_EXPIRED` → SDK tự `SNAPSHOT_REPLACED` một lần
  mỗi slice.
- Ánh xạ key → partition: học từ bản ghi đã tiêu thụ, **không tự tính murmur2**. Nếu key
  chưa từng thấy thì không cấp cursor (typed `NOT_READY`).
- Lỗi typed: token sai → `CURSOR_INVALID`; ngoài retention → `CURSOR_EXPIRED`; quá giới
  hạn quét → `CURSOR_EXPIRED` kèm lý do `REPLAY_SCAN_CAP`.

#### 14.4.2 Replay sang live trong một replica (thay barrier dùng file chung)
1. Luồng consumer đẩy batch vào event loop. Mỗi partition có `applied_offset[p]` (bản
   ghi cuối đã xử lý).
2. `Subscribe(token X)`: trong event loop (không có await xen giữa), đăng ký subscription
   vào index với trạng thái `REPLAYING`, và chốt `boundary B = applied_offset[p]`. Bản
   ghi mới hơn B được đưa vào **pending buffer** của subscription (giới hạn byte/item).
3. Replay (X, B]: nếu X ≥ đầu ring của key thì lấy từ ring; nếu không thì dùng
   **replay reader riêng** (consumer khác, `assign` một partition, seek X+1, quét đến B,
   lọc key, có trần byte/thời gian/số bản ghi, huỷ được).
   **Không bao giờ seek consumer live.**
4. Phát pending buffer, rồi chuyển sang `LIVE`. Pending đầy thì trả typed
   `RECOVERY_REQUIRED`, không drop im lặng.
5. Không cần lease: tính đúng nằm trong phạm vi một process.

#### 14.4.3 LatestView (dùng chung cho Stream và Query)
- Poll batch; nhóm theo message key (bytes, chưa decode); **chỉ decode bản ghi cuối của
  mỗi key** trong batch cho các feed latest-state. Feed lossless không đi vào LatestView
  của Query.
- Mỗi slice giữ trạng thái typed: `READY` / `STALE` / `NOT_READY`, dựa trên timestamp gốc
  của nguồn. Không thay timestamp gốc bằng thời điểm nhận (theo §13.5).
- Checkpoint: file `latest-checkpoint.sqlite3` trên volume riêng của mỗi replica; mỗi 5 s
  ghi `(key, partition, offset, payload)` + `applied_offset[p]` trong một transaction.
- Khởi động có checkpoint: nạp checkpoint, seek mỗi partition tới `applied_offset[p]+1`,
  đuổi kịp rồi mới báo readiness.
- Khởi động không có checkpoint: seek tới đầu retention (6h). Key không thấy trong 6h thì
  `NOT_READY` typed cho tới khi có cập nhật, **không trả dữ liệu sai**.

#### 14.4.4 Kho BAR (`bars.sqlite3`)
- **Bảng:**
  - `bars(binding_key, open_time_ns, close_time_ns, interval, open, high, low, close, volume, quote_volume, final, revision, event_id, kafka_partition, kafka_offset, payload BLOB)`.
    PK là `(binding_key, open_time_ns)`; thêm index `(binding_key, close_time_ns)`.
  - `materializer_checkpoint(kafka_partition PK, next_offset)`.
  - `store_identity(store_id)`.
- **Quy tắc revision:** dùng lại logic hiện có (`_exact_final_bar_window`,
  `qdl/runtime/final_bar_watermark.py`). Repair nến cũ không làm `latest_final_bar` lùi.
- **Materializer:**
  - `assign` 6 partition và seek theo `materializer_checkpoint`. Không dùng group commit;
    vị trí đọc nằm trong SQLite.
  - Mỗi batch: `BEGIN IMMEDIATE`, upsert, cập nhật checkpoint, `COMMIT`. Crash ở giữa
    không thể làm offset đi trước dữ liệu.
- **Migration:**
  - Đọc spool `mode=ro`, copy các dòng BAR và đối chiếu count + hash theo key.
  - Sau đó materializer bắt đầu từ đầu retention Kafka. Upsert idempotent nên phần chồng
    lấn vô hại.
- **Backup:** `sqlite3 .backup` hằng ngày vào volume state, có diễn tập restore. Lý do:
  lịch sử dài hơn retention Kafka nên không coi là cache.

#### 14.4.5 Tài nguyên khởi điểm (cap; số thật chốt sau P1)

| Role mới | CPU cap | RAM cap | Thay cho |
|---|---|---|---|
| stream_v3 ×2 | 1,0 | 512 MiB | stream_v2 active/passive (2,0 + 2,0) |
| query_v3 ×2 | 1,0–1,5 | 1 GiB | query_v2 ×2 |
| bar_materializer ×1 | 0,25 | 256 MiB | projector ×6 (6 × 1,0) |
| **Tổng cap** | **≈4,25–5,25** | | **≈13,0** (stream 4 + query 3 + projector 6) |

#### 14.4.6 Kafka ACL
- Reader mới dùng principal TLS có sẵn `phase8-consumer`, vốn có READ/DESCRIBE trên topic
  (`scripts/phaseb_bootstrap_stable_broker.py:182-190`).
- Với `assign()` và không commit, **[chưa kiểm]** có cần quyền READ trên group không.
  Kiểm ở WI P1.0. Nếu cần thì phải thêm ACL, mà đó là thay đổi broker nên **cần owner
  duyệt riêng**.

<a id="opus-astra-phase-wbs"></a>
### 14.5 Bốn phase — danh sách việc chi tiết

Quy ước: `WI` = work item. Mỗi WI là một slice có test và được commit riêng (G1). Journal
cập nhật theo slice, không theo từng finding (G4).

#### Phase 1 — Stream Kafka-native (shadow). **[ước lượng] 5–7 ngày; +5–8 ngày nếu phải chuyển Rust**

| WI | Việc | File / module | Test / bằng chứng |
|---|---|---|---|
| P1.0 | Chốt baseline: digest image, topic id, manifest revision, 5 lỗi suite có sẵn; **sửa lỗi `subscriber_count`** ở `83fa1bc` bằng test dùng gateway thật; kiểm ACL `assign()` | `qdl/runtime/stable_ingest.py:118`, `tests/test_stable_ingest_spans.py` | Test với class `StreamGateway` thật, bắt được lỗi property |
| P1.1 | **Spike có hạn 2 ngày** với luật quyết định ghi sẵn: một replica Python prototype đọc topic thật (assign, không commit), index fan-out, gRPC tới 150 subscriber SDK từ container client `--rm` | prototype trong scratch; image `qdl-v2-python` có sẵn | Đo: CPU / 1.000 event tiêu thụ; CPU / 1.000 lượt giao; RSS của ring 120 s; p99 latency thêm. **Python đạt nếu:** ở tải giao của stage 50 và tốc độ đuổi ≥3.000 event/s (đọc lại dữ liệu đã lưu, không publish tổng hợp), một replica ≤1,0 vCPU và p99 thêm ≤50 ms. **Không đạt → gateway Rust (tonic) chỉ cho Stream** |
| P1.2 | Codec token v3 + generation | `qdl/replay/handoff.py` | Golden token; token cũ → `CURSOR_EXPIRED`; token bị sửa → `CURSOR_INVALID`; xoay key |
| P1.3 | `KafkaPartitionReader` + `LatestView` + ring theo key (giới hạn byte và thời gian) | mới: `qdl/stream/kafka_source.py`, `qdl/stream/latest_view.py` | Integration với Kafka thật: transaction commit/abort, marker điều khiển tạo khoảng nhảy offset, key thưa |
| P1.4 | Replay reader riêng có trần | mới: `qdl/stream/kafka_replay.py` | Ring hit/miss; ngoài retention; vượt trần quét; huỷ khi client bỏ đi |
| P1.5 | `KafkaStreamGateway` cùng interface mà `grpc_service.py` đang dùng; subscription index; pending buffer; bỏ lease | `qdl/stream/gateway.py` (lớp mới cạnh lớp cũ) | Replay→live không mất và không trùng; 2 subscriber cùng identity; client chậm/bỏ đi; hàng đợi đầy → typed |
| P1.6 | Entrypoint + compose profile shadow `stream_v3_1/2` (port shadow, **chưa gắn alias consumer**) | `qdl/runtime/stable.py` `serve_kafka_stream`; `docker-compose.v2-stable.yml` profile `kafka-native` | Container healthy; readiness = đủ 6 partition + đã đuổi kịp |
| P1.7 | Oracle so sánh: client đọc Kafka độc lập làm chân lý, so với stream_v3 (và với stream_v2 để tham khảo) | mới: `scripts/kafka_native_stream_parity.py` | Báo cáo theo slice: thiếu / thừa / sai thứ tự / sai giá trị |
| P1.8 | Chạy shadow ≥60 phút, có một cửa sổ thị trường sôi động; kill một replica; SDK reconnect sang replica kia | runbook trong Plan | Bằng chứng ở run dir |

**Cổng thoát P1 (số):**
- Trên toàn bộ slice stream của TS + driver 50 alpha: thiếu = thừa = sai thứ tự = **0**
  so với oracle Kafka.
- Kill replica: reconnect p99 ≤5 s, **0 mất event**.
- Tốc độ đuổi ≥3.000 event/s mỗi replica.
- Latency giao thêm p99 ≤50 ms.
- CPU/RAM mỗi replica đo được và nằm trong cap §14.4.5.

**Rollback:** dừng container shadow. Không đụng stream_v2, projector, offset nào.

#### Phase 2 — Query + kho BAR (shadow). **[ước lượng] 5–7 ngày; bắt đầu song song với P1 sau P1.2**

| WI | Việc | File / module | Test / bằng chứng |
|---|---|---|---|
| P2.0 | Kiểm kê: đường ghi repair của bar_edge; consumer còn đọc key/pubsub V1 trên **`stable_redis`** hay trên `redis_marketdata` của V1 (TS `data_layer_bridge.py:390`, `performance/repository.py:755`, alpha `data_layer_client.py:624`); người dùng MARK execution | báo cáo trong Plan | Bảng dependency có `file:line` |
| P2.1 | `BarStore` + schema + quy tắc revision | mới: `qdl/history/bar_store.py` | Unique; repair không làm latest lùi; lịch theo interval |
| P2.2 | `bar_materializer` (lọc key, upsert + checkpoint cùng transaction) | mới: `qdl/runtime/bar_materializer.py`, entrypoint, compose | Crash trước/sau commit; chạy lại idempotent; abort |
| P2.3 | Migration spool → BarStore, đối chiếu count/hash theo key; chuyển checkpoint và identity của bar_edge sang BarStore | mới: `scripts/migrate_spool_bars_to_bar_store.py`; `qdl/runtime/stable_bar_edge.py:102-109,922` | Tie-out 140 key; bar_edge repair ghi đúng kho mới |
| P2.4 | `KafkaNativeQueryBackend`: latest/snapshot/MARK từ `LatestView`, BAR từ BarStore, cấp cursor v3; bỏ HTTP MARK sang Stream | `qdl/runtime/stable_source.py` (backend mới cạnh backend cũ), `qdl/reference/local_mark_index.py` | Matrix read-plane 132 ô; handoff warmup → stream khớp watermark (`verify_bar_handoff` của alpha) |
| P2.5 | Warmup BAR render từ cột, giữ lane lạnh có trần và huỷ được; gỡ duty-cycle nếu số đo cho phép | `qdl/api_v2/router.py`, `qdl/query/cold_work.py` | Warmup 2.500/5.000 cạnh tải nóng: hot p95/p99 vẫn trong budget |
| P2.6 | Checkpoint latest + khởi động lại | `qdl/stream/latest_view.py` | Restart có checkpoint: đo RTO; không checkpoint: key im lặng → `NOT_READY`, không trả dữ liệu sai |
| P2.7 | *(Tùy chọn)* bỏ header raw khỏi canonical sau kiểm dependency | `rust/qdl-kafka/src/lib.rs:836-839` | Đo bytes/s trước/sau; replay dữ liệu cũ vẫn đọc được |
| P2.8 | Shadow `query_v3_1/2` (port shadow) + backup/restore BarStore diễn tập | compose profile `kafka-native` | Parity với Query cũ **tại cùng watermark**, oracle là Kafka |

**Cổng thoát P2 (số):**
- Matrix 132/132 trên cả 2 replica.
- Tie-out BAR bằng nhau cho 140 key.
- Hot p95/p99 trong budget khi có warmup 5.000 chạy đồng thời.
- Restart có checkpoint: RTO ≤60 s **[mục tiêu, đo thật]**.
- Diễn tập restore BarStore thành công.

**Rollback:** dừng shadow. Spool cũ và Query cũ không đụng.

#### Phase 3 — Cutover + nghiệm thu. **[ước lượng] 3–4 ngày**

| WI | Việc | Chi tiết |
|---|---|---|
| P3.1 | Packet cutover ghi trước | Role nào nhận alias; digest image; lệnh `docker network disconnect/connect --alias`; rollback; phạm vi **không đụng** (TS, alpha, Kafka, offset, V1) |
| P3.2 | ~~Chuyển alias Stream trước, giữ Query cũ~~ **Sai — xem §15.1 F2: Stream và Query phải đổi alias cùng lúc trong một script** | TS reconnect: mỗi slice `SNAPSHOT_REPLACED` một lần. Quan sát TS 60/60 ≥2h, có cửa sổ thị trường sôi động |
| P3.3 | ~~Chuyển alias Query sau~~ **Gộp vào P3.2 (§15.1 F2)** | Quan sát như trên; alpha đi theo alias |
| P3.4 | Driver đích stage 20 → 35 → 50 với budget đã đóng băng (`config/v2/v211-target-acceptance-budget.json`), đủ 4 đại lượng latency | Dùng nguyên driver hiện có |
| P3.5 | Burst: một cửa sổ thị trường thật + test đuổi 3.000/s | Lag theo partition có trần và tự hồi |
| P3.6 | Diễn tập rollback: đổi alias về stack cũ rồi đổi lại | Đo thời gian và ảnh hưởng tới TS |

**Cổng thoát P3:**
- Stage 50 + TS PASS theo budget; DOGE QUOTE chỉ miễn đúng phạm vi đã ghi.
- Không OOM; lag theo partition có trần.
- Rollback ≤5 phút đã diễn tập.

**Không đụng:** image/env của TS và alpha, offset Kafka, V1.

#### Phase 4 — Gỡ bỏ + phát hành. **[ước lượng] 2–3 ngày**

| WI | Việc |
|---|---|
| P4.1 | Giữ stack cũ **dừng nhưng chưa xoá** 72h sau cổng thoát P3; lưu bản sao spool read-only 7 ngày |
| P4.2 | Gỡ code: HTTP ingest, lease Stream, phần tick của spool, projector, cache identity, `consumer_checkpoints`. Chỉ gỡ khi đã kiểm kê không còn caller |
| P4.3 | Viết ADR-0007 (thay ADR-0006); runbook boot recovery rút gọn (không còn rebuild cache); cập nhật CLAUDE.md coupling #4 |
| P4.4 | Chỉnh cap CPU/RAM theo số đo; dọn image theo digest (giữ active + một bản rollback); ghi disk trước/sau |
| P4.5 | Release **v2.2.0** qua PR feature → dev → main; receipt digest/source/config; ledger |

**Cổng thoát P4:** không còn writer cũ; CPU V2 đo được ≤ ngân sách đã chốt sau P1;
release có provenance.

### 14.6 Chạy shadow trên host đang bận
- Máy đang bận khoảng 87% (A3). Ngân sách shadow: **≤1,5 vCPU dùng thật**. Chỉ chạy một
  replica shadow cho phần lớn P1; chạy hai replica chỉ khi test failover.
- **Dừng ngay** khi: idle của host <5% liên tục 60 s; TS ready giảm; hoặc lag của group
  cũ tăng liên tục 5 phút.
- Không publish dữ liệu tổng hợp lên topic production. Tải 3.000/s đo bằng cách đọc lại
  dữ liệu đã lưu.

### 14.7 Thời gian và song song hoá **[ước lượng, chưa có số đo]**
- Tuần tự: P1 5–7 + P2 5–7 + P3 3–4 + P4 2–3 = **15–21 ngày làm việc**.
- Song song P1 (Stream) và P2.1–P2.3 (BarStore) sau P1.2: còn khoảng **12–16 ngày**.
- Spike P1.1 (≤2 ngày) là điểm quyết định sớm. Nếu phải chuyển Rust: +5–8 ngày.
- Hai agent (Codex + Claude) có thể chia P1 / P2 nhưng **cùng repo, cùng identity**:
  phải chia file rõ ràng (CLAUDE.md §3).

### 14.8 Rủi ro lớn nhất và cách giảm

| Rủi ro | Xác suất / ảnh hưởng | Giảm thiểu |
|---|---|---|
| Python không đạt ở tải fan-out stage 50 | Trung bình / cao | Luật quyết định ở P1.1, biết sau ≤2 ngày |
| Replay reader phải quét nhiều vì key thưa trong partition | Trung bình / trung bình | Ring đủ dài cho reconnect thông thường; trần quét typed |
| BarStore migration lệch | Thấp / cao | Tie-out count/hash; spool cũ giữ tới P4 |
| Shadow làm nặng host, ảnh hưởng TS | Trung bình / cao | §14.6 ngân sách + điều kiện dừng |
| ACL Kafka cần thêm quyền group | Thấp / thấp | Kiểm ở P1.0; nếu cần thì xin duyệt riêng |
| Consumer còn dùng key/pubsub V1 trên `stable_redis` | **[chưa kiểm]** | P2.0 kiểm kê; nếu cần thì giữ publisher tương thích nhỏ |

<a id="opus-astra-owner-approval"></a>
### 14.9 Owner cần duyệt

1. **D1–D9** ở §14.3, đặc biệt:
   - **D2**: Python trước, kèm luật chuyển Rust, khác khuyến nghị Rust-first của Astra;
   - **D3**: bỏ Redis latest projector, khác khuyến nghị của cả §9 lẫn §13;
   - **D7**: cutover bằng network alias.
2. **Dừng phương án A** (tối ưu bên trong writer đơn). Chỉ sửa lỗi an toàn cấp bách của
   runtime cũ, ví dụ P1.0.
3. **Phạm vi runtime của P1:**
   - tạo 1–2 container shadow `stream_v3` (image mới build từ commit P1, ghi digest);
   - chỉ đọc Kafka;
   - port shadow, không gắn alias consumer;
   - rollback là dừng container.
   - Không đụng: stream_v2, projector, Query, Redis, SQLite, offset, TS, alpha.
4. **Sau khi owner duyệt:** cập nhật trạng thái trong Unified Plan, dẫn về anchor
   `#opus-astra-phase-wbs`, và bắt đầu P1.0.

---

<a id="opus-final-rebuttal"></a>
## 15. Opus: phản biện cuối và chi tiết thực thi từng bước

> **Ngày:** 2026-09-23. **Mục đích:** ý kiến cuối cùng của Opus trước khi Astra hợp nhất
> với ý kiến owner. Sau mục này, Opus **thực thi theo bản thống nhất**, không theo §14
> hay §13 riêng lẻ.
> **Kiểm thêm trong lượt này (chỉ đọc):**
> - reader của Redis projection;
> - mạng và client của `stable_redis`;
> - Redis mà TS dùng;
> - đường ghi của bar_edge;
> - SAN và hạn của chứng chỉ TLS;
> - chu kỳ BOOK_SNAPSHOT;
> - rule tài nguyên R1.29 của owner.
>
> Không thay đổi code hay runtime.

### 15.1 Tám phát hiện mới làm thay đổi kế hoạch

| # | Phát hiện | Bằng chứng | Hệ quả |
|---|---|---|---|
| F1 | **Redis latest projection chỉ được ghi, không ai đọc.** Trong `qdl/`, key `…:latest:…` chỉ xuất hiện ở code ghi (`qdl/projection/stable.py:319`, `market.py:31`, `trade.py:109`). Query không đọc Redis cho market data. `stable_redis` chỉ nằm trên mạng `qdl_v2_stable_candidate_stable_internal`. TS (market_data, performance) dùng `redis_marketdata` của V1 (`DATA_LAYER_REDIS_URL=redis://redis_marketdata:6379/0`). Client đang nối vào `stable_redis` đều là `eval` từ projector | `docker inspect` networks/env; `redis-cli CLIENT LIST`; grep `qdl/`, `app/`, `scripts/` | Toàn bộ Lua EVAL mỗi record, cache identity, `ProjectionCacheMismatch`, runbook rebuild và unit boot-recovery đang phục vụ **một cache không có người đọc**. D3 không chỉ đơn giản hơn mà còn **bỏ việc chết**. Key/pubsub tương thích V1 trên `stable_redis` cũng không consumer nào chạm tới được, nên bỏ an toàn. Redis còn giữ: quota JWT, provider admission, lease (bỏ ở P4) |
| F2 | **Stream và Query phải chuyển cùng lúc.** Token do Query cấp (HTTP warmup/snapshot) được Stream nhận lại (`grpc_service.py:322-328`), và generation gắn với backend (`stable.py:560-571`) | code | **§14.5 P3.2/P3.3 (chuyển Stream trước, Query sau) là sai.** Nếu tách, TS sẽ lặp vô hạn: Query cũ cấp token cũ → Stream mới trả `CURSOR_EXPIRED` → lấy lại snapshot ở Query cũ → token cũ lại. Phải đổi 4 alias trong **một script**, vài giây |
| F3 | **SAN của chứng chỉ đã có alias.** stream: `qdl-v2-stream`, `qdl-v2-stream-a`, `qdl-v2-stream-b`; query: `qdl-v2-query`. **Hết hạn 2026-11-20** | đọc `server.crt` trong volume `stable_tls` (container `--rm --network none`) | Container mới dùng lại chứng chỉ, không cần cấp mới. Shadow phải gọi bằng tên có trong SAN, tức alias trên mạng test riêng (§15.3 P1.6). **P4 phải xong trước ~2026-11-10**, nếu không sẽ trùng với việc xoay CA, mà CA không roll được (CLAUDE.md coupling #5) |
| F4 | **bar_edge ghi vào Kafka raw** (`stable_bar_edge.py:1744`) và **xác nhận nến đã bền bằng cách đọc spool read-only** (`:969-989`, identity `:102-109,922`) | code | P2.3 chỉ là đổi đường dẫn + truy vấn sang BarStore (cùng dạng `binding, open_time, final`), không phải thiết kế lại |
| F5 | **BOOK_SNAPSHOT và BOOK_DELTA chung key;** rust_core phát snapshot đã materialize **mỗi 1.000 ms** (`config/v2/stable-acquisition-bindings.yaml:177,192,421`) | config + `stable_source.py:935-979` | LatestView phải decode record book để tách snapshot khỏi delta. Chi phí nhỏ (18 key book). Tuỳ chọn: rust_core thêm header `qdl-feed` để lọc mà không cần decode |
| F6 | **Rule owner R1.29:** data layer dùng thật **≤5,0 vCPU**; tăng cap phải bù bằng cắt tương đương; ghi tổng cap cạnh mỗi thay đổi; mỗi cửa sổ chỉ đổi một biến; **không revert khi backlog đang xả** | Plan `dl-v2-r129-method-and-guide-20260918` | Shadow thêm cap nên **cần owner cho ngoại lệ có thời hạn** (§15.4 Q3). ≤5,0 vCPU là **cổng thoát bắt buộc của P3/P4**, không phải mục tiêu mềm |
| F7 | **Bốn đại lượng latency đã có định nghĩa của owner** (09-18): request latency, durable event age, delivery lag, end-to-end tới cache alpha. §13.10 của Astra dùng bốn định nghĩa khác | memory owner rule; §13.10 | Báo cáo dùng **định nghĩa của owner**; các mốc của Astra ghi thêm như phần phân rã (§15.2) |
| F8 | **Kafka đã nặng:** kafka2 ≈101% của cap 1,75 (A3). Kiến trúc mới thêm 5 consumer đọc canonical (2 stream, 2 query, 1 bar) | `docker stats` | Đặt `fetch.wait.max.ms` và `fetch.min.bytes` rồi đo CPU broker trước và sau. P2.7 (bỏ header raw, giảm bytes) trở nên có giá trị hơn |

### 15.2 Lập trường cuối so với Astra

**Tôi nhận là Astra đúng và tôi sai hoặc thiếu:**
- replay tick vẫn bắt buộc;
- EOS có ranh giới;
- trần quét tính theo **byte/thời gian** chứ không chỉ số bản ghi;
- BarStore phải có backup và diễn tập restore;
- không viết hai gateway hoàn chỉnh;
- "vài phút" là sai;
- thời gian 2–3 tuần là quá lạc quan.

**Tôi giữ quan điểm khác (và lý do cuối):**
1. **D2 — Python trước, có luật chuyển Rust định sẵn.** Thêm một bằng chứng giảm phạm vi:
   consumer chỉ gọi **`Subscribe`** (TS và alpha không gọi `Replay`, `GetSnapshot`,
   `GetFeedStatus` qua gRPC). Gateway mới, dù Python hay Rust, chỉ cần `Subscribe` +
   health. Vì vậy nếu phải chuyển Rust, phạm vi port cũng nhỏ hơn Astra giả định:
   - `qdl-stream-gateway` = tonic + rdkafka (đã có trong `qdl-kafka`) + JWT RS256/ES256;
   - codec token HMAC khớp byte với Python, dùng chung golden test;
   - matching và freshness cho `Subscribe`.
2. **D3 — bỏ Redis latest.** F1 cho thấy đây là việc chết, không phải một thiết kế cạnh
   tranh.
3. **Một bar_materializer**, không chia theo partition: BAR chỉ 0,3% lưu lượng.

**Bốn đại lượng latency: bảng ánh xạ để hai bên dùng chung:**

| Đại lượng owner | Điểm đo | Mốc tương ứng ở §13.10 |
|---|---|---|
| 1. Request latency | client SDK trong container consumer | `consumer_call -> usable validated result` |
| 2. Durable event age | trong response (`measured_freshness_ms`) | một phần của `host_receive -> served_view` |
| 3. Delivery lag | sự kiện sàn (BAR: close time) → publish canonical | `provider_event -> host_receive` + `-> canonical_commit` |
| 4. End-to-end tới cache alpha | sự kiện sàn → nằm trong cache alpha/TS | `final_bar_close -> consumer_signal_input_ready` (BAR) |

**Điểm tôi chưa chắc (phải đo, không tuyên bố):**
- Python có đạt fan-out ở stage 50 không.
- LatestView trong Query có làm tăng tail latency do GIL không.
- Ring ≤512 MiB có đủ không.
- CPU broker khi thêm 5 consumer.

<a id="opus-step-by-step"></a>
### 15.3 Chi tiết từng bước (runbook cho các work item)

Quy ước chung cho mọi WI:
- **Trước khi sửa:** `git status`, `git log -5`; không đụng file Codex đang có diff.
- **Test:** `python3 -B -m pytest …` trong image test có sẵn.
- **Commit:** một slice đã test = một commit, tác giả BobbyAxerol, không trailer AI; kèm
  một hunk journal (stage bằng `git apply --cached`).
- **Runtime:** chỉ làm trong phạm vi đã duyệt.
- **Evidence:** vào `/home/bobby/.local/state/qdl-v2/kafka-native-<phase>-<date>/`.

#### Phase 1 — Stream Kafka-native (shadow)

**P1.0 — Baseline và chuẩn bị (0,5 ngày)**
1. Ghi baseline:
   - digest image từng role (`docker inspect`);
   - tổng cap CPU hiện tại (R1.29);
   - topic id, partition, retention (A8);
   - manifest revision;
   - 5 lỗi có sẵn của test suite, kèm tên.
2. Sửa `stable_ingest.py:118` (`subscriber_count` là property). Thêm test dùng
   **`StreamGateway` thật**, xác nhận test fail trên code cũ và pass trên code mới.
   Commit riêng. **Không roll** lên Stream trừ khi owner yêu cầu.
3. Thử ACL: container `--rm` dùng principal `phase8-consumer`, `assign()` partition 0,
   `enable.auto.commit=false`, đọc 100 bản ghi, **không commit**. Ghi lại có cần quyền
   READ trên group không. Nếu cần thì dừng, xin owner duyệt ACL.
4. Chốt các tham số sẽ đo cho spike (xem P1.1).

**P1.1 — Spike có hạn 2 ngày, luật quyết định ghi trước**
1. Prototype trong scratch, **không commit vào `qdl/`**. Chạy trong container `--rm` từ
   image `qdl-v2-python` có sẵn, mạng `stable_internal`, chỉ đọc.
2. Đo bốn thứ:
   - (a) tiêu thụ: `assign` 6 partition, `consume(1000)`, nhóm theo key; CPU cho mỗi
     1.000 event;
   - (b) đuổi: bắt đầu tại offset của (now − 15 phút) để đo tốc độ đọc tối đa, **cách
     tạo tải 3.000/s mà không publish gì**;
   - (c) fan-out: 150 subscriber gRPC từ container client dùng SDK thật, phân bố slice
     như stage 50;
   - (d) LatestView cho Query: decode bản cuối mỗi key, đo CPU và độ trễ của một luồng
     hot HTTP song song (GIL).
3. Ghi sẵn một tối ưu nếu CPU fan-out cao: serialize `StreamRecord` bằng cách **nối các
   field protobuf đã encode sẵn** (event bytes dùng chung, chỉ token và offset là riêng
   cho mỗi subscriber).
4. **Luật (ghi vào journal trước khi chạy):**
   - Python đạt nếu một replica ≤1,0 vCPU ở tải fan-out stage 50, **và** đuổi được
     ≥3.000 event/s, **và** p99 latency thêm ≤50 ms, **và** LatestView không làm p99
     hot HTTP vượt budget.
   - Không đạt thì chuyển Rust cho Stream (chỉ `Subscribe`). Query vẫn là Python.
5. Kết quả được ghi vào Plan; owner được báo. Tiếp tục mà không cần hỏi lại **nếu kết quả
   nằm trong luật đã duyệt**.

**P1.2 — Token v3 (1 ngày)**
1. `qdl/replay/handoff.py`: payload v3 như §14.4.1.
   - `generation_id` lấy từ TopicId (đọc bằng AdminClient lúc khởi động),
     `partition_count` và `plan_epoch`.
   - Codec v2 vẫn giữ để decode token cũ và trả typed `CURSOR_EXPIRED`.
2. Test:
   - golden bytes;
   - token cũ trả `CURSOR_EXPIRED`;
   - token bị sửa trả `CURSOR_INVALID`;
   - xoay key;
   - sai scope (venue/symbol khác) bị từ chối.
3. Commit. **P2 bắt đầu song song từ đây.**

**P1.3 — `KafkaPartitionReader` + `LatestView` + ring (1,5 ngày)**
1. Tạo `qdl/stream/kafka_source.py`:
   - luồng nền đọc Kafka (`read_committed`, `assign` 6 partition, không commit);
   - `applied_offset[p]`; bảng key → partition học từ bản ghi đã tiêu thụ;
   - đẩy batch vào event loop qua queue có trần.
   - Tham số: `fetch.wait.max.ms` = 20–50 (theo F8), `fetch.min.bytes` đo.
2. Tạo `qdl/stream/latest_view.py`:
   - theo từng batch, lấy bản cuối mỗi key rồi mới decode;
   - key book thì decode để tách snapshot/delta (F5);
   - trạng thái `READY` / `STALE` / `NOT_READY` theo timestamp nguồn;
   - checkpoint `latest-checkpoint.sqlite3`: mỗi 5 s, một transaction, trên volume riêng
     của replica.
3. Ring theo key, chỉ cho feed lossless và BAR:
   - lưu **value thôi, bỏ header raw**;
   - trần theo byte (tổng ≤256 MiB) và theo thời gian (tick 120 s; BAR 128 bản ghi mỗi
     key).
4. Test với Kafka thật (`apache/kafka` digest `9516fb7634ba` đã có sẵn), container `--rm`:
   - transaction abort không lọt vào;
   - control marker tạo khoảng nhảy offset;
   - key thưa;
   - tràn ring;
   - khởi động lại từ checkpoint;
   - khởi động không có checkpoint → `NOT_READY` đúng.

**P1.4 — Replay reader riêng (1 ngày)**
1. Tạo `qdl/stream/kafka_replay.py`: consumer riêng, `assign` một partition, seek X+1,
   quét tới boundary B, lọc key.
   - Trần: ≤200.000 bản ghi **hoặc** ≤64 MiB **hoặc** ≤2 s. Vượt trần → typed
     `CURSOR_EXPIRED` kèm `REPLAY_SCAN_CAP`.
   - Tối đa 4 replay cùng lúc mỗi replica. Client huỷ thì dừng ngay.
2. Test: ring hit/miss, ngoài retention, vượt trần, client huỷ giữa chừng.

**P1.5 — `KafkaStreamGateway` (1,5 ngày)**
1. Lớp mới trong `qdl/stream/gateway.py`, cùng interface `grpc_service.py` đang dùng. Chỉ
   cần `Subscribe` + health; các RPC còn lại trả `UNIMPLEMENTED` và ghi lý do.
2. Index `partition_key → [subscription]`, thay vòng quét tuyến tính
   (`gateway.py:400-407`).
3. Replay sang live đúng thuật toán §14.4.2: đăng ký + chốt boundary trong cùng một
   bước của event loop; pending buffer có trần.
4. Queue theo subscription có trần theo byte và item:
   - feed latest-state: coalesce;
   - feed lossless: tràn → `RESOURCE_EXHAUSTED` (giữ hành vi hiện có).
5. Không lease. Readiness = đủ 6 partition và `lag < 1 s`.
6. Test:
   - replay → live không mất, không trùng (property test với chèn ngẫu nhiên);
   - 2 subscriber cùng identity;
   - client chậm, client bỏ đi;
   - xác thực từ chối venue/symbol khác;
   - payload hỏng → fail-closed.

**P1.6 — Chạy shadow (0,5 ngày)**
1. Thêm profile compose `kafka-native` với service `stream_v3_1`, `stream_v3_2`:
   image mới (build `git archive` + mạng tắt, như quy trình hiện có; ghi digest), mount
   `stable_tls/stream` và cùng env JWT/manifest.
2. Mạng test tạm `qdl_v2_kafka_native_shadow` (tạo khi chạy, xoá khi xong, ghi vào
   cleanup) với alias `qdl-v2-stream-a/-b` **chỉ trên mạng này** (F3). **Không** gắn vào
   `executor_network`.
3. Khởi động bằng `docker compose --profile kafka-native up -d stream_v3_1` (service mới,
   không đụng service cũ). Ban đầu chỉ chạy 1 replica (§14.6).

**P1.7 — Oracle so sánh (1 ngày)**
1. Tạo `scripts/kafka_native_stream_parity.py`:
   - reader Kafka độc lập làm chân lý;
   - N client SDK subscribe stream_v3 trên đúng slice của TS + driver;
   - so theo slice: event_id, giá trị, thứ tự; ghi thiếu / thừa / sai thứ tự.
2. Thêm log JSON mỗi 10 s ở replica (triển khai công cụ đo **trước** khi tune, R1.29):
   offset/lag từng partition, event/s, lượt giao/s, byte ring, pending, số subscription,
   replay đang chạy.

**P1.8 — Chạy shadow và cổng thoát (1 ngày)**
1. Chạy ≥60 phút, có một cửa sổ thị trường sôi động (theo giờ phiên Mỹ/Á đã ghi).
2. Bật replica thứ 2, kill replica 1, xác nhận SDK reconnect sang replica 2 với token cũ.
3. Đọc lại từ (now − 15 phút) để đo đuổi ≥3.000/s trong lúc vẫn phục vụ live.
4. Cổng thoát như §14.5. Không đạt thì sửa trong P1 hoặc báo owner kèm số đo.
5. Cleanup: dừng replica 2 nếu không cần, giữ replica 1 cho P2/P3; xoá mạng test khi
   xong; ghi lại tổng cap.

#### Phase 2 — Query + BarStore (shadow; bắt đầu sau P1.2)

**P2.0 — Kiểm kê (0,5 ngày):** phần lớn đã xong ở F1 và F4. Còn lại:
- người gọi execution MARK (`/internal/v2/execution/mark-index/latest`);
- cách `monitor_service` hoặc các script chứng nhận đọc `stable_redis`
  (grep toàn workspace);
- chốt danh sách gỡ bỏ cho P4.

**P2.1 — `qdl/history/bar_store.py` (1 ngày)**
1. Schema theo §14.4.4, WAL, `synchronous=FULL` (tốc độ ghi thấp nên chi phí không đáng
   kể).
2. Quy tắc revision tái dùng `final_bar_watermark.py`: upsert chỉ khi
   `revision ≥ hiện có`; repair không làm latest lùi.
3. Test: unique, revision, lịch 1m…1w, `read_final_bar_window` tương đương bản cũ.

**P2.2 — `qdl/runtime/bar_materializer.py` (1 ngày)**
1. `assign` 6 partition, seek theo `materializer_checkpoint`; lọc key `/bar/` trước khi
   decode.
2. Mỗi batch: `BEGIN IMMEDIATE` → upsert → cập nhật checkpoint → `COMMIT`.
3. Entrypoint + service compose `bar_materializer`, cap 0,25 CPU / 256 MiB.
4. Test: kill -9 giữa batch rồi chạy lại → không lệch; bản ghi trùng; abort.

**P2.3 — Migration + bar_edge (1 ngày)**
1. Tạo `scripts/migrate_spool_bars_to_bar_store.py`:
   - đọc spool `mode=ro` + `query_only`, chạy ngoài giờ cao điểm, batch nhỏ để không
     giữ snapshot WAL lâu;
   - ghi các dòng BAR với `kafka_offset=NULL`;
   - báo cáo count/hash theo key.
2. Materializer bắt đầu từ đầu retention Kafka (6h). Upsert idempotent sẽ điền
   `kafka_offset` cho các dòng gần đây.
3. bar_edge: đổi `canonical_cache_path` và truy vấn `_durable_final_bar_opens` sang
   BarStore; identity chuyển từ `cache_identity.cache_id` sang `store_identity.store_id`
   (`stable_bar_edge.py:102-109,922,969-989`).
4. Tie-out cả 140 key: count và hash của `(open_time, OHLCV, revision)` bằng nhau trên cửa
   sổ chung.

**P2.4 — `KafkaNativeQueryBackend` (1,5 ngày)**
1. Backend mới cạnh backend cũ trong `qdl/runtime/stable_source.py`:
   - latest/snapshot/MARK/BOOK_SNAPSHOT lấy từ LatestView;
   - BAR latest/warmup/history lấy từ BarStore;
   - cấp cursor v3.
2. MARK cho alpha và execution đọc thẳng LatestView; bỏ HTTP sang Stream.
   `qdl/reference/local_mark_index.py` chuyển sang LatestView.
3. Test:
   - matrix read-plane 132 ô;
   - `verify_bar_handoff` của alpha: watermark warmup = watermark view;
   - batch strict 1/8/16/32/50.

**P2.5 — Hot/cold (1 ngày)**
1. Warmup BAR render từ cột, không decode protobuf từng dòng.
2. Giữ lane lạnh có trần và huỷ được.
3. Đo lại. Chỉ gỡ duty-cycle (`qdl/query/cold_work.py`) khi số đo cho thấy không cần.

**P2.6 — Checkpoint và khởi động lại (0,5 ngày):** đo RTO khi có checkpoint, và hành vi
`NOT_READY` khi không có checkpoint.

**P2.7 — Tuỳ chọn: bỏ header raw** (`rust/qdl-kafka/src/lib.rs:836-839`)
1. Chỉ làm khi đã kiểm dependency. Sau P4, không còn reader nào của header này (projector
   và ingest bị gỡ).
2. Đo bytes/s và CPU broker trước và sau.
3. Đây là thay đổi rust_core nên **cần owner duyệt riêng** vì đụng producer.

**P2.8 — Shadow Query (1 ngày)**
1. `query_v3_1/2` trên mạng test với alias `qdl-v2-query` (có trong SAN).
2. So parity tại **cùng watermark** với Query cũ, oracle là Kafka.
3. Diễn tập backup và restore BarStore.

#### Phase 3 — Cutover và nghiệm thu

**P3.1 — Packet cutover (ghi vào Plan trước khi chạy)**
- Role nhận alias: `stream_v3_1/2` và `query_v3_1/2`.
- Digest image của từng role.
- Script `scripts/kafka_native_alias_cutover.py` làm các bước sau:
  1. Preflight:
     - 4 replica mới `READY`;
     - lag < 1 s;
     - BarStore cách head < 5 s;
     - baseline TS 60/60;
     - `python3 -B scripts/preflight_env.py` phía TS **không cần**, vì không recreate TS.
  2. Gỡ **4 alias cũ trước**: `docker network disconnect executor_network` cho
     stream_v2_active, stream_v2_passive, query_v2_1, query_v2_2. Container cũ **vẫn
     chạy** trên các mạng khác.
  3. Gắn 4 alias mới: `docker network connect --alias qdl-v2-stream-a executor_network
     stream_v3_1`, tương tự cho `-b` và `qdl-v2-query`. Mục tiêu tổng thời gian < 5 s (F2).
  4. Kiểm lại: DNS mỗi alias chỉ trả địa chỉ của container mới.
- Rollback: script ngược lại. Token mới không hợp lệ ở stack cũ, nên rollback cũng gây
  **một lần** `SNAPSHOT_REPLACED` mỗi slice. Đây là hành vi đã được xử lý.
- **Không đụng:** image/env TS và alpha, offset Kafka, V1, projector, stream writer cũ
  (vẫn chạy để phục vụ rollback).
- **Compose:** đổi alias trong `docker-compose.v2-stable.yml` **trong cùng commit**, để
  một lần `compose up` sau này không gắn lại alias cũ. Đồng thời ghi rõ vận hành bằng
  `docker start`, không dùng `compose up` (CLAUDE.md §1).

**P3.2 — Chạy cutover rồi quan sát ≥2h**, có một cửa sổ thị trường sôi động:
- TS 60/60 (trừ DOGE QUOTE đã miễn);
- số `SNAPSHOT_REPLACED` bằng số slice rồi dừng;
- fallback V1 = 0;
- lag từng partition.

**P3.3 — Driver đích stage 20 → 35 → 50** với budget đã đóng băng, báo cáo đủ 4 đại
lượng theo định nghĩa owner (§15.2). Mỗi stage chỉ chạy khi stage trước PASS.

**P3.4 — Burst:** một cửa sổ thị trường thật cộng test đuổi. Lag có trần và tự hồi; không
OOM.

**P3.5 — Diễn tập rollback** (đổi alias về cũ rồi đổi lại), đo thời gian và ảnh hưởng
tới TS.

**P3.6 — Tài nguyên:** đo CPU V2 dùng thật **sau khi dừng thử projector và stream writer
cũ trong một cửa sổ**. Cổng thoát: ≤5,0 vCPU (F6).
- Nếu cần stack cũ cho rollback, cửa sổ này chỉ tính cho các role mới.
- Tổng cuối được tính ở P4.

#### Phase 4 — Gỡ bỏ và phát hành (xong trước ~2026-11-10, F3)

**P4.1** 72h sau cổng thoát P3:
1. Dừng projector ×6, stream_v2 ×2, query_v2 ×2 (`docker stop`, chưa `rm`).
2. Sao chép spool read-only vào vùng lưu 7 ngày.
3. Ghi tổng cap trước và sau.

**P4.2** Gỡ code, **mỗi nhóm một commit**, sau khi grep không còn caller:
- HTTP ingest (`stable_ingest.py`);
- lease Stream (`lease.py` — phần gateway);
- projector + Redis projection (`stable_projector.py`, `projection/stable.py`);
- phần tick của spool;
- `consumer_checkpoints`;
- execution MARK HTTP;
- unit `qdl-v2-stable-boot-recovery` và `rebuild_v2_stable_projection_cache.py` (không
  còn cache identity).

**P4.3** Viết ADR-0007 (Kafka-native, thay ADR-0006); rút gọn runbook; cập nhật CLAUDE.md
§1 và coupling #4 (stable_redis không còn giữ cache identity).

**P4.4** Chỉnh cap theo số đo, bù theo R1.29. Xoá image không còn tham chiếu **theo
digest**, giữ active + một bản rollback. Xoá volume/mạng test. Ghi disk trước và sau.

**P4.5** Phát hành v2.2.0: PR feature → dev → main; receipt digest/source/config; ghi
ledger.

<a id="opus-owner-questions"></a>
### 15.4 Câu hỏi cho owner (để Astra hợp nhất)

| # | Câu hỏi | Khuyến nghị Opus | Khuyến nghị Astra (§13) |
|---|---|---|---|
| Q1 | Hướng C? Dừng A? | Có / có | Có / có |
| Q2 | Gateway Stream: Python có luật chuyển Rust, hay Rust ngay? | Python + luật P1.1 | Rust-first |
| Q3 | Ngoại lệ R1.29 cho shadow: thêm tối đa ~2,5 cap CPU (stream_v3 ×1–2, query_v3 ×2, bar_materializer), **tối đa đến hết P3**, kèm điều kiện dừng §14.6? | Đồng ý, có thời hạn | (§13.7: không double-run không kiểm soát) |
| Q4 | Bỏ Redis latest projection (F1)? | Có | Materializer Rust → Redis |
| Q5 | Cutover bằng alias, đổi Stream + Query cùng lúc (F2)? | Có | Route theo manifest/config |
| Q6 | Bỏ header raw (P2.7, đụng rust_core)? | Tuỳ chọn, duyệt riêng | Tuỳ chọn |
| Q7 | Mốc hoàn tất trước khi chứng chỉ hết hạn (2026-11-20)? | P4 xong trước ~11-10 | — |
| Q8 | Phạm vi runtime P1 (§14.9 mục 3) + mạng test tạm? | Duyệt | — |

**Cam kết của Opus:** sau khi Astra gửi bản thống nhất (đã có ý kiến owner), Opus làm
**đúng** bản đó. Nếu trong lúc làm có số đo mâu thuẫn với bản thống nhất, Opus dừng tại
WI đó, báo số đo và xin quyết định, **không tự đổi hướng**.

---

<a id="opus-invariant-check"></a>
## 16. Opus: đối chiếu với mục tiêu và 42 invariant của Unified Plan

> **Ngày:** 2026-09-23. Đối chiếu §14–15 với §2 "Program-Wide Rules And Invariants" của
> `DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md` (dòng 25–177) và các mục tiêu owner nêu:
> kết nối ổn định, latency thấp nhất, tiết kiệm tài nguyên, dữ liệu chính xác tuyệt đối,
> không nhầm lẫn, bền.
> **Kết luận ngắn:**
> - Hướng C **đúng mục tiêu** và **gần invariant hơn** thiết kế hiện tại. Invariant 37
>   viết gần như nguyên văn lời giải: *"broker-native cursor/barrier"*.
> - Nhưng **chưa đủ**: cần thêm 11 bổ sung (§16.2) vào bản thống nhất.
> - Có 4 giới hạn mà 4 phase này **không giải được** (§16.3). Phải ghi rõ, không được coi
>   là đã đạt.

### 16.1 Đối chiếu theo mục tiêu

| Mục tiêu | Hiện tại | Sau hướng C (§14–15) | Còn thiếu → bổ sung |
|---|---|---|---|
| **Kết nối ổn định** | Lease đổi chủ làm đóng mọi stream; TS mất 1–2,5 phút; backlog khi burst | Không lease; replica độc lập; SDK failover nhiều target; không còn phễu tạo backlog | Độ lệch đọc giữa 2 replica Query (A9); diễn tập DR (A10) |
| **Latency thấp nhất** | Projector chờ batch 0,1 s + HTTP + fsync + SQLite trước khi fan-out; snapshot đọc SQLite | Bỏ chặng chờ batch, HTTP và fsync; snapshot đọc RAM; warmup BAR đọc cột | Đo 4 đại lượng của owner **trước** (baseline) và **sau**, cùng workload (A11) |
| **Tài nguyên** | ≈7,8 vCPU dùng thật; 6 projector làm việc không ai đọc (F1) | Mục tiêu ≤5,0 vCPU (R1.29) | Là cổng thoát bắt buộc, chưa phải kết quả |
| **Chính xác tuyệt đối** | Nhiều lớp kiểm tra trùng nhau; token thiếu một phần invariant 29 | Kiểm một lần ở rust_core; oracle là Kafka đã commit | Token v3 đủ invariant 29 (A1); revision BAR append-only (A2) |
| **Không nhầm lẫn** | Chung một file nên chung watermark | Offset Kafka tất định; test từ chối sai venue/symbol | Coalesce theo vòng đời (A6); độ lệch đọc giữa replica (A9) |
| **Bền** | Kafka RF3 + SQLite trên một máy | Kafka là nguồn duy nhất cho realtime; kho BAR có backup | Đường dựng lại BAR từ provider phải được diễn tập (A3); giới hạn một host (§16.3) |

### 16.2 Mười một bổ sung bắt buộc cho bản thống nhất

| ID | Invariant | Bổ sung | Phase |
|---|---|---|---|
| A1 | **29** (cursor gắn đủ hợp đồng phục hồi) | Token hiện tại (`qdl/replay/handoff.py:87-96`) **thiếu** environment, requirement digest, schema major, source-policy revision, catalog revision. Token v3 phải có đủ: environment, consumer, requirement digest, stream/partition_key, watermark, schema major, plan epoch, source-policy revision, catalog revision, topic identity, expiry. Lệch bất kỳ trường nào → từ chối tất định. **Đây là nâng cấp so với hiện tại**, không chỉ giữ nguyên | P1.2 |
| A2 | **35** (sửa đổi là sự kiện append-only) | BarStore không được "upsert đè". Dùng bảng `bar_revisions` append-only (mỗi revision kèm event_id và revision bị thay thế) + `bars_current` (bản mới nhất). Warmup đọc `bars_current`; audit đọc `bar_revisions` | P2.1 |
| A3 | **32** (bền nhân bản trước khi làm authority) | BarStore là **kho dẫn xuất**, không phải authority. Authority là canonical Kafka (6h) + provider (nguồn gốc, dựng lại qua bar_edge → raw → canonical). Phải **diễn tập dựng lại** một interval từ provider qua đường chuẩn, cộng với restore từ backup. Nếu owner muốn bản sao nhân bản thật: topic `md.bars.v2` compacted theo `(binding, open_time)`, do materializer ghi bằng transaction (xem Q9) | P2.3, P2.8 |
| A4 | **33** (sink có fencing) | Chỉ được có **một** bar_materializer: giữ `fcntl.flock` độc quyền trên file khoá và ghi epoch vào `store_identity`; instance thứ hai thất bại ngay. Zombie của rust_core đã bị Kafka transaction fencing chặn (`transactional.id`) | P2.2 |
| A5 | **28** (readiness là đo được) | Readiness của replica = đủ 6 partition + lag < 1 s + checkpoint đã nạp + khoá ký cursor + catalog + source policy + auth/JWKS + (Query) BarStore cách head < 5 s. Thiếu bất kỳ cái nào → không `READY`. Readiness **theo từng slice** | P1.5, P2.4 |
| A6 | **27, 5** (giao theo vòng đời, không mất im lặng) | Key book (snapshot, delta, reset) là **lossless** trong ring và queue, không coalesce. BAR final/revision không bao giờ bị bản in-progress đè. Chỉ BBO/ticker và bar in-progress được coalesce theo key vòng đời | P1.3, P1.5 |
| A7 | **2** + ranh giới tương thích | Bỏ key/pubsub tương thích V1 trên `stable_redis` phải có **bản ghi sunset có quản lý** (bằng chứng F1: không consumer nào chạm tới được), không xoá âm thầm. Redis V1 (`redis_marketdata`) và `/v1` **không đụng** | P4.2 |
| A8 | **16** (chuyển consumer theo manifest; không để ownership lẫn lộn) | Đổi alias là chuyển tất cả consumer cùng lúc. Để có bằng chứng canary **trước** cutover: chạy driver stage 20 và 35 **trên mạng shadow** (driver là consumer logic độc lập), rồi mới cutover. Stage 50 chạy sau cutover. Owner chấp nhận cutover toàn phần bằng một manifest version (Q10) | P2.8 → P3 |
| A9 | "Không nhầm lẫn" | Hai replica Query có LatestView riêng nên có thể lệch nhau vài ms. Response luôn kèm watermark. HTTP của SDK giữ kết nối nên phần lớn dính một replica. **Đo độ lệch p99** và đặt trần. BAR đọc từ một BarStore chung nên hai replica cho kết quả giống hệt (giữ đúng `verify_bar_handoff` của alpha) | P2.8 |
| A10 | **39** (DR trước khi phụ thuộc thực thi) | Diễn tập trong phạm vi test: xoay khoá cursor; mất checkpoint LatestView; restore BarStore; mất một replica; mất một broker (Kafka test, **không kill broker production**) | P2, P3 |
| A11 | **36** + rule owner 4 đại lượng | Đo baseline 4 đại lượng trên stack hiện tại **trước P3**, rồi đo lại sau cutover với cùng workload. Budget máy chấm đã đóng băng; PASS chỉ khi mọi tiêu chí đạt | P3.0 (mới) |

### 16.3 Giới hạn mà 4 phase này không giải được (phải ghi rõ)

1. **Một host duy nhất.** Ba broker Kafka và mọi state cùng một máy, cùng một đĩa. RF3
   chống được lỗi process/broker, **không chống mất host/đĩa**. Invariant 32 ("real
   deployment topology") và 39 (DR) ở mức production vẫn là quyết định hạ tầng. **V1
   fallback phải giữ.**
2. **Retention canonical 7–30 ngày theo Guide (dòng 1016–1026) không khả thi trên đĩa
   này.** 1,18 GB/h/broker (A4) × 168 h ≈ 198 GB/broker × 3 ≈ **595 GB**, trong khi đĩa
   trống 159 GB. Bỏ header raw (P2.7) giảm được một phần, chưa đo, vẫn không đủ.
   Consumer hiện không cần lịch sử tick, nên giữ 6h là hợp lý. Quyết định thuộc owner.
3. **Chứng chỉ hết hạn 2026-11-20; CA không xoay dần được** (coupling #5). Là mốc cứng
   cho P4.
4. **Chứng nhận production đa venue (DNSE/Spot/Deribit)** nằm ngoài phạm vi (§13.11), giữ
   nguyên trạng.

### 16.4 Câu hỏi bổ sung cho owner

| # | Câu hỏi | Khuyến nghị Opus |
|---|---|---|
| Q9 | Độ bền BAR: (i) BarStore + backup + dựng lại từ provider đã diễn tập, hay (ii) thêm topic `md.bars.v2` compacted, nhân bản RF3, ghi bằng transaction? | **(i) trong 4 phase** vì nhanh hơn và đủ invariant 32 khi BarStore chỉ là kho dẫn xuất. (ii) để sau nếu owner muốn bản sao nhân bản |
| Q10 | Chấp nhận cutover toàn phần bằng một manifest version, với canary là driver stage 20/35 trên mạng shadow (A8)? | Có |
| Q11 | Giữ retention canonical 6h (§16.3-2)? | Có, đến khi có hạ tầng đĩa/host khác |

---

<a id="opus-rust-first-refined"></a>
## 17. Opus: refine theo mục tiêu V2 Rust-first (thay thế một phần §14–16)

> **Ngày:** 2026-09-23. **Lý do:** owner xác nhận core V2 là **Rust-first**. Đọc lại Guide:
> - §6.2: Rust sở hữu *Kafka producer/consumer, Redis projector, replay engine,
>   high-throughput gRPC stream gateway*.
> - §6.3: không FFI từng message; Rust và Python là process riêng, nói chuyện qua Protobuf
>   + log bền.
> - §20.1 liệt kê các role: `qdl-stream-gateway` (Rust), `qdl-projector-redis` (Rust),
>   `qdl-api` (Python).
> - §5: *Redis Projector → API*.
> - §7.3: Redis giữ latest snapshot và warmup cache, dựng lại được từ log.
>
> Các quyết định D2–D4 ở §14 (gateway Python, LatestView trong Python, BarStore SQLite)
> **đi ngược mục tiêu này**. Chúng là lối tắt theo code Python hiện có, không phải kiến
> trúc chuẩn.
> **Kiểm trong lượt này (chỉ đọc):** `Cargo.lock` đã có `rdkafka 0.39`, `redis 0.25`,
> `prost 0.13`, `tokio 1.48`, `hyper 1.11`, `rustls 0.23`, `ring 0.17`, `serde_json`,
> `base64`. **Chưa có** `tonic`, `h2`, `serde_yaml`. `qdl-kafka` đã có
> `TransactionalKafkaBridge` và các fenced sink; `qdl-realtime-core` đã dùng Redis +
> Lua `Script`. Build Rust dùng `Dockerfile.qdl-rust-runtime` (có sẵn).

### 17.1 Phần bị thay thế

| Mục cũ | Trạng thái | Thay bằng |
|---|---|---|
| §14.3 D2 (gateway Python, luật chuyển Rust) | **Thay thế** | R1: `qdl-stream-gateway` bằng Rust |
| §14.3 D3, §14.4.3 (LatestView trong Python, checkpoint SQLite) | **Thay thế** | R2: `qdl-projector` Rust → Redis + changelog compacted |
| §14.3 D4, §14.4.4 (BarStore SQLite, bar_materializer Python) | **Thay thế** | R3: bar trong projector Rust → Redis ZSET + `md.bars.v2` |
| §15.3 P1.1, P1.3–P1.5, P2.1–P2.6 | **Thay thế** | Work item ở §17.6 |
| §16.2 A2, A3, A4, A9 | **Bỏ (legacy của thiết kế Python/SQLite)** | Được giải bằng cơ chế Kafka/Rust ở §17.4 |
| §16.2 A5, A10 | **Viết lại** | §17.4 |
| §14.3 D1, D5–D9; §15.1 F1–F8; §16.2 A1, A6, A7, A8, A11; §16.3 | **Giữ** | — |

### 17.2 Kiến trúc đích (khớp Guide §5, §6, §20)

```text
Sàn ─► qdl-ingestor (Rust) ─► md.raw.realtime.v2 ─► rust_core (Rust, EOS) ─► md.canonical.v2
                                                                              │ (RF3, 6 partition)
        ┌─────────────────────────────────────────────────────────────────────┼──────────────────┐
        ▼                                                                     ▼                  │
 qdl-stream-gateway ×2 (Rust, tonic)                          qdl-projector ×1..2 (Rust)         │
  - assign 6 partition, read_committed, không commit          - consumer group, read_committed   │
  - ring theo key + index subscriber                          - stage A (EOS): canonical →       │
  - replay engine (reader riêng, có trần)                        md.latest.v2 (compacted)         │
  - JWT + manifest + entitlement + freshness                     md.bars.v2   (compacted)         │
  - cursor HMAC (khớp byte với Python)                           + offset, cùng một transaction   │
  - Subscribe only, không lease                               - stage B: apply Redis (Lua CAS    │
  alias qdl-v2-stream-a / -b                                     theo offset); khởi động = phát  │
                                                                 lại changelog vào Redis         │
                                                                         │                       │
                                                                         ▼                       │
                                                               stable_redis (cache, dựng lại được)
                                                                         │
                                                                         ▼
                                                qdl-api ×2 (Python, không state, không consumer Kafka)
                                                  - snapshot/latest/MARK: Redis GET/MGET
                                                  - BAR warmup/history: Redis ZRANGEBYSCORE
                                                  - cấp cursor HMAC từ offset lưu trong Redis
                                                  alias qdl-v2-query
bar_edge (Python, giữ nguyên thuật toán) ─► md.raw → canonical (không đổi); xác nhận bền: đọc Redis
```

**SQLite biến mất khỏi đường dữ liệu.** Đây chính là thủ tục sunset của ADR-0006, cuối
cùng được thực hiện.

### 17.3 Các quyết định refine (thay D2–D4, bổ sung D10–D12)

| ID | Quyết định | Vì sao chuẩn nhất / tối ưu nhất |
|---|---|---|
| **R1** | `qdl-stream-gateway` Rust (tonic), chỉ có `Subscribe` + health; 2 replica độc lập, không lease | Guide §6.2/§20.1. Consumer chỉ gọi `Subscribe` (§15.2), nên phạm vi port nhỏ: JWT (verify bằng `ring`, đã có trong lock), manifest/catalog, matching + freshness, codec HMAC. Chi phí mỗi lượt giao (dựng protobuf + ký token) không bị GIL giới hạn |
| **R2** | `qdl-projector` Rust: **stage A** EOS canonical → `md.latest.v2` (compacted, key = `partition_key`); **stage B** apply Redis bằng Lua CAS (`offset mới > offset lưu`) | Pattern chuẩn của Kafka (changelog + sink idempotent). Dùng lại `TransactionalKafkaBridge` (`rust/qdl-kafka/src/lib.rs:826-898`). Giải cả 3 vấn đề cũ cùng lúc: feed ít cập nhật vẫn có latest (changelog không hết hạn); Redis mất thì dựng lại trong vài giây; zombie bị fencing bằng `transactional.id` (invariant 33). **Không còn cache identity, không còn `ProjectionCacheMismatch`** |
| **R3** | Bar trong cùng projector: `md.bars.v2` compacted, key = `binding|open_time|revision` (append-only theo revision); Redis `ZSET bars:{binding}` (score = open_time, trần 10k + headroom) | Lịch sử BAR được nhân bản RF3 (invariant 32 đạt tự nhiên). Revision là append-only vì nằm trong key (invariant 35), compaction chỉ xoá bản trùng. Warmup = một lệnh `ZRANGEBYSCORE`. Không file dùng chung giữa các process |
| **R4** | `qdl-api` Python **không có trạng thái**: chỉ đọc Redis, cấp cursor, auth REST | Guide §6.1/§20.1. Không consumer Kafka trong Python, nên hết tranh chấp GIL giữa hot và cold. Hai replica đọc cùng một Redis nên **không lệch** (A9 tự hết) |
| **R5** | Payload public dựng sẵn: projector Rust lưu vào Redis **JSON public đã render** (đúng schema V2 đóng) + các trường đã decode | Warmup 5.000 dòng chỉ là nối bytes, không dựng model, không decode protobuf. Cổng: **golden test khớp byte** với Python renderer hiện tại (`qdl/api_v2/router.py`). Nếu không đạt khớp byte thì phương án dự phòng là Python render từ các trường đã decode (Q12) |
| R6 (= D5) | Checkpoint consumer chỉ ở phía client (token) | Giữ |
| R7 (= D6 + A1) | Token v3 đủ invariant 29; codec có **golden vector dùng chung** Rust/Python trong `contracts/` | Query (Python) cấp token, gateway (Rust) nhận, nên phải khớp byte |
| R8 (= D7 + F2) | Cutover đổi 4 alias cùng lúc | Giữ |
| **R9** | Redis: namespace mới `qdl:v3:{env}`; `maxmemory` 128 MB → **768 MB [ước lượng: 140 key BAR × ≤10k × ~300 B + latest]**, `noeviction`, vẫn tmpfs, không persistence | Redis chỉ là cache dựng lại được từ changelog (Guide §7.3). Chấp nhận ephemeral vì dựng lại tự động |
| **R10** | Topic mới: `md.latest.v2`, `md.bars.v2`: compact, RF3, min ISR 2, 6 partition. `md.latest.v2` co-partition với canonical theo key. Cần ACL cho `qdl-projector-v3` (group, `transactional.id`, write) | Là thay đổi broker, **owner duyệt riêng** |
| **R11** | Giữ nguyên rust_core, ingestor, bar_edge (chỉ đổi chỗ xác nhận bền: Redis ZSET thay spool) | Không đụng producer trong 4 phase: giảm blast radius; đường ghi hiện tại đã đúng kiểu Kafka-native |
| **R12** | *Để sau, ngoài 4 phase:* "Initial low-latency mode" của Guide §11.4 (ingestor ghi raw + canonical trong một transaction, bỏ một hop) | Giảm latency thật nhưng đụng producer, blast radius lớn. Chỉ làm khi số đo sau P3 cho thấy cần |

### 17.4 Tính đúng, độ bền, readiness theo thiết kế mới

- **Thứ tự và không mất:**
  - Gateway đọc partition liên tục, replay → live trong một task tokio: đăng ký +
    chốt boundary atomic, rồi pending buffer có trần (§14.4.2).
  - Lossless tràn → `RESOURCE_EXHAUSTED` / `RECOVERY_REQUIRED` typed (invariant 5).
  - Book và BAR final không bao giờ coalesce (A6, invariant 27).
- **EOS và idempotency:**
  - Stage A: Kafka transaction.
  - Stage B: CAS theo `(generation, offset)`. Crash giữa chừng thì phát lại, không ghi
    đè lùi.
  - Hai projector cùng partition: bên cũ bị fencing bởi Kafka.
  - Thay A4 (flock) bằng cơ chế chuẩn của Kafka.
- **Độ bền (invariant 32):**
  - Realtime: canonical RF3 (6h).
  - Latest: `md.latest.v2` RF3, không hết hạn theo thời gian.
  - BAR: `md.bars.v2` RF3.
  - Redis là cache: mất thì stage B phát lại changelog.
  - Thay A3; không còn "bản duy nhất trên SQLite".
- **Readiness (invariant 28; Guide §24):**
  - Gateway: broker reachable + đủ 6 partition + lag < 1 s + khoá cursor + catalog + JWKS.
  - Projector: đã được assign + lag < ngưỡng + Redis generation đã dựng xong.
  - API: Redis đạt + generation khớp + heartbeat projector còn tươi.
  - Readiness **theo từng slice** dựa vào latest + timestamp nguồn + trạng thái gap.
- **DR (thay A10):**
  - Diễn tập: mất Redis (dựng lại từ changelog, đo RTO); mất một gateway; mất projector
    (group rebalance); xoay khoá cursor; mất broker trong **Kafka test**.
- **Độ lệch replica (A9):** không còn, vì API đọc một Redis.
- **Giới hạn giữ nguyên (§16.3):**
  - một host;
  - retention canonical 6h;
  - chứng chỉ hết hạn 2026-11-20;
  - DNSE/Spot ngoài phạm vi.

### 17.5 Tài nguyên mục tiêu **[ước lượng; cổng thoát = đo thật ≤5,0 vCPU theo R1.29]**

| Role | Cap đề xuất | Thay cho |
|---|---|---|
| qdl-stream-gateway ×2 (Rust) | 0,5 CPU / 256 MiB mỗi cái | stream_v2 ×2 (2,0 + 2,0) |
| qdl-projector ×1 (+1 dự phòng khi cần) (Rust) | 0,5 CPU / 256 MiB | projector ×6 (6 × 1,0) |
| qdl-api ×2 (Python, không state) | 0,75–1,0 CPU / 512 MiB | query_v2 ×2 (1,5 + 1,5) |
| stable_redis | 0,5 CPU / 768 MiB | 0,5 / 160 MiB |
| **Tổng cap các role này** | **≈3,5–4,0** | **≈13,5** |

### 17.6 Bốn phase theo kiến trúc refine (work item chính)

**P1 — `qdl-stream-gateway` Rust, shadow. [ước lượng] 8–12 ngày**
- **P1.0:** baseline (đo baseline 4 đại lượng theo A11); sửa lỗi `subscriber_count`;
  kiểm ACL `assign`; **đề xuất dependency mới** (`tonic`, `h2`, `tower`, `serde_yaml`
  hoặc chuyển manifest sang JSON lúc build) qua `cargo deny check`. Owner duyệt
  dependency.
- **P1.1 — lát dọc đầu tiên (3–4 ngày):** crate `rust/qdl-stream-gateway` (binary
  `qdl-stream-gateway`), tonic server dùng TLS hiện có (`stable_tls/stream`, SAN có
  alias), nhận `Subscribe` cho **một** feed (TRADE) với JWT + manifest thật, cursor HMAC
  khớp golden vector Python, đọc Kafka live. SDK Python thật subscribe được qua mạng
  shadow. **Đo sớm:** CPU / 1.000 lượt giao, p99 latency thêm.
- **P1.2:** token v3 (invariant 29) ở cả Python issuer và Rust verifier;
  `contracts/cursor/golden/*.json`.
- **P1.3:** ring theo key (byte + thời gian; BAR theo số lượng), index subscriber, queue
  có trần, coalesce theo vòng đời.
- **P1.4:** replay engine (consumer riêng, trần 200k bản ghi / 64 MiB / 2 s, tối đa 4
  cùng lúc, huỷ được).
- **P1.5:** đủ mọi feed stream của manifest TS + alpha; freshness filter; lỗi typed khớp
  mapping của SDK (`qdl_sdk/transport.py:503-526`).
- **P1.6–P1.8:** shadow (mạng test, alias trong SAN), oracle Kafka
  (`scripts/kafka_native_stream_parity.py`), kill replica, đuổi ≥3.000/s. Cổng thoát như
  §14.5.

**P2 — `qdl-projector` Rust + `qdl-api` đọc Redis, shadow. [ước lượng] 7–10 ngày;
bắt đầu song song sau P1.2**
- **P2.0:** tạo topic `md.latest.v2`, `md.bars.v2` + ACL (R10, owner duyệt).
- **P2.1:** crate `rust/qdl-projector`:
  - stage A (dùng lại `TransactionalKafkaBridge`) và stage B (Lua CAS);
  - schema Redis `qdl:v3:{env}:latest:{partition_key}`, `…:bars:{binding}` (ZSET),
    `…:barmeta:{binding}`, `…:projector:generation`, `…:projector:heartbeat`.
- **P2.2:** render JSON public trong Rust + **golden test khớp byte** với Python cho mọi
  feed/payload (R5).
- **P2.3:** migrate BAR: xuất các dòng BAR từ spool (`mode=ro`) thành bản ghi
  `md.bars.v2` qua một producer có transaction (script một lần, đối chiếu count/hash
  theo key), rồi projector dựng Redis.
  - bar_edge `_durable_final_bar_opens` đọc Redis ZSET
    (`stable_bar_edge.py:969-989`); identity chuyển sang projector generation.
- **P2.4:** `qdl-api` backend Redis: snapshot/latest/MARK/BOOK_SNAPSHOT = GET/MGET;
  warmup/history = ZRANGEBYSCORE + nối JSON dựng sẵn; cấp cursor v3. Không còn lane
  lạnh/duty-cycle, **chỉ gỡ sau khi đo**.
- **P2.5:** diễn tập: xoá Redis namespace test → dựng lại từ changelog (đo RTO); kill
  projector giữa transaction; zombie.
- **P2.6:** shadow `qdl-api` ×2 trên mạng test; matrix 132/132; parity tại cùng watermark
  với oracle Kafka; driver stage 20/35 trên mạng shadow (canary A8).

**P3 — Cutover + nghiệm thu. [ước lượng] 3–4 ngày** (như §15.3, với F2: đổi 4 alias cùng
lúc)
- Baseline đã đo ở P1.0.
- Stage 50 + TS; burst thật.
- Diễn tập rollback.
- Cổng ≤5,0 vCPU.

**P4 — Gỡ bỏ + phát hành. [ước lượng] 2–3 ngày** (như §15.3)
- Gỡ thêm: toàn bộ `qdl/transport/sqlite_spool.py` khỏi đường dữ liệu, projector Python,
  `stable_ingest`, lease Stream, Python gRPC stream service.
- ADR-0007 ghi rằng **ADR-0006 đã sunset**.
- Release v2.2.0 trước ~2026-11-10.

**Tổng [ước lượng]:**
- Tuần tự: 20–29 ngày làm việc.
- Chạy P1 và P2 song song: **15–20 ngày**. Hai agent có thể chia P1 (gateway) và P2
  (projector + API), mỗi bên một crate/module riêng.
- Dài hơn hướng Python ở §14, nhưng là **đích cuối**, không phải bước trung gian phải
  làm lại.

### 17.7 Rủi ro riêng của Rust-first

| Rủi ro | Giảm thiểu |
|---|---|
| Dependency mới (tonic, h2, tower) | `cargo deny`, pin version, owner duyệt ở P1.0 |
| Token/JSON Rust lệch byte với Python | Golden vector dùng chung trong `contracts/`, test chạy ở cả hai ngôn ngữ trong CI |
| Port auth/manifest sai quyền | Test từ chối chéo venue/symbol/consumer; chạy song song với verifier Python ở shadow, so từng quyết định |
| Thời gian build Rust (image lớn) | Dùng lại builder stage có cache; build chỉ khi crate đổi (rule 3b) |
| Redis 768 MiB trên host | Đo thật khi shadow; trần theo số đo |

### 17.8 Câu hỏi cho owner (thay Q2–Q4, thêm Q12–Q14)

| # | Câu hỏi | Khuyến nghị |
|---|---|---|
| Q2′ | Gateway Rust (R1)? | Có (theo mục tiêu V2) |
| Q4′ | Projector Rust + changelog compacted + Redis cho latest/BAR (R2, R3); bỏ SQLite hoàn toàn? | Có |
| Q12 | JSON public dựng sẵn trong Rust (R5), với golden khớp byte; dự phòng là Python render? | Có |
| Q13 | Duyệt dependency Rust mới và topic/ACL mới (R10)? | Duyệt ở P1.0 / P2.0 |
| Q14 | Key/pubsub tương thích V1 **không** port sang projector Rust (F1: không ai tới được), ghi sunset có quản lý (A7)? | Có |

---

<a id="kn-v220-approved-guide"></a>
## 18. OWNER-APPROVED - Kafka-Native Rust-First V2.2.0: Five-Phase Execution Guide

> [!IMPORTANT]
> **BẢN THỐNG NHẤT ĐỂ THỰC THI, KHÔNG TIẾP TỤC CHỌN LẠI GIỮA MỤC 13/14/17.**
> Owner duyệt phương án năm phase của Astra ngày 2026-09-23 và yêu cầu ghi guide
> cùng tracker. **Executor: Claude Opus 5.5. Reviewer: Astra sau mỗi phase.**
> Lượt soạn này chỉ thay tài liệu; không chạy phase, build, rollout, push/merge hay
> đổi authority. Khi owner giao bắt đầu một phase, thực hiện trọn scope dưới đây,
> ghi journal, test và bàn giao review; không sinh thêm phase vì một test fail.

### 18.1 Cách đọc, thứ tự ưu tiên và bản đồ năm phase

Đọc theo thứ tự: `/home/bobby/AGENTS.md` -> repo `AGENTS.md` ->
`/home/bobby/CLAUDE.md` -> phase KN hiện hành trong Unified Plan -> mục 18 này ->
code/config/runtime receipt và evidence liên quan. Các fact có ngày trong CLAUDE.md
phải kiểm lại tại execution time; không dùng path/image/revision lịch sử làm mặc định.

- **Guide này** sở hữu thiết kế chi tiết, invariants, work items, test/exit và rollback.
- **[Unified Plan](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#kn-v220-plan)** sở hữu
  status, việc đã làm, command/test thực chạy, findings/fixes, evidence và review receipt.
- **[Guide kiến trúc gốc](quant-data-layer-fund-grade-upgrade-architecture.md)** vẫn
  sở hữu nguyên tắc canonical/quality/compatibility; mục 18 cụ thể hóa migration này.
- **[Certification ledger](../CERTIFICATION_LEDGER.md)** giúp kế thừa evidence đúng
  source/image/config. Dòng chứng nhận cũ không tự chứng nhận backend mới.
- Sửa mâu thuẫn bằng explicit decision trong journal, không trộn hai phương án cũ.

| Phase | Mục tiêu | Guide | Tracker |
|---|---|---|---|
| KN-1 | Contract, baseline và lát dọc Rust có số đo | [18.8](#kn-guide-phase-1) | [KN-1](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#kn-plan-phase-1) |
| KN-2 | Rust Stream và replay, bỏ single-writer bottleneck | [18.9](#kn-guide-phase-2) | [KN-2](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#kn-plan-phase-2) |
| KN-3 | Rust materialization, BAR migration và bounded recovery | [18.10](#kn-guide-phase-3) | [KN-3](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#kn-plan-phase-3) |
| KN-4 | Query/SDK và toàn read plane, hot/cold isolation | [18.11](#kn-guide-phase-4) | [KN-4](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#kn-plan-phase-4) |
| KN-5 | Tải 50 alpha + TS, cutover, retirement và release | [18.12](#kn-guide-phase-5) | [KN-5](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#kn-plan-phase-5) |

Đây là **năm phase thay thế bốn phase đề xuất**, không cộng thành chín phase. Không
renumber hay xóa evidence của các chương trình trước. Capacity closure v2.1.1 còn
chưa đạt được kế thừa thành target của KN-5, không được đánh dấu pass hồi tố. Sau khi
train này bắt đầu, không song song tối ưu vô hạn writer cũ; chỉ làm safety fix hẹp có
phạm vi và regression cụ thể nếu cần giữ runtime đang chạy an toàn.

<a id="kn-decisions-and-scope"></a>
### 18.2 Các quyết định đã thống nhất và phạm vi được giữ

| ID | Quyết định hiện hành | Thay thế/giới hạn đề xuất cũ |
|---|---|---|
| KD01 | Rust Stream/replay/projector; Python API/SDK/control/history orchestration | Bỏ Python-first gateway và Python Kafka LatestView của mục 14-15 |
| KD02 | Stream đọc committed canonical trực tiếp, không chờ Redis/SQLite/HTTP writer | Bỏ lease cho read fan-out, không bỏ producer/sink fencing |
| KD03 | Projector Rust hai stage: durable state topics, rồi Redis cache idempotent | Kafka EOS không tự bảo vệ Redis; sink generation/CAS vẫn bắt buộc |
| KD04 | Latest key theo logical product/lifecycle; giữ source coordinates | Không compact snapshot/delta chung một key như physical key hiện tại |
| KD05 | BAR revision append-only trong history window đã cam kết; có expiry/tombstone | Key có revision không được tăng vô hạn vì "đã bật compaction" |
| KD06 | Python Query đọc cache thật, không tự consume Kafka | Không dựng thêm 2 consumer canonical bằng Python; snapshot vẫn có consistency proof |
| KD07 | Giữ cả Subscribe, Replay, GetSnapshot, GetFeedStatus và REST hiện hành | "Ít caller" không cho phép trả UNIMPLEMENTED thay implementation public đang có |
| KD08 | Cursor v3 đầy đủ claim, signed bytes golden Rust/Python | Canonical offset khác changelog offset; không tự đặt số token từ cache position |
| KD09 | Chuyển Query + Stream theo một routing revision, có drain/reconnect | Một script đổi alias không được coi là atomic handoff |
| KD10 | Market cache riêng process với Redis quota/provider-admission | Prefix/DB number không cô lập memory. Thêm đúng một cache role có lý do, không theo symbol |
| KD11 | Render trước phần immutable khi có lợi; freshness/auth/eligibility lúc read | Không cache nguyên response chứa token, request identity hoặc kết luận freshness |
| KD12 | Tổng steady-state Data Layer <=5.0 vCPU theo R1.29; resource claims phải đo | 768 MiB cache/256 MiB ring chỉ là giả định cũ, không cấu hình mặc định đã chứng minh |
| KD13 | Giữ ingestor/canonical business logic, Kafka partition map và provider quotas | Không gộp raw+canonical producer hop hay bỏ raw header trong scope này |
| KD14 | Một feature train, một bộ harness chính, năm exit; review theo tested slice/phase | Không tạo phase, image, container hoặc binding ceremony cho mỗi retry |

**Trong scope:** backend Stream/replay; projector/state-topic schemas và exact ACL;
market cache; Query/SDK compatibility hẹp; BAR readback/migration; measured resource
budgets; no-order consumers; paired read-plane rollout; retire old read path và release.
`stable_redis` hiện giữ control/quota/admission được giữ nguyên khi tạo market cache.
Không tự flush/recreate nó để thử khả năng rebuild của cache mới.

**Ngoài scope:** TS risk/order/domain upgrade, signal/sizing alpha, giao dịch sandbox/live,
DNSE/Spot/Deribit production activation mới, thay canonical domain math, raw retention
reset, manual offset jump, Kafka partition-count change, Flink/Kubernetes/lakehouse,
thêm ngôn ngữ, multi-host/regional DR hoặc thay toàn bộ lịch sử nguồn.
VN capability phải còn đi qua contract chung; route đang deferred không được đổi nhãn
certified nhờ test Binance/OKX. Bỏ publisher V1-compat không có reader chỉ sau inventory
và sunset record; không đụng Redis V1 hay endpoint `/v1` đang phục vụ consumer.

**Ranh giới phê duyệt:** thiết kế và năm phase đã được owner duyệt. Đây không phải
approval xóa production data hoặc recreate một digest chưa tồn tại. Trước runtime
action, điền packet cụ thể ngay trong phase: role, digest, env/mount/config revision,
topic/group/ACL/prefix, thời lượng, budget và rollback. Tái sử dụng approval đã cấp
đúng phạm vi; không hỏi lại cho retry/test không đổi blast radius. Nếu vượt boundary
hoặc cần quyền phá hủy thì dừng đúng thao tác đó và báo rõ, không dừng mọi việc source
không bị chặn. Push/merge/release vẫn theo quyền owner cấp và workflow remote.

<a id="kn-architecture-and-reuse"></a>
### 18.3 Kiến trúc đích, ownership và code tái sử dụng

```text
Approved provider -> existing ingestor -> Kafka raw -> existing Rust canonical core
                                                       -> Kafka canonical (committed)
                                                            |
                   +----------------------------------------+-------------------+
                   |                                                            |
          Rust Stream replicas x2                                    Rust projector
          indexed fan-out / bounded ring                        A: canonical -> state topics
          separate bounded replay readers                       (transaction + input offsets)
          existing public gRPC surface                           B: state topics -> market cache
                   |                                             (fenced idempotent apply)
                   |                                                            |
                   +---------------- SDK / consumer ------------ Python Query x2

Existing control Redis: quota/admission only. New market Redis: rebuildable data cache.
BAR edge still writes provider-authentic raw; only durable readback backend changes.
```

| Boundary | Reuse trước khi viết mới | Việc thực sự thay |
|---|---|---|
| Canonical domain | `rust/qdl-core`, `qdl-venue-core`, `qdl-realtime-core`, existing golden/provider tests | Không port lại math/identity/sequence chỉ vì đổi backend |
| Kafka | `rust/qdl-kafka`, committed source và transactional bridge | State topic layout/ownership; source coordinates được giữ trong projection records |
| Stream | `qdl/stream/grpc_service.py`, generated proto, SDK transport và error map làm oracle | Crate `rust/qdl-stream-gateway` nếu chưa có tương đương; native auth/fan-out/replay |
| Projector | Existing Rust Redis/Lua patterns, `qdl/projection/stable.py` semantics | Native projector, logical state keys, bounded rebuild/retention |
| Query | Existing router/service/schema, provider/reference wrappers và interval owner | Backend đọc cache mới; không rewrite public API/framework |
| BAR | `stable_bar_edge.py`, `final_bar_watermark.py`, governed calendar/repair | Cache readback adapter, history migration receipt và coverage checks |
| Bench/test | `phase3_consumer_load_acceptance.py`, existing budget/matrix, `report_feed_latency_quantities.py` | Thêm mode/typed evidence còn thiếu, không tạo engine hoặc 5 harness cạnh tranh |

Native gRPC vẫn phải verify identity và scope trong application. Không dựa vào mTLS
network membership thay authorization. Query cấp cursor, Stream kiểm; manifest/catalog
JSON/proto canonical được compile một lần và hash, không phát minh parser YAML khác
semantics. Dùng JOSE/JWT library được duyệt, pinned/audited; không tự lắp JWT từ `ring`.
Test RS256/ES256, alg allowlist, issuer/audience, kid-subject binding, exp/nbf/lifetime,
rotation/revocation và shared quota. Không đưa secret/private key vào fixtures hoặc Git.

Hai Stream replica đọc đủ partition độc lập; group riêng hoặc manual assignment phải
được chọn/test rõ. Hai replica cùng group chia partition không đáp ứng mọi slice trên
mỗi target. Projector có ownership theo partition, số replica được đo; không đổi thành
một global lock. Stage A và B có thể cùng image/role với task pools hữu hạn, không cần
service riêng cho từng topic/stage/symbol. Quota Redis là dependency thực của auth,
không được nói Stream độc lập mọi Redis khi auth vẫn cần shared quota.

<a id="kn-contracts-and-correctness"></a>
### 18.4 Contract dữ liệu, durability và consistency bắt buộc

#### 18.4.1 Product identity và delivery policy

- `LogicalProductKey` gồm environment, venue/market, instrument UID, logical feed,
  interval/session semantics, depth/qualifier khi contract yêu cầu; encoding/version
  được freeze ở KN-1. Physical Kafka key không thay thế product identity.
- BOOK snapshot/delta/reset có thể chung physical partition để giữ thứ tự, nhưng latest
  state phải phân biệt loại; không lấy một delta làm snapshot khi rebuild compacted log.
- TRADE, book lifecycle, final BAR/revision và quality/authority transition lossless
  theo invariant 27. Chỉ coalesce lifecycle đã được contract cho phép; không port mù
  `LATEST_STATE_FEEDS` cũ khi nó mâu thuẫn invariant mới được chốt.
- State projection có thể giữ latest verified snapshot mà không lưu mọi delta trong
  cache, nhưng delta stream vẫn lấy canonical; snapshot phải có matching source sequence,
  generation và canonical watermark để nối đúng delta.
- Quiet market, session liveness, event recency, gap và execution eligibility là những
  trường riêng. Không refresh timestamp gốc để che stale; global heartbeat projector
  không chứng minh provider/slice đang live.

#### 18.4.2 State topics và BAR retention

- Dùng `md.latest.v2` và `md.bars.v2` nếu topic inventory xác nhận tên/schema chưa xung
  đột. RF/min-ISR/read-committed/ACL phải theo approved broker topology; không rename
  hoặc overwrite topic cũ để tránh migration. Kafka cùng host không phải multi-host HA.
- Latest topic key là logical state identity; quality/generation/provenance đủ để dựng
  lại view. Source generation reset hoặc disable/delist phải có state transition/tombstone
  có thể replay, không chỉ xóa Redis key cục bộ.
- BAR durable key phân biệt binding/open-time/revision (hoặc event identity tương đương
  đã golden). Giữ append-only revision fact, reference tới event bị thay thế. Current
  cache một row/open-time; equal revision nhưng khác nội dung là conflict, không last-write-wins.
- Chốt `retained_opens`, revision/audit retention, headroom và tombstone lifetime theo
  binding/capability ở KN-1. Không tự nâng từ 10k sang infinite; không tự giảm maxlen đã
  công bố. Expiry controller là task bounded trong projector, không service mới.
- BAR compacted topic có unique revision key **không tự nhỏ đi**. Expiry phải publish
  tombstone cho key ngoài cửa sổ đã duyệt; retention floor/watermark phải durable.
  Không dùng một TTL thời gian ngắn chung cho 1m/1w khiến mất history contract.
- Tombstone retention đủ cho approved offline/rebuild horizon; vượt horizon phải rebuild
  fresh generation, không overlay cache cũ đã bỏ lỡ deletes. Không silently trim revisions
  còn thuộc audit window; hết disk budget thì typed pressure/stop promotion, không giả pass.
- Chỉ materialize hot/history depth đã có demand; provider wrapper vẫn phục vụ bounded
  on-demand history ngoài cache theo policy. Request lớn không tự tăng memory cap/authority.

#### 18.4.3 Hai hệ offset và sink fencing

- Mỗi state record giữ original canonical `(topic identity, partition, offset)`, domain
  event ID/revision, source generation, lineage/hash và materializer schema epoch.
  Changelog delivery coordinate là metadata riêng; không cấp public cursor từ nó.
- Stage A produce state + input offset trong Kafka transaction. Chỉ ACK/commit thành
  công mới được xem state durable. Không dùng Redis ACK thay durable publication.
- Stage B atomic apply kiểm generation/owner fence, source identity và domain revision;
  compare transport coordinates trong cùng topic/partition/epoch, không so số offset
  giữa topics/generations như một global counter. Cursor/watermark phải đúng view đã apply.
- Crash sau Redis write/trước checkpoint phải idempotent. Kafka producer fencing không
  chặn Redis client zombie: delayed write sau rebalance/generation swap phải bị sink từ chối.
- Marker/quality update không được làm mới giá cũ thành execution-ready; late BAR repair
  có offset cao không làm `latest_final_bar` lùi về open-time cũ.
- Lua/Redis không đổi u64 offset/epoch-nanosecond sang double mất precision. ZSET index
  dùng integer milliseconds hoặc encoding chính xác phù hợp, còn timestamp gốc giữ nguyên.
  Atomic payload + index + quality + checkpoint, không tách thành writes nhìn thấy nửa chừng.

#### 18.4.4 Snapshot, batch và rebuild

- Query đọc payload/quality/watermark/generation nhất quán từ một versioned view. Warmup
  nhiều page phải pin snapshot/generation hoặc detect/retry bounded nếu view đổi.
- Shared Redis giảm state duplication, **không** làm hai HTTP reads ở thời điểm khác
  nhau byte-identical. So parity tại cùng version; report progress/replica lag khi live.
- `require_all=True` không biến multi-product batch thành atomic global snapshot. Response
  phải giữ per-item watermark và all-or-nothing contract hiện có, không ghép lỗi thành empty OK.
- Rebuild cache: allocate staging generation -> capture readable committed boundaries ->
  restore logical state/retention floors -> tail đến boundary -> verify coverage/quality ->
  atomic publish ready generation. Cache epoch và public canonical generation khác nhau:
  cache rebuild không tự bắt mọi consumer reset nếu canonical recovery contract còn hợp lệ.
- Latest có priority trước cold BAR restore, nhưng readiness chỉ true cho slice thật đủ;
  không dùng một cờ global để giấu missing key. Feed yên lặng phải có durable latest,
  không hứa replay 15 phút là đủ. Missing snapshot/retention gap phải typed NOT_READY.

<a id="kn-cursor-security"></a>
### 18.5 Cursor v3, auth và replay-to-live

Token bind environment, authenticated consumer, normalized requirement digest,
schema major, stream/logical product, snapshot identity/watermark, topic identity,
physical partition, partition-plan epoch, source-policy/catalog revision và expiry.
Golden bytes áp dụng cho signing/canonical encoding; không đòi JSON property order ở
response không có yêu cầu byte identity. Decimal/unit/null/presence semantics phải exact.

- Token cũ không được diễn giải như Kafka token. Query + Stream cùng approved route
  generation; migration có typed expiry/resnapshot, tránh vòng snapshot cũ -> stream mới.
- Per-replica applied boundary khác source event time. Offset jump do filtering/transaction
  marker hợp lệ; source gap phải chứng minh riêng. Đọc `read_committed` đến readable
  boundary, không giả log-end là committed completeness.
- Đăng ký REPLAYING + chốt boundary atomic trong replica; pending buffer giữ events mới;
  replay bằng reader riêng -> merge dedup theo identity -> LIVE. Không seek live consumer.
- Replica chưa đạt cursor từ replica khác phải catch-up bounded hoặc trả retryable typed
  lag, không báo cursor expired giả để bỏ event. Ngoài retention mới resnapshot theo policy.
- Ring/replay cap theo bytes, time, record scans và concurrency; key thưa không scan vô hạn.
  Cap 64 MiB/2s/4 reader ở proposal cũ chỉ là starter hypothesis, KN-1/KN-2 phải đo.
- Overflow lossless trả typed recovery/backpressure, không drop oldest rồi healthy.
  Cancel/disconnect phải join/terminate task và release permit; request hết deadline
  không được để reader, buffer hoặc auth quota lease rò rỉ.
- ACK/checkpoint là client-owned đã có. Không thêm server checkpoint dùng chung identity;
  hai subscription cùng identity không được làm tiến độ của nhau nhảy lên.
- Negative auth/quota tests chạy qua gRPC/HTTP thật. Không tái sử dụng token/data/cursor
  consumer A cho B vì cache dùng chung; cache chỉ giữ immutable product data.

<a id="kn-resources-and-latency"></a>
### 18.6 Resource, latency và workload: cách đo thống nhất

**CPU:** <=5.0 vCPU steady-state tổng Data Layer phục vụ cuối cùng theo R1.29. Ghi rõ
service denominator gồm brokers, ingestors, canonical cores, BAR edge, stream, projector,
Query, control Redis và market cache; tách TS/alpha test compute, build và old rollback
stack. Không báo <=5 bằng cách bỏ Kafka khỏi tổng. Peak/burst/headroom report riêng.
Cap không phải usage. Mọi thay cap có before/after, total-cap ledger và evidence lợi ích.
Shadow exception phải hữu hạn theo role/thời lượng và host/TS stop condition đã ghi;
không tự vay CPU của TS để làm mới xanh.

**RAM:** market cache process riêng, `noeviction` không thay thế retention. Đo payload
và Redis overhead thật, reserve cho index/Lua/clients/rebuild headroom. Journal đã ghi
5.000-row response 10.3 MB; không sizing whole-public-JSON bằng 300 B/row. Không prefill
mọi interval 10k khi không có demand, cũng không âm thầm cắt depth hiện được cam kết.
Cold restore/expiry bounded để không chặn hot Redis reads; quota/admission Redis không
chịu full-memory của market history. Nếu staging rebuild cần gấp đôi RAM thì tính trước,
hoặc rebuild partition/slice có controlled unavailability; không chờ OOM mới phát hiện.

**Bốn đại lượng owner, không đổi định nghĩa giữa baseline và candidate:**

| Đại lượng | Đo từ đâu đến đâu | Bổ sung bắt buộc |
|---|---|---|
| Request latency | SDK trong container consumer gọi -> dữ liệu decode/validate xong, dùng được | cold/warm connection; endpoint, batch shape, result/rejection |
| Durable event age | age field theo source/component/BAR semantics trong response | không thay age bằng request duration; nêu timestamp nào |
| Delivery lag | provider event (BAR: close time) -> canonical publish/commit theo receipt | host receive và Kafka commit làm subspan, kiểm clock skew |
| End-to-end cache | provider event/BAR close -> state đã apply tại alpha/TS client | client CPU/queue tách khỏi server; không chỉ đo response arrival |

Session/gap/generation, reconnect RTO, materializer lag từng partition và correction
visibility report thêm. Quiet/feed eligibility giữ contract hiện hành; không bỏ rejected
samples khỏi denominator. BAR history dropout horizon không phải close-to-signal SLA.

**Profile đích:** 20 candle clients (BAR stream + QUOTE 1 Hz), 15 realtime clients
(TRADE + QUOTE streams, MARK 1 Hz), 10 grid/L2 clients (BAR + QUOTE + BOOK_DELTA,
snapshot khi bootstrap/recovery, MARK 1 Hz), 5 multi-symbol clients (hai QUOTE streams,
hai-item MARK batch 1 Hz, một reference/minute), cộng TS 60 route đã inventory.
Khoảng 90 alpha subscriptions và 50 hot HTTP requests/s **chưa tính TS**; đo cả item/s,
bytes/s và delivery fan-out. Logical clients phải có identity/quota đúng, không chia
quota nhỏ rồi gọi offered-load giảm là capacity pass. Lấy symbol/interval/feed/maxlen
từ manifest thật, không dựng target chỉ cho BTC.

Budget từ `config/v2/v211-target-acceptance-budget.json` và receipt owner hiện hành phải
được parse/đối chiếu ở KN-1, freeze thành candidate budget có hash. Không hardcode lại
ngưỡng khác ở từng script. Mốc đề xuất trước đây: hot QUOTE/TRADE p95/p99 100/250ms,
MARK 250/500ms, L2 300/750ms, BAR-latest 1000/2000ms chỉ áp operation tương ứng khi
khớp approved budget; không áp chúng cho whole 5k warmup hoặc provider cadence.
3.000 **canonical** events/s là challenge có real capture/event mix, không đồng nghĩa
3.000 raw frame/s hoặc synthetic tiny events. Replay tăng tốc không được giả là live
freshness. Stream direct canonical và Query qua changelog có latency budget riêng;
stage A transaction/cleaner cost mới phải đo, không tin Rust tự triệt tiêu overhead.

Reuse [`scripts/phase3_consumer_load_acceptance.py`](../scripts/phase3_consumer_load_acceptance.py),
[`scripts/report_feed_latency_quantities.py`](../scripts/report_feed_latency_quantities.py)
và [`scripts/measure_consumer_request_latency.py`](../scripts/measure_consumer_request_latency.py).
Chỉ bổ sung phần thiếu, cùng một evidence schema; không xây harness thành engine thứ hai.

<a id="kn-testing-and-review"></a>
### 18.7 Test strategy, journal và review contract cho Claude/Astra

1. Mỗi work item = coherent source/test slice, không phải phase con. Trước sửa ghi scope
   và affected evidence; sau pass commit kèm journal. Không để một commit khổng lồ cuối
   phase, cũng không commit mỗi assertion/finding. Đúng user identity, không AI trailer.
2. Fast tests trước runtime: unit/golden/property -> real Kafka/Redis isolated integration
   -> exact read/protocol matrix -> bounded load -> final consumer acceptance. C2 không
   dùng làm bug detector cho mỗi lần sửa; test fail phải chỉ binding/layer/code/watermark.
3. Reuse certified provider/domain evidence có pinned source/image/config không đổi;
   test lại read/replay/security/migration path bị thay. Không biến con số 132/299/140
   lịch sử thành denominator vĩnh viễn; inventory hash + exact counts được chốt ở KN-1.
4. Synthetic chỉ unit/fault fixtures có test provenance. Runtime/cert dùng provider thật
   hoặc durable capture hợp lệ; chaos Kafka/Redis chỉ namespace test, không giết broker
   production. Test không được direct venue-bypass read plane để ép thành công.
5. Real collaborator/transport tests bắt lỗi fake như property/method mismatch. Fault
   tests dùng kill/restart/timeout thực trong scope isolated khi cần; không chỉ assert
   tên hàm trong source. Required integration phải fail rõ nếu dependency thiếu, không
   `skip` lặng rồi ghi phase xanh.
6. Journal theo mẫu: `work item | SHA | command/test cases/counts | evidence path/hash |
   observed failure/root cause/fix | runtime mutations | cleanup | next`. Không lưu raw
   secrets/unbounded logs; evidence ngoài Git theo layout sẵn có.
7. Claude bàn giao `IMPLEMENTED_PENDING_ASTRA_REVIEW`, không tự ghi Astra PASS. Review
   dựa source + tests + real evidence, có thể rerun focused cases; không mặc định rerun
   toàn certified suite. Astra findings in-scope phải sửa/test trong phase đó.
8. Trạng thái: `READY/PENDING -> IN_PROGRESS -> IMPLEMENTED_PENDING_ASTRA_REVIEW ->
   REVIEW_CHANGES_REQUIRED hoặc ASTRA_REVIEW_PASS -> CLOSED`. Owner có thể đổi sequencing
   rõ ràng; không suy ra model tự approve bản thân hoặc certified runtime chưa test.
9. Sau mỗi phase báo domain behavior, cases pass/fail/skip, evidence chưa có, artifacts,
   canonical branch/SHA, active image/config và rollback inventory; cleanup done/deferred
   có lý do. Report consumer readiness khác release readiness, không dùng một chữ PASS
   cho mọi cấp.

<a id="kn-guide-phase-1"></a>
### 18.8 KN-1 - Contract, Baseline And Measured Rust Foundation

**Goal:** khóa những quyết định xuyên phase bằng spec/golden/prototype đo được, không
viết một bản framework mới hay kéo dài planning. **Owner:** Claude; **review:** Astra.
**Entry:** owner giao bắt đầu; đọc §18.1-18.7, tracker KN-1, source/evidence hiện hành.

| Work item | Implement / kiểm chứng cụ thể | Kết quả phải bàn giao |
|---|---|---|
| K1.1 Baseline | Inventory source/branch/images/config, RPC/HTTP/binding, manifest/JWT, TLS expiry/SAN, topics/ACL, quotas và retained history | Baseline receipt, known-failure list và evidence-reuse map; không ghi fact từ trí nhớ |
| K1.2 Safety regression | Sửa `subscriber_count` property bug bằng class gateway thật; không rollout Stream cũ mặc định | Test fail trên source cũ/pass patch; slice source-only |
| K1.3 Contracts | Freeze logical keys, cursor v3, source vs changelog coordinates, revision/conflict, cache rebuild epoch và read consistency | Shared vectors/spec trong nơi contracts hiện có; không đổi public field shape |
| K1.4 Resource/retention | Đo serialized payload/index overhead, history demand, CPU denominator, broker read/transaction budget; freeze expiry/rebuild constraints | Machine-readable budget + sizing; không chốt 768 MiB bằng phỏng đoán |
| K1.5 Native vertical slice | Reuse Rust Kafka/protobuf; thêm approved tonic/JOSE deps; authenticated TRADE Subscribe qua SDK thật trên shadow | Real commit -> token -> native gateway -> SDK, Binance/OKX capture hoặc bounded live |
| K1.6 Test/release wiring | Native/Python vectors, isolated integrations và required CI jobs; planned namespace/ports/identities/cleanup | Không skip required gates; một harness path; affected-test command map |

**Tests bắt buộc:**
- K1-T01: decimal/null/unit/time/identity golden; same feed ở hai venue không cross-mix.
- K1-T02: cursor signature bytes, env/consumer/requirement/policy/catalog/schema mismatch,
  expiry, tamper, old token typed reset; u64/nanosecond không mất precision.
- K1-T03: real JWT/TLS/manifest authority, wrong alg/kid/aud/issuer/expired identity và
  limit denial; không key/private credentials trong evidence.
- K1-T04: committed vs aborted Kafka records tới prototype; ACL least privilege, không
  permission reset/write trên canonical cho read-only clients.
- K1-T05: SDK thật qua container network decode/validate event; đo bốn đại lượng có
  source provenance, không dùng same-process timing làm consumer latency.
- K1-T06: bounded payload-memory benchmark dùng BAR/L2 thật; record bytes/event và
  fan-out, không so tiny trade với large snapshot để hứa throughput.
- K1-T07: real gateway property regression; classify existing suite errors by test ID,
  không tự bỏ chúng khỏi required gates nếu ảnh hưởng path mới.

**Exit:** contract vectors pass cả Python/Rust, minimal real vertical slice hoạt động;
inventory/budget/retention/compatibility artifacts có hash và rõ boundary; CPU/RAM số đo
tách actual vs estimate. Các con số chưa có không được gọi certified. Astra review
spec/source/prototype evidence trước P2/P3; một BTC smoke chỉ là prototype, không cert.
**Stop:** không rewrite full Query, không migrate data hoặc cutover TS/alpha.
**Rollback:** stop prototype đúng scope; production unchanged. **Cleanup:** --rm clients,
scratch/session artifacts và unused test image/cache; giữ builder/candidate cần phase
tiếp có tên và expiry, không broad prune. **Debt:** lỗi contract trong scope phải fix;
dependency/license/infra bất khả thi cần quyết định owner, không tự chuyển kiến trúc.

<a id="kn-guide-phase-2"></a>
### 18.9 KN-2 - Rust Stream, Replay And Public Streaming Compatibility

**Goal:** native realtime delivery/replay không singleton spool/lease, bounded và tương
thích public RPC. **Entry:** KN-1 Astra PASS; đọc §18.3-18.7 và tracker KN-2.

| Work item | Implement / kiểm chứng cụ thể | Kết quả phải bàn giao |
|---|---|---|
| K2.1 Native service | Crate/native entrypoint, TLS/JOSE/manifest/quota; generated service cho bốn public RPC | Subscribe/Replay native; GetSnapshot/status qua read-view interface đúng contract |
| K2.2 Live delivery | Independent committed readers, indexed fan-out, shared immutable bytes, byte/item bounds | Không global writer lock hoặc Python HTTP per event |
| K2.3 Replay | Token v3, ring, replay pool riêng, committed barrier, pending merge và cancel/join | Replay không seek live reader; progress/cap metrics |
| K2.4 Lifecycle | Explicit lossless/coalescible rules, quality/control frames, quiet/session/gap/reset | Same decision oracle với domain policy, không dựa riêng tuổi tick |
| K2.5 Shadow/oracle | Một replica thường trực bounded trong test window; replica hai cho failover; Kafka oracle theo coordinate | Full demanded stream matrix và real SDK receipt; không mutate old groups |

Read-view interface của snapshot/status được test tại KN-2 với contract fixture/adapter
đã xác định; integration cache mới thuộc KN-4, được báo **chưa certified Query**. Không
expose route production `UNIMPLEMENTED`, không ngụy trang fixture thành real data.
Không mở thêm RPC/endpoint nội bộ nếu interface hiện có dùng lại được.

**Tests bắt buộc:**
- K2-T01: read_committed, abort/control-marker offsets, duplicate transport delivery,
  sparse logical key và source sequence gap độc lập với offset jump.
- K2-T02: replay ring hit/miss, expiry/scan cap, consumer cancellation, no live-reader
  seek; boundary interleaving property test và real Kafka integration.
- K2-T03: two subscribers chung identity nhưng ACK riêng; overlapping consumers,
  per-consumer quota fairness, auth revocation/rotation giữa stream.
- K2-T04: kill replica, reconnect target đi sau cursor, bounded catch-up, không false
  expiry/resnapshot loop; no unexplained missing/reordered application events.
- K2-T05: slow reader/pending overflow, bounded memory, no task/permit/socket leak;
  repeated connect/disconnect đưa resource count về baseline.
- K2-T06: BOOK snapshot/delta/reset và BAR final/revision không coalesce sai; quote
  coalescing chỉ scope được phép; negative cross-product/depth/interval cases.
- K2-T07: every public RPC schema/error/auth contract; snapshot/status dependency NOT_READY
  có nghĩa rõ; stream đủ scope Binance/OKX manifest, không chỉ prototype K1.
- K2-T08: canonical capture challenge + fan-out target, CPU/RSS/lag/queue per replica;
  giới hạn replay không được làm nóng pipeline chính nghẽn.

**Exit:** native Stream shadow đúng contract và oracle, reconnect/replay qua hai replica
pass, resource bounded; no loss/cross-mix vượt delivery policy; số đo native stage cost
và budget rõ. Không dùng ACK Kafka nội bộ làm ACK alpha đã xử lý. Astra review source
và all-RPC compatibility, không chỉ test count. **Stop:** chưa Query promotion, không
đụng producer authority/topology/offsets. **Rollback/cleanup:** stop shadow service/client,
giữ stream cũ; test topics/groups/network chỉ dỡ khi không còn phase sau cần và scope
cho phép. **Debt:** pending KN-4 integration là dependency khai báo, không gap Stream
implementation được lén chuyển đi.

**Astra KN-2 R2 entry clarification (2026-09-24, owner decisions):**
- Cursor v3 remains snapshot-then-replay. A real record at canonical offset 0
  is included in the first valid snapshot; the token names that applied offset
  and replay starts at 1. An empty/insufficient product view is DATA_NOT_READY,
  without a manufactured cursor. No genesis record, offset shifting or topic
  mutation is required. Contract section 1 and the KN-2 bootstrap regression
  freeze this behavior; KN-3/K3.2-K3.7 and KN-4/K4.3 prove persisted coverage
  and actual SDK handoff, as already planned. A before-first archive iterator
  is not the public v3 snapshot-handoff API.
- The 384 Subscribe + 32 Replay / 256 MiB profile is the measured KN-2
  profile, not a reduction of the program capacity target. The owner permits
  measured resources after optimization; the 1,024 + 32 candidate is budgeted
  at 307 MiB with a 384 MiB container planning envelope (77 MiB headroom).
  No runtime cap is changed here and 1,024 concurrency is not yet certified.
  Account for old-plus-new peak; reclaim old services only after safe cutover.
- Decode factor eight and transport/reserve allowances are empirical, not
  universal heap/OOM guarantees. Keep typed bounded admission/backpressure;
  remeasure new payload/depth profiles, not every already-passed unchanged test.
- DNSE/VN remains V1_PRIMARY by explicit owner decision, outside KN V2
  promotion. Preserve negative/expected-filtered results, never count them as
  event-positive delivery or claim dated-book freshness from them.
- Existing tracked capture/oracle CLI and `kn-native-integration` CI already
  cover the reusable KN-2 tests. Retain hashed historical orchestration as
  evidence; do not copy host paths/secrets into Git or block KN-3 on a wholesale
  1,458-line harness refactor. Future reusable operations belong in `scripts/`
  with explicit isolated inputs, using the existing CI job rather than a new
  duplicate job. Four production end-to-end latency quantities remain KN-4/5
  measurements, not inferred from captured-Kafka stage latency.
- Authoritative review/entry receipt:
  [Unified Plan KN-2 Astra R2](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#kn2-astra-review-r2).

<a id="kn-guide-phase-3"></a>
### 18.10 KN-3 - Rust Materialization, BAR Migration And Bounded Recovery

**Goal:** cache phục vụ được Query thật, dữ liệu bền và rebuild đúng, không RAM/disk tăng
vô hạn. **Entry:** KN-1 reviewed contracts và mặc định KN-2 Astra PASS. Chỉ làm source
song song K2/K3 khi owner phân công rõ modules; không hai agent sửa shared files ngầm.
Đọc §18.2-18.7 và tracker KN-3.

| Work item | Implement / kiểm chứng cụ thể | Kết quả phải bàn giao |
|---|---|---|
| K3.1 State topics/ACL | Exact topic identity, schema, partition routing, RF/ISR/compaction/retention, producer/group permissions | Packet/dry-run/idempotent provision; không sửa canonical topic |
| K3.2 Stage A | Transactional canonical -> logical latest/BAR revision + offsets; keep provenance | Source/change coordinates tách; no source timestamp rewrite |
| K3.3 Stage B | Native Redis CAS/fence, atomic payload/index/meta/checkpoint, bounded writes | Crash/replay/zombie safe; latest không bị stale write lùi |
| K3.4 Market cache | Separate process/config from quota/admission Redis; measured cap/noeviction/headroom | Memory-full không phá auth/provider admission; workload sizing receipt |
| K3.5 History/expiry | BAR current index + immutable revision facts; retention-floor/tombstones; bounded cleaner | Window depth đúng contract và disk slope đo được |
| K3.6 Migration/readback | Read-only old spool export, history tie-out/import with lineage; BAR edge readback adapter | Count/hash theo binding/open/revision; no invented canonical offset |
| K3.7 Rebuild/recovery | Fresh cache generation staging, load floors/latest/history, tail barrier và atomic readiness | Cold boot/restart/partial restore đúng RTO; no manual flush runbook phụ thuộc |

BAR import từ legacy khi canonical retention đã hết phải mang `legacy_import` provenance,
original event/hash/cursor nếu còn, và replay cutoff thật đã capture. Không tạo offset
giả hoặc publish lại toàn history thành tick realtime. Public handoff cursor gắn live
canonical boundary của snapshot coherent, không gán offset NULL/0 cho đủ schema. Migration
rerun phải idempotent và không overwrite correction mới hơn.

**Tests bắt buộc:**
- K3-T01: Stage A crash/abort/commit, restart/rebalance và duplicate input -> correct
  durable outputs/offsets; không checkpoint đi trước dữ liệu.
- K3-T02: Stage B crash trước/sau Redis apply, delayed zombie, generation switch,
  offsets không comparable; fence rejection và recovery đúng.
- K3-T03: shared physical book key nhưng latest snapshot không bị delta/reset xóa nhầm;
  quality/session state cùng generation; MARK/INDEX component clocks không bị gộp.
- K3-T04: late BAR repair, higher/lower/equal-conflicting revision, final vs in-progress,
  ZSET precision, current index uniqueness, calendar interval boundary.
- K3-T05: tombstone/retention floor, missed deletes, sparse/quiet latest, rebuild từ
  earliest committed changelog; new key chưa có -> typed NOT_READY, không default 0.
- K3-T06: empty cache, interrupted/staging rebuild, full-memory, cache outage;
  control Redis unaffected; ready generation publish atomic.
- K3-T07: migration toàn retained demanded BAR coverage, rerun/crash, count/hash/value
  parity; long intervals và insufficient provider history được nói đúng.
- K3-T08: lag/throughput/bytes/disk-cleaner/Redis memory với thực tế payload, latest
  reads không bị cold rebuild/expiry chiếm hết budget.

**Exit:** projector native và cache shadow đúng oracle; old history giữ coverage; full
rebuild có measured RTO, không fake authority; expiry policy được implement/test không
để sau; memory/disk bounded trong workload. Astra review transaction/fence/migration,
không accept "Kafka EOS nên Redis tự an toàn". **Stop:** chưa query/consumer cutover,
không delete old SQLite/volumes. **Rollback:** stop candidate projector/cache và BAR
readback candidate; source writers/control Redis giữ nguyên. **Cleanup:** exact test
namespace/topic/volume theo packet, không FLUSHDB shared. **Debt:** không còn in-scope
recovery/correctness thiếu implementation trước exit.

<a id="kn-guide-phase-4"></a>
### 18.11 KN-4 - Query, SDK And Full Read-Plane Compatibility

**Goal:** mọi endpoint trong declared scope được consume đúng, hot reads không chờ
warmup nặng, SDK không sửa strategy để thích nghi backend. **Entry:** KN-2/KN-3 Astra
PASS; đọc §18.3-18.7 và tracker KN-4.

| Work item | Implement / kiểm chứng cụ thể | Kết quả phải bàn giao |
|---|---|---|
| K4.1 Backend | Existing Python Query -> market-cache view; typed status/errors/cursors | Không Kafka reader Python; read-time eligibility/auth giữ đúng |
| K4.2 Render | Cache immutable typed/encoded fields; lightweight per-request envelope; bounded native/cold batch work | No duplicate full Pydantic walk trên hot loop; không cache stale verdict |
| K4.3 History | Warmup/history/strict batch + stable snapshot/handoff | 2.5k/5k/10k nơi contract cho phép, repairs không làm gap giả |
| K4.4 API surface | All existing REST/gRPC, diagnostics, reference wrappers, OpenAPI/SDK contracts | Không drop unused RPC; no INTERNAL_STREAM external limiter quay lại |
| K4.5 Consumers | SDK thật và TS/alpha read adapters qua shadow paired targets | Four classes no-order, cursor/reconnect/allowed fallback/BLOCKED |
| K4.6 Preflight/load | Fast matrix cả hai replica, targeted protocol + stages 20/35 trên shadow | Typed failures định vị được; không dùng C2 để dò bug |

Cold path không được tự đóng semaphore khi request timeout trong khi worker tiếp tục
chạy không kiểm soát. Cache read/batch Lua không scan toàn namespace; tránh một script
Redis dài gây latency cho mọi hot key. Stream snapshot/status read interfaces của KN-2
được bind cache thật tại đây, rồi test end-to-end cùng all-RPC surface.

**Tests bắt buộc:**
- K4-T01: exact HTTP/gRPC/SDK contract, schema closed, decimal/unit/null/missing,
  auth/quota/errors; toàn endpoint đã inventory, không chỉ route mới.
- K4-T02: strict batches 1/8/16/32/50, per-item identity/policy, cancellation/deadline,
  partial handling; replica parity ở cùng watermark và progress khi live.
- K4-T03: 2.500/5.000/10.000 warmup và maxlen thực, calendar/long interval,
  page consistency, finality/revision/history->stream; không giảm rows để pass.
- K4-T04: quote/trade/mark/book quiet vs disconnect/gap/reconnect/generation;
  delayed read làm giá hết hạn thì bị reject dù cached payload lúc tạo còn fresh.
- K4-T05: L2 snapshot/delta/resync, mark/index component age, native/derived reference
  lineage; read wrappers không tự trở thành execution authority.
- K4-T06: hot traffic cùng cold warmup/restore, queue saturation/fairness và canceled
  worker leak; quota Redis không bị market-cache memory pressure.
- K4-T07: SDK no-order consumers warmup/stream/ack/reconnect, V2-primary,
  allowed V1 fallback/return và BLOCKED đúng policy; no direct venue bypass.
- K4-T08: stages 20/35 shadow theo frozen workload + full two-replica fast matrix;
  endpoint latency đủ bốn đại lượng, no missing sample denominator.

**Exit:** all-scope affected read matrix, SDK scenarios và shadow 20/35 pass; mọi public
RPC giữ behavior; hot/cold budgets đạt; no auth/session/gap ambiguity chưa xử lý. Astra
review actual consumer evidence, không health-only. **Stop:** chưa certified 50 + TS,
chưa cutover all consumers hoặc release. **Rollback:** Query shadow/backend off, consumers
test stopped, old targets giữ nguyên. **Cleanup:** scoped read clients/network/state,
không để compose alpha test chạy nền. **Debt:** deferred venue capability đã kê khai
không mở rộng; lỗi public read path trong scope phải sửa trước close.

<a id="kn-owner-read-completion"></a>
#### Owner Read-Plane Completion Before KN-5 (2026-09-26)

Owner-approved completion of K4.3/K4.4/K4.5; five-phase architecture and rollout
boundaries unchanged. Astra implements source; Claude reads its journal before K5.1.

1. Universe warmup is one logical SDK batch with bounded HTTP chunks/response
   memory, declared interval/limit and native identity per item. Alpha uses
   this batch instead of single-read fanout. No unbounded workers or silent
   universe/row reduction. Listing/history bounds are explicit, never padded.
   Daily anchor/as-of cutoff matter; strict batch is not a globally atomic
   snapshot. Test sufficient history separately from honest short history.
2. Reuse validated warmup/cursor for stream handoff without a second full
   warmup. Preserve signed identity/generation, resnapshot, duplicate handling
   and ack after application. Pass-through has no invented durable cursor.
   Add thin alpha QUOTE subscription over existing gRPC, no venue bypass.
   MARKET uses bid/ask/L2 as reference, not candle/mark as a guaranteed fill;
   Risk remains admission owner.
3. Complete OKX contract analytics for OI history, long/short variants and
   taker flow through Python vendor edges and shared admission/pagination.
   Instrument/currency scope, contracts/base/USD, ratio population, sampling
   and history coverage must be explicit. Missing is not zero; no native OKX
   basis claim, new execution permission or fake replay. New sources reuse
   typed capability/schema/provider contracts, not services per symbol.
4. All latency durations are ms: caller-to-first-usable and caller-to-complete
   validated batch/window, queue/SDK/decode and source age separately. Report
   counts/bytes/failures per venue/symbol/feed/interval/limit/replica. Callback
   completion differs from actual TS Redis-write completion. Do not report
   successful-only timing or handler time as total consumer latency.
5. KN-5 freezes an actual universe profile alongside 60 TS routes: same-interval
   daily batch, 2500/5000 rows where available, bounded incremental windows and
   reconnect; reference coverage per venue; hot QUOTE/L2/TRADE/MARK concurrent
   with cold work. Reuse unchanged KN evidence; affected source/fast matrix
   precedes final 300s in KN-5. No additional phase train is introduced.

##### Concrete Consumer Contract And KN-5 Handoff

The public API remains parameterized; 60 TS bindings are not 60 distinct HTTP
paths and are not the universe capacity certificate. Keep these read classes:

| Consumer work | Shared API / facade | Correct interpretation |
|---|---|---|
| Universe daily signal initialization | `POST /v2/market-data/warmup:batch`; SDK `iter_warmup_batches`; alpha `warmup_batch` | One logical universe, sequential bounded chunks, native instrument identity, interval, requested rows and per-item outcome |
| Single history/latest closed bar | `GET /v2/market-data/{instrument_uid}/warmup`, `/history`, `/snapshot` | Final/revised bars, declared calendar, short history reported; never pad missing candles |
| Incremental indicator updates | gRPC `QueryStream/Subscribe`; SDK `warmup_then_stream(initial_warmup=...)` | Caller owns applied window; BAR-only reuse, signed cursor, generation, dedup and application-before-ack |
| MARKET/limit advisory prices | QUOTE snapshot / alpha `stream_quotes`, TRADE; L2 snapshot/delta when depth matters | Bid/ask and liquidity are references, not guaranteed execution price; no trade-as-quote substitution |
| Conditional trigger/margin context | MARK/INDEX typed snapshot plus declared trigger reference | Native component timestamps/lineage and execution eligibility; not bar-close substitutes |
| Liquidity/slippage/market impact | BOOK_SNAPSHOT, BOOK_DELTA; alpha `book_snapshot`, `stream_book_delta` | Sequence/generation/gap/resync and native depth, never disconnected cached book as execution input |
| Funding/OI/ratios/taker/basis/metadata | `POST /v2/market-data/reference:batch`; alpha `reference_batch` | Venue-specific capability, unit, population and cadence; reference input does not acquire execution authority or replay cursor |

Alpha `warmup_batch` defaults to **50 items / 10000 estimated rows** per HTTP
chunk, respecting current alpha item quotas. At limit2500 that is at most four
items/chunk; at5000 two; at10000 one. The SDK protocol permits100 items, not an
entitlement to exceed an identity's50. No additional per-symbol worker exists.
The compatibility result map retains the requested output; callers needing
bounded incremental universe processing use the SDK iterator and release each
chunk. A universe of350 x5000 retained strategy rows still consumes consumer
memory and must be measured, not hidden by clearing test scratch windows.

A caller-owned BAR `WarmupResponse` can seed `warmup_then_stream` or alpha
`stream_messages(initial_warmup=..., warmup_limit=<original limit>)`. The alpha
legacy dictionary does not contain a trusted signed cursor and must not be cast
into one. The last bar may be re-emitted; dedup by native identity, interval,
open time and revision. Only update the retained deque and ACK after successful
application. A historical pass-through window never invents replay continuity.
QUOTE messages from `stream_quotes` contain advisory `QuoteRead`; existing
`decode_message` passes it through. Trade/BAR legacy JSON payloads stay unchanged.
Risk re-reads eligible market context at order admission; this task does not
change MARKET, limit, OCO, sizing, order submission or strategy logic.

**Bounded universe measurement tool:** `scripts/benchmark_kn_universe.py`.
`--schema` prints the closed profile schema; `--manifest <profile.json>` is
inventory-only and opens no credentials/network. `--run-approved` performs
reads against explicit paired HTTPS/gRPC targets with mounted file credentials.
Run in one disposable consumer container, read-only source/credentials, bounded
CPU/RAM, existing candidate image, no Docker socket or order credentials. Do
not use synthetic unit fixtures as a real profile. The report contains no tokens
or row payloads. The existing KN endpoint matrix remains responsible for all
11 HTTP endpoints / four gRPC RPCs; this tool supplements, not replaces it.

Freeze per profile: catalog snapshot, venue/native symbol/UID, manifest+JWT
revision, interval, native daily anchor, `limit`, `maxlen`, item/row/stream quota,
source policy and paired replica targets. `as_of_ns` is a client-side validation
cutoff, NOT an atomic historical snapshot request. Stop/re-plan a run crossing
its next daily close; do not ignore a newer bar or claim chunks are simultaneous.
Listing age/retention can make2500 or5000 daily bars unavailable: record actual
rows, coverage and typed short-history result, never generate or silently claim
full-depth PASS. Test the long-row compute/memory case on sufficiently old
supported intervals as a separate profile when daily listing age is insufficient.

**Claude must include these in K5.1/K5.2/K5.6, not add a new phase:**
1. Freeze Data Layer + alpha commits, publish/pin the corresponding SDK candidate
   together, compile/seal actual admitted universe and reference demand with
   aligned JWT/manifest revisions. No five-liquid manifest inference for350.
2. Run changed source/protocol matrix once, then real exact endpoint matrix on
   both KN replicas, true1d universe batch and2500/5000 window scenarios above.
   Source tests, provider-only smoke and historical shadow receipts are distinct.
3. Run hot QUOTE/TRADE/MARK/L2 alongside cold universe at stages to50 logical
   alpha plus real TS60. Report offered/admitted/usable/partial/failed/timed-out/
   not-attempted counts by binding, interval, rows and replica; missing metrics
   cannot disappear from denominators. No symbol-specific quiet exemptions.
4. Report **milliseconds** for caller-before-queue -> decoded/validated/applied
   first item and whole batch, queue/SDK/decode separately, actual consumer
   cache-write completion where instrumented, source component/event age,
   provider->canonical and event/bar-close->consumer usable. Callback timing
   is not Redis-write timing; Kafka CreateTime is not commit time. Small sample
   count cannot justify p99. Include errors and refused execution eligibility.
5. Optimize measured bottlenecks first. Preserve bounded executors/admission,
   disclose retained universe RAM and full-stack CPU/IO/lag. Increase a resource
   cap only with measured benefit and the approved resource envelope. One final
   300s acceptance follows green affected matrix; do not rerun C2 to debug.
6. Paired cutover, old-path retirement, remote feature->dev->main and release
   provenance/cleanup stay exactly K5.3-K5.6. Keep V1 only for allowed fallback;
   preserve named old-V2 rollback for V2-only products. No alpha execution here.

Future venues/metrics extend native instrument identity, typed units/capability
and adapter/normalizer tests, then explicit demand and existing admission/cache
lanes. Realtime authoritative products additionally require canonical lineage,
watermark, replay and quality semantics. A REST-only metric uses reference
batch with native sampling/retention; it is not forced through a fake WebSocket
or treated as an executable quote. DNSE remains its existing V1 route; options
and unsupported products require their own approved provider contracts.

<a id="kn-d48-daily-universe"></a>
#### D48 Daily Market-Cap Universe (owner decision, 2026-09-26)

**Rule.** One crypto universe for daily bars: at most **300** bases, ranked by
CoinGecko market cap, among bases that trade as USDT-margined perpetuals on
**both** Binance USD-M (`TRADING`) and OKX Swap (`live`, linear), listed at
least 30 days on each, excluding stable/fiat-pegged/wrapped assets. An
incumbent stays while eligible and ranked <= 330; newcomers enter by rank while
fewer than 300 members. Only **1d** bars are admitted for universe symbols and
consumers read them with the batch warmup (`warmup:batch`, SDK
`iter_warmup_batches`, alpha `warmup_batch`).

**Files.** `config/v2/universes/crypto-top300-1d.json` (signed membership:
rank, market cap, Binance symbol, OKX instId, listing times, rules, source
hashes) and `crypto-top300-1d.changes.jsonl` (one line per revision:
`effective_date`, added with rank/`SIGNED`/`ENTERED_BY_RANK`, removed with
`NOT_TRADING_BINANCE`/`NOT_LIVE_OKX`/`TOO_NEW`/`NO_MARKET_CAP`/`EXCLUDED_ASSET`/
`RANK_BELOW_BAND`). Revision 1 (2026-09-26) signed **255** members: only 265
bases were listed on both venues; 1 stablecoin, 5 without a market cap and 4
listed < 30 days were excluded. The cap binds when more bases qualify.

**Backtest contract.** Use point-in-time membership, never today's list:
`scripts/kn_universe_top300.py::members_as_of(changes, "YYYY-MM-DD")` replays
the change log; the same `select()` rules apply to historical inputs. A symbol
removed on day D is out from D (no survivor bias); a short listing yields
honest short history (never padded).

**Re-run.** `python -B scripts/kn_universe_top300.py` (dry run) then `--apply`
(new revision only when membership changes; one Binance, one OKX and six
market-cap GETs with 429 back-off), then `--sync --apply` (BAR 1d demand rows
for bar-only symbols and verbatim metadata rows of new symbols appended to
the captures), `phase115c_materialize_active_native_bars.py --apply` (catalog,
acquisition, scope, routing; universe symbols stay 1d and on the REST final
lane - one poll per daily boundary through provider admission) and
`phase533_materialize_alpha_runtime_entitlements.py --apply` (alpha manifests;
bump consumers' JWT manifest revision in the same rollout). A catalog revision
strands the bar-edge checkpoint (C.19): it ships only in an approved rollout.

<a id="kn5-astra-correctness-review-handoff"></a>
#### Astra Implementation Handoff To Claude - 2026-09-26

The owner assigned Astra implementation and Claude review for this correction.
Read the [main journal](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#kn5-astra-correctness-handoff)
and [source/evidence receipt](evidence/KN5_ASTRA_PREDEPLOY_REVIEW.md) before K5.2.
This addendum does not grant deployment or execution authority and does not add
another phase or change the five-phase architecture.

- D48 owns only fingerprinted demand rows in the checked-in ownership ledger.
  Missing ownership means no deletion authority. Independently edited/borrowed
  rows and non-universe intervals survive. Native symbol changes are logged
  even if base-asset membership does not change.
- OKX alpha source manifest is revision 14, with 35 reference requirements,
  including five each of daily OI history, long/short and taker flow. Binance
  stays revision 14/35 reference requirements. Reference/L2 source revision 5;
  release routing 26 and primary routing 8. Regenerate the sealed candidate and
  JWT revisions together; do not infer runtime entitlement from these YAMLs.
  New OKX statistics must retain the existing shared Rust provider-admission
  policy/bucket wiring; offline authorization tests are not a live-provider run.
- Preserve 10,000-row entitlement. `whole_cache_d48` supersedes the old whole-
  cache fit: full-cap peak is approximately 5.87 GB **extrapolated**, excluding
  unmeasured RSS/client-buffer/overlap reserves. Measure existing execution plus
  daily universe plus staging, not 500 daily products in isolation. A lower
  retention is a contract change, not a hidden internal optimization.
- Timestamp/open/bucket integrity and cooperative cancellation/deadline checks
  apply to the KN Redis BAR reader. Wire/state encoding is unchanged; no new
  Rust writer, replica or service is needed for these source corrections.
- Diagnostic 409/206/503 is fail-closed but incomplete; it does not certify a
  completed scan. The KN HTTP matrix now records FAIL for incomplete/malformed
  diagnostics. Do not loosen its budget to turn the old result green.
- Report every declared binding x replica, attempted/usable/refused counts,
  queue + SDK + application + completed cache-write latency separately, all in
  ms. Withhold p99 below 100 and p95 below 20 samples. Raw event/component age
  and session health do not replace execution eligibility. Quiet TRADE remains
  blocked as an execution price; MARKET references explicit fresh QUOTE/L2 and
  Risk revalidation, never a fabricated latest trade.
- K5.2 must still measure actual whole-universe completion and TS Redis-write
  boundary under load. These are not proven by source tests or the old callback
  receipt. After Claude review, the existing K5.3 packet remains the deployment
  boundary. No rerun of unrelated certified Rust/Kafka evidence is requested.

<a id="kn5-astra-postpatch-review"></a>
#### KN-5 Astra Implementation Handoff - 2026-09-26

Owner assigned Astra implementation and Claude independent review for the
predeployment corrections. Read the [new authentic acceptance receipt](evidence/KN5_ASTRA_PREDEPLOY_REVIEW.md#post-patch-acceptance)
and [main journal](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#kn5-astra-acceptance-completion)
instead of treating the historical source-only receipt as completion.

Architecture is unchanged: Kafka durable authority, shared native projector/cache,
Python public compatibility, bounded client-side history chunks. Do not reintroduce
SQLite spool, symbol-workers, synthetic candles or execution eligibility based only
on heartbeat. Rust BAR diagnostic summaries are derived versioned indexes atomically
written with rows/checkpoint. Fresh KN caches create them naturally; an older cache
keeps the exact fallback scanner until approved rebuild, not a fake empty diagnostic.
The one-time summary initialization in the receipt was ISOLATED TEST STATE ONLY.

SDK aggregate history chunk defaults to2,500 rows without truncating a single5k/10k
request. Read-only cold admission may retry at most3times on typed retryable
RATE_LIMITED, respecting bounded Retry-After. No quality/auth retry, cross-feed
substitution, hidden fallback or replayed order. Consumer wall time includes queue,
retry, decode and application; attempt counts remain visible. A mixed partial batch
is not silently merged. Real provider gaps remain explicit unavailable history.

The50-alpha gate must report startup retries separately from steady successful reads,
state replay separately from usable execution prices, and actual TS Redis ACK/readback
separately from SDK response. A deliberate slow-reader fault must prove non-executable
replay plus strict current recovery, not accept5s-old quotes against a2s price bound.
Whole current-cache memory is now measured; full-retention growth is still explicitly
projected. The final rollout must account for production ingress, old/new overlap,
rebuild and buffers, not quote the read-plane-onlyCPU as a full-stack result.

Continue existing K5.3-K5.6 only after Claude review and owner deployment decision.
Build release images from the reviewed source; use affected smoke for changed SDK
or packaging rather than repeat unrelated provider/domain certifications. No new
phase, production authority grant, blanket resource increase or remote merge is
implied by the isolated acceptance.

<a id="kn-guide-phase-5"></a>
### 18.12 KN-5 - Target Load, Paired Cutover, Retirement And V2.2.0 Release

**Goal:** dùng chính thức backend mới trong scope đã duyệt, đạt tải đích, retire đường
cũ và phát hành có provenance; không thêm phase sau để làm phần đã hứa. **Entry:**
KN-1..KN-4 Astra PASS, fast preflight hiện hành xanh và runtime packet đầy đủ. Đọc
§18.6-18.7, §18.13-18.14, tracker KN-5 và rules release/cleanup.

| Work item | Implement / kiểm chứng cụ thể | Kết quả phải bàn giao |
|---|---|---|
| K5.1 Freeze | Exact candidates/config/manifest/targets/TLS, all endpoints và workload, baseline/rollback matrix | Không đo rồi đổi budget; no generic "prior image" |
| K5.2 Target load | Stages tới 50 logical alpha + TS 60 routes, real/capture burst, cold work/recovery | Bốn latency, per-partition lag, throughput/bytes và whole-stack resources |
| K5.3 Paired handoff | Versioned Query+Stream target pair, canary TS data consumer rồi representative no-order alpha | Bounded drain/reconnect, no mixed-generation loop, no order mutation |
| K5.4 Final acceptance | Fast/protocol matrix trước; một final 300s sau candidate đã ổn; rollback/return rehearsal | All counters và typed policy outcomes, receipt linked to exact artifacts |
| K5.5 Review/provenance | Astra release review, feature->dev CI->main release flow, immutable artifact attestation | Runtime tree/image/config certified traceable; affected smoke khi artifact đổi |
| K5.6 Retire/release | Old writer/projector/tick-spool off, ADR/runbook/update guides, cleanup, publish v2.2.0 | One active architecture + named bounded rollback, release notes/capacity envelope |

**Predeploy correction, 2026-09-27 (same KN-5 scope):** retire expired
research book bindings from source/acquisition/promotion/manifest together,
preserving historical metadata and never inferring a successor contract.
`/v2/data-quality/gaps` scans **retained windows**, not listing-to-now history.
KN reports missing demanded generations as `PARTIAL_RESULT`; optional
`include_coverage=true` exposes per-binding SCANNED/UNAVAILABLE/EXCLUDED,
retained bounds and explicit unassessed leading/trailing coverage. Disabled
acquisition is named, not silently counted as ready. A completed diagnostic
scan is not a full-history certificate. History request coverage/readiness and
provider discontinuity evidence remain mandatory. No fabricated repair bars.
Full-cap cache sizing must include both execution and universe, diagnostic
indexes, concurrent rebuild staging, buffers and RSS; test-only allocator
fill is not market-data or full-stack live acceptance. Runtime retirement and
paired rollback-return are performed only in K5.3/K5.6, not closed by docs.

**Tests bắt buộc:**
- K5-T01: 50-client offered/admitted/completed/error/timeout/in-flight accounting,
  TS route-by-route readiness/eligibility; không lấy client throttle làm zero miss.
- K5-T02: canonical burst và hot partition catch-up, headroom, OOM/throttle, full-stack
  <=5.0 steady-state vCPU; shadow/rollback overhead tách nhưng không giấu.
- K5-T03: paired handoff với persistent HTTP/gRPC channels, rolling disconnect,
  stale DNS/old token/new token, replica lag; no endless resnapshot/fallback loop.
- K5-T04: controlled rollback V2->old V2 và return; V1 chỉ product policy cho phép;
  source correctness/retained history/checkpoints không bị reset.
- K5-T05: cold restart/recovery relevant candidate, key rotation và isolated broker
  failure evidence còn đúng artifact; không kill broker production để cert.
- K5-T06: one final 300s actual no-order read acceptance toàn scope đã freeze;
  auth/quota/freshness/gap/reconnect/cursor + zero order/signal/sizing mutation.
- K5-T07: immutable image/config/CI release receipt, public compatibility, startup,
  cleanup inventory/disk before-after và absence of unintended restarts.

**Exit:** workload/correctness/resource/recovery gates pass, Astra review accepted;
production read routing coherent, old data path không ghi âm thầm; source/image/config
được chứng nhận đúng và release published qua workflow được duyệt. Nếu chưa được
publish quyền remote thì ghi `CERTIFIED_PENDING_PUBLICATION`, không nói đã release.
Không có gate tự sinh phải đợi 72h; retained rollback/background monitor có thời hạn,
owner kiểm định kỳ, không giữ agent session chạy chờ. 300s không được mô tả là multi-day
soak hoặc multi-host DR. **Stop:** không mở alpha execution hoặc TS upgrade tiếp theo.
**Debt:** in-scope failure block exit; single-host/declared retention/deferred venues
được ghi là external limits, không giả đã giải quyết bằng rename status.

#### SDK Cursor Persistence Closure (2026-09-27)

Async stream consumers use SDK2.0.5 `await session.acknowledge_async(event)`
**after** applying the corresponding batch to their own cache/state. The file
store bounds admitted operations to64 and serializes fsynced atomic writes off
the event loop; pending updates may share a transaction without a debounce
sleep. Duplicate retries still fsync. Acknowledgement is not completed before
durability, and reconnect/snapshot generation changes are fenced against it.
Cancellation/shutdown drains admitted I/O and closes the session transport;
file, rename and directory-fsync errors never become successful acknowledgements.
The synchronous acknowledgement API remains for compatibility, not the preferred
async high-throughput path. This SDK patch does not change feed freshness, quota,
Kafka/cache authority or execution eligibility. Runtime evidence must identify
the exact wheel hash as well as its version; unpublished candidate artifacts are
not interchangeable just because both say2.0.5.

<a id="kn-rollout-and-cleanup"></a>
### 18.13 Packet runtime, rollback và cleanup chuẩn hóa

Packet được journal trước action, gồm: exact service/container names + compose project;
source SHA/image digest; config/manifest/JWT/catalog/route revisions; ports/network/SAN;
groups/topics/ACL/Redis namespaces/mounts; allowed writes; duration/stop conditions;
rollback từng role và data assumptions. Không chạy bundle generator trên live bundle
vì có thể xoay keys/secrets (CLAUDE.md coupling 2); build từ immutable source, state ngoài Git.

- Chuyển **read route** không có nghĩa cấp canonical producer authority mới. Giữ owner/
  source-policy đã đúng; cursor backend change là migration contract được version hóa.
- Không xem Docker alias edits là transaction. Ưu tiên manifest/config pair và bounded
  consumer reconnect; nếu cần recreate TS, chỉ `market_data_service` đúng image hiện tại
  + read-target/config cần đổi, preflight env/mount theo CLAUDE.md; không upgrade image
  gateway/risk/executor/portfolio hay mặc định từ `.env` gây downgrade service khác.
- TLS SAN/expiry và JWT-manifest revision phải kiểm execution-time. Không tự đổi CA
  đồng loạt để thuận tên shadow; chọn target/cert path đúng. Deadline expiry lịch sử
  không phải chứng minh runtime hiện còn cùng certificate.
- Old V2 rollback cho L2/reference phải có state/config/replay horizon đủ, không chỉ
  giữ image. V1 không cover mọi product và không độc lập host failure khi cùng máy.
- Retire old code/runtime sau dependency inventory; giữ archive chỉ theo tên/hash/
  expiry đã duyệt. Stop container không đồng nghĩa có quyền xóa production volume.
  Không dời Kafka offset/flush shared Redis/delete SQLite để ép caught-up/green.
- Mỗi phase cleanup resources chính phase: --rm client; exact test topics/groups/prefixes;
  scratch ở session/evidence root có deadline, không `/tmp`/repo dump theo CLAUDE.md;
  image by digest unreferenced, BuildKit cache scoped. Giữ active và named rollback,
  candidate cần phase sau có lý do/expiry. No broad-prune collateral.
- Final report: canonical checkout/branch/SHA; active feature worktrees; released main
  tag; service->image/config map; artifact retention; disk before/after và restart check.
  Branch đã merge chỉ xóa sau code-preservation check; không xóa branch người khác còn code.

<a id="kn-review-handoff"></a>
### 18.14 Handoff review cho Astra và định nghĩa hoàn tất

Claude gửi một receipt mỗi phase, không dán toàn log:

```text
Phase / status / source SHA / affected files and line counts:
Approved scope and actual work items completed:
Domain invariants and behavior changed/preserved:
Tests: command, cases, pass/fail/skip, isolated/real-provider, evidence hash/path:
New failures -> root cause -> fix -> regression evidence:
Runtime: exact mutations or NONE; active/config/rollback map:
Resources: latency/capacity/memory/disk measured vs budget; untested limits:
Cleanup: removed/retained artifacts, reason/expiry, disk/restart evidence:
Remaining decision gates, not relabelled implementation gaps:
Astra requested review points and next allowed step:
```

Astra kiểm ít nhất: source/runtime correspondence, domain math/identity/ordering,
checkpoint/fence/recovery races, auth/cursor boundary, bounded task/memory lifecycle,
actual consumer behavior, denominator/gate math và cleanup. Reviewer chỉ rerun affected
or missing evidence, không tái chạy toàn provider certification không đổi.

Finding của Astra ở scope phase được Claude fix/test và append receipt cùng phase;
không mở `KN-2.1/KN-2-final-final` train mới. Sau `ASTRA_REVIEW_PASS`, ghi CLOSED trong
tracker nếu goal/exit đúng cấp đã đủ. Final release phải có KN-5 real rollout/provenance,
không chỉ đủ năm mục docs hoặc source tests.

**Thời gian:** năm phase không đồng nghĩa năm ngày. KN-1 chốt estimate sau prototype
và sizing; không promise best possible latency/100% mọi failure trong hạ tầng một host.
Tối ưu thời gian bằng reuse đúng code/evidence, source slices đã test, deterministic
fault tests và một final acceptance; không bỏ tests hoặc nới SLA cho kịp ngày.

**Tài liệu này được công bố để Claude thực hiện theo lệnh bắt đầu phase của owner.**
Trạng thái thực thi ban đầu của cả năm phase là NOT STARTED. Source ở thời điểm soạn:
`74337e71ce8ce7611ca424d33f7a0ab29535733a`; facts runtime trong mục 0-17 là lịch sử,
không có health/latency certification mới từ việc soạn guide.

### Post-release consumer realms (owner approved 2026-09-27)

`paper`, `sandbox`, and `live` are workload authorization realms, not different
copies of public mainnet market data. The existing `paper` cache/product/cursor
namespace is retained as the physical market-data realm for compatibility;
it does not certify an execution account mode. Every additional identity has a
separate TLS client identity and JWT signing key. Explicit key-to-realm and
key-to-subject maps must cover the entire keyring. A token cannot select its own
realm independently of its pinned key. Consumer ID, manifest revision, venue/feed
entitlements, purposes, quotas and consumer-bound signed cursor remain enforced.
Old single-realm configuration keeps its existing rejection behavior.

No extra network authorization hop or service is introduced. Query loads a sealed
configuration on startup; Rust Stream atomically reloads its authority and revokes
an admitted stream if its key realm changes. Configuration is public-key material
only; all private material stays in protected runtime directories outside Git.
`compile_consumer_realms.py` maps only the three approved TS/Binance-alpha/OKX-alpha
scopes. TS PAPER_ONLY becomes ALLOWED for its explicitly approved sandbox/live
read identity. Alpha FORBIDDEN direct execution dependency stays FORBIDDEN: alpha
uses data to construct intent, while TS Risk remains the admission authority.
Read activation never enables orders, changes broker mode, or starts an alpha.

#### Three-realm operation and consumer handoff

Adding an approved reader is configuration materialization, not a new service or
per-symbol image. Compile the versioned manifest with compile_consumer_realms.py,
provision a separate TLS identity and JWT key, and include the public key/subject/
realm and manifest in the reader packet. Current Query startup requires a bounded
reader reload/recreate for configuration changes; do not promise zero reload.
No code/image rebuild is needed merely for another binding within supported
contracts. New feed implementations still require source tests and release.

Consumer identity, sealed binding, manifest revision and credential must change
together. TS settings use DATA_LAYER_V2_JWT_KEY_ID, JWT_SUBJECT, JWT_ENVIRONMENT,
JWT_MANIFEST_REVISION and JWT_PRIVATE_KEY_FILE (all prefixed DATA_LAYER_V2_), plus
TLS_CA_FILE/TLS_CERT_FILE/TLS_KEY_FILE and CONSUMER_BINDING_FILE. Use the real
configuration field names in each consumer; do not rename a paper key to live.
Keep separate cursor/audit paths by consumer ID and preserve source-policy gates.
The runtime packet under consumer-realms contains private credentials outside Git;
mount only the selected consumer identity, never the whole identity directory.

The realm labels workload permission only: sandbox/live still read the same
public mainnet market products. They do not select demo exchange books, change
account mode, establish broker execution parity, or permit order submission.
Current TS process remains on its existing paper read identity until a consumer
config handoff; the additional server mappings are active and tested no-order.


### Execution Handoff Hot Backup - 2026-10-01

Approved narrow continuation of KN-5 K5-T03/T05, tracked in the Unified Plan
under Canonical Hot Backup Implementation Checkpoint. No new topology.
The public Rust GetSnapshot/GetFeedStatus still delegates to Query and is NOT
an independent fallback. Internal `qdl.internal.v2.CanonicalHotView/ReadLatest`
reads the existing read-committed Hub ring on the same mTLS listener. Query
remains the sole public entitlement/quality/cursor owner. The private response
is canonical bytes and original record offset plus atomic partition watermark,
not a synthetic Redis generation or execution approval. Both coordinates must
remain distinct; a partition watermark may advance without a new product price.

Disabled unless Stream QDL_KN_HOT_READ_ENABLED=true and Query
QDL_KN_HOT_READ_TARGETS lists one/two TLS host:port targets. Reuse existing
QDL_KN_READ_VIEW_SECRET_FILE with a distinct HMAC domain. No new secret copied
from a consumer, provider limiter, service, group membership or per-read reader.
Work is bounded (8 operations, 100ms Stream work, 250ms total Query attempt by
default, <=256KiB canonical payload); measure peak RSS and tail before promotion.
Cancellation must retain the admission permit until admitted work actually ends.

Only same-product TRADE/QUOTE/MARK_INDEX_PRICE/BOOK_SNAPSHOT/BOOK_DELTA may use
backup. Keep BAR/history warmup semantics and public schemas unchanged. Query
checks source lineage and CURRENT quality after the read, and refuses a view
behind its last selected record/watermark. MARK/INDEX must rerun component and
session checks, not just generic event recency. Hard identity/fence/integrity
refusals cannot be healed by trying a replica holding an older view. A newer
book sibling with reset/gap/unverified state or different book/provider generation
blocks the older snapshot. Backup failure never promotes an expired cached value.

This is projector-path redundancy, not independent-host or provider HA. The two
paths still share canonical Kafka/core and host infrastructure. Test no-recursion,
read_committed (aborted records absent), identity/HMAC/mTLS negatives, bounded
work, paired restart, exact cursor handoff, lost backup and primary return.
Observe actual consumer usable data, requests/refusals and resource overhead;
unit/in-process/loopback evidence alone cannot certify execution continuity.
Roll only tested changed existing roles, preserve per-role image/config rollback,
no offsets/reset/cache deletion. Clean isolated resources and record exact receipt.


#### Broker-confirmed quiet reads and production receipt

A quiet eligible cache view requires independent canonical verification: provider
session liveness alone does not prove projector progress. The Stream reuses its
existing Kafka consumer for bounded background positive broker-head confirmation;
proof age <=1s, consumed head required,250ms cycle budget/80ms call maximum,
500ms cadence. Empty poll never renews proof. A newer in-flight head does not
extend the previous caught-up proof. Loss of broker proof fails closed even if
provider heartbeats continue. Retain newest valid-lineage ineligible quality for
SDK rejection; do not hide it behind an older primary. This is not a timestamp
refresh or an alternative execution policy.

Runtime c8015ca six-role rollout2026-10-01 and accepted TS readiness9160e5a are
recorded in Unified Plan under Hot Backup Production Acceptance And Cleanup.
Final300s session60/60, no cache/watermark/report-expiry errors; six TRADE snapshot
eligibility refusals retained. The rolling quota-probe interference is separately
recorded and rules out a zero-downtime claim. Same-host backup is not independent
HA. Published release remainsv2.2.1; newsource/images await remote release workflow.


<a id="execution-data-contract-closure-guide-20261001"></a>
### EDC-1 Consumer Execution Contract Closure

2026-10-01 owner request: audit all actual TS Risk/execution data dependencies and
write ONE consolidated repair phase before implementation. The authoritative
scope, source findings, test cases, latency definitions, rollback and journal are
[EDC-1 in the Unified Plan](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#execution-data-contract-closure-20261001).
Latest decision: IN_PROGRESS / OWNER_GOAL_RESUMED under the
[owner scope review refresh](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#edc1-owner-final-scope-review-20261002).
The latest owner goal continuation resumes implementation of this consolidated scope.
Preserve prior evidence; runtime still requires the exact changed-role packet.
The existing final approval plan remains the detailed test baseline. Existing receipts remain
scoped evidence, not whole-path availability or deployment approval.
Prior implementation followed the
[final dependency closure plan](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#edc1-owner-review-freeze).
Source receipts remain evidence, not deployment approval.
Earlier source tests are not runtime certification.
Deployment/release still requires the reviewed changed-role packet.
It supplements the existing KN architecture, not another rearchitecture phase.

Preserve the canonical Kafka -> Rust core -> KN cache/Stream -> Query boundary.
Query remains entitlement/quality authority; Risk owns intent admission and must
revalidate the required typed evidence at use. The accepted private Stream hot
backup provides same-product projector-path redundancy, not independent HA.
Public Stream GetSnapshot still delegates to Query and cannot be its fallback.

The narrow completion is lossless quality/provenance propagation into TS,
intent-and-portfolio-specific dependency selection, bounded recovery outside DB
locks, and proof at actual native admission/dispatch/paper-matching boundaries.
Do not replace a strict TRADE requirement with QUOTE, manufacture timestamps,
let fresh QUOTE renew an old paper LAST price, or weaken component/session/gap
checks. No new per-symbol workers, broker topology or SQLite spool path.

Use the existing operation budgets. Measure request through consumer validation,
provider event through actual TS Redis apply, and Risk use separately in ms.
Do not quote callback time as Redis commit or session health as execution-ready.
Inherited tests remain valid only for unchanged predicates; exact changed rows
run before one final no-order window. Production rolling stress is not the core
goal, and no historical C2/catalogue rerun is justified merely to search for bugs.

Deployment/release requires the reviewed closure ledger and changed-role packet
specified in EDC-1. Shared TS legacy runtime, native TS candidate, paper/sandbox/
live market-data identity and broker order authority must remain distinguishable.
No alpha/mainnet activation, TS accounting redesign or P18.3E money certificate
is implied. This planning entry changes no code, images, limits or runtime state.
