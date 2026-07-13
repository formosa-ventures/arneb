# 現代 OLAP 查詢引擎的共通底層哲學與 arneb 架構綱領

> 寫在前面：本文淬鍊自當今業界最頂尖的五個分析型查詢引擎——**Trino (Java)、DuckDB (C++)、ClickHouse (C++)、DataFusion (Rust) 與 Spark (Scala/JVM)** 的原始碼深度解析。
> 儘管它們各自有著獨特的設計以適應不同的戰場（例如 DuckDB 專攻單機極限、ClickHouse 專攻儲存運算耦合、Trino 專注聯邦查詢、Spark 專攻容錯的大規模批次），但為了達到極致的效能，它們在最底層的架構上演化出了高度的「趨同性」。
> 本文梳理了這些引擎共通的 **Key Design Patterns**。對於以 Rust + Apache Arrow 為基礎自建分散式引擎的 **`arneb`** 團隊而言，這些並非只是參考，而是必須遵循的「鐵律」。

---

## 1. 嚴格分層的查詢生命週期 (The Engine Lifecycle)

所有現代引擎都不約而同地放棄了「在 Parser 階段邊解析邊做事」的古老設計。它們將從 SQL 字串到實體執行的過程，切分為嚴格且不可逆的獨立階段。

### 1.1 語意綁定 (Binding) 是一道不可逆的邊界
*   **共通現象**：DuckDB 的 `Binder`、ClickHouse 的 `QueryAnalyzer`、Trino 的 `StatementAnalyzer`，皆負責將初步的 AST (抽象語法樹) 綁定到真實的 Schema 上。
*   **強型別 IR (Intermediate Representation)**：在經過 Analyzer 階段後，必須產生一棵**不再包含任何未解析 Identifier 的強型別 IR 樹**（例如 ClickHouse 耗費巨大力氣重構出的 `QueryTree`）。
*   **對 arneb 的鐵律**：Optimizer (邏輯最佳化階段) 接收到的 `LogicalPlan` 絕對不能再有歧義。所有的 Table、Column、型別推導，都必須在 `Planner` 階段的 Resolver 中一次性解決。Optimizer Pass 必須能 100% 信任現有的 Schema 綁定。

---

## 2. 向量化批次記憶體佈局 (Vectorized Columnar Batches)

分析型引擎 (OLAP) 絕對不能使用 Volcano 模型（Row-by-Row 逐行處理），因為虛擬函式呼叫的開銷與 CPU Cache Miss 會徹底摧毀效能。

### 2.1 批次與列式是唯一解
*   **共通現象**：資料在運算子 (Operator) 之間流動的最小單位，永遠是一「批 (Batch)」列式記憶體區塊。
*   **實踐**：Trino 有 `Page`/`Block`，DataFusion 有 `RecordBatch`，DuckDB 有固定大小 (預設 2048) 的 `DataChunk`，ClickHouse 有 `Block`/`Chunk`。
*   **另一條路 (反例)**：Spark 是這條鐵律的重要反例。Spark SQL 預設仍是 row-at-a-time（運算子之間流動的是 `InternalRow`），但它用 **Whole-Stage Codegen** (`WholeStageCodegenExec`) 把一整段可 codegen 的子樹 fuse 成單一 Java 函式，藉此消除逐列處理的虛擬函式呼叫開銷（達到接近向量化的效果，但走的是「程式碼生成」而非「批次列式」這條路）。它的 columnar 型別 (`ColumnVector`/`ColumnarBatch`) 主要用於 scan（如 Parquet 向量化讀取）與 in-memory cache，而非作為運算子之間的通用流動單位。
*   **對 arneb 的鐵律**：`arneb` 基於 Apache Arrow 是最正確的選擇。必須確保在 Pipeline 傳遞過程中，盡可能維持 `RecordBatch` 的原貌，並充分利用 Arrow 既有的 Dictionary/Constant 特性來避免不必要的記憶體拷貝 (Zero-copy)。

---

## 3. 執行模型與背壓機制 (Execution Model & Back-pressure)

雖然各個引擎在排程哲學上各有取捨（Pull-based 或是 Push-based），但它們都必須嚴肅面對「當下游處理不及時，上游如何暫停 (Back-pressure)」的問題。

### 3.1 非阻塞式的背壓 (Non-blocking Back-pressure)
*   **共通現象**：運算子 (Operator) 的 `work()` 階段絕對不可持有阻塞資源或盲目等待。
*   **實踐**：
    *   ClickHouse 引入了 `IProcessor` 狀態機，當輸出埠滿了，會優雅地回傳 `PortFull` 狀態，讓排程器掛起該任務，而不是卡死執行緒。
    *   DuckDB 的 `OperatorResultType` 有 `HAVE_MORE_OUTPUT` 和 `BLOCKED`。
    *   Spark 走的是另一種取捨：RDD 是 pull-based 的 `Iterator`（`RDD.compute` 回傳 `Iterator[T]`，由下游 `next()` 拉動上游），背壓不在運算子的單批粒度上處理，而是上移到 **stage/task 排程層**——以 shuffle 邊界切 stage、由 DAGScheduler/TaskScheduler 控制 task 的並行與排隊。
*   **對 arneb 的鐵律**：在 Rust Async 生態中，千萬不要使用「讓 Tokio task 持有 Semaphore permit 在 stream 內死等」的方式來做背壓（這會導致 Deadlock）。應該善用狀態機或 Channel 的容量限制，讓背壓成為一種「排程器層級的可觀測訊號」，讓出 CPU 執行權 (`yield_now()`)。

---

## 4. 兩階段分散式聚合與「中間狀態」 (Two-Phase Execution & Intermediate States)

在分散式或多核並行的情境下，`GROUP BY` 或 `JOIN` 幾乎無法在一個節點上一次算完。

### 4.1 第一級別的可序列化狀態 (First-class Serializable State)
*   **共通現象**：聚合操作必須被拆分為 Partial (節點/Shard內) 與 Final (跨節點合併)。
*   **實踐**：ClickHouse 的 `Aggregator::convertToChunks(final=false)` 會輸出包含狀態 (如 HyperLogLog) 的 Chunk。Trino 的 Accumulator state 亦是如此。Spark 把這拆成四種 `AggregateMode`——`Partial`（讀原始列、輸出 aggregation buffer）、`PartialMerge`（合併 buffer、再輸出 buffer）、`Final`（合併 buffer、產出最終結果）、`Complete`（不切兩階段）；其中 `TypedImperativeAggregate` 明確提供 `serialize`/`deserialize`/`merge`/`eval`，讓中間狀態能跨 shuffle 傳輸。
*   **對 arneb 的鐵律**：不要把 Partial Aggregation 和 Final Aggregation 當成兩個完全無關的 Operator 來寫！應該將聚合函數的「中間狀態」設計成一種**可被序列化傳輸的獨立資料型別**（提供 `serialize`/`merge`/`finalize` 方法）。當跨網路 Exchange 洗牌時，傳輸的就是這些中間狀態，到了 Coordinator 再進行最終的 `finalize`。

---

## 5. 統一記憶體管帳與防 OOM 溢寫 (Centralized Memory Tracking & Spill-to-Disk)

OLAP 查詢極其消耗記憶體。如果只讓各個 Operator「憑感覺」回報記憶體用量，在面對大型 JOIN 時必定會發生 OOM 被 OS 獵殺 (Kill)。

### 5.1 記帳必須下沉到分配點 (Allocator-level Tracking)
*   **共通現象**：必須有全域的記憶體監控，且強制實施 Hard Limit。
*   **實踐**：
    *   DuckDB 所有的配置都必須通過 `BufferManager`，如果達到上限，會強制驅逐未 Pin 的 Block。
    *   ClickHouse 更是直接覆寫了底層的 `MemoryTracker`，在 Allocator 層攔截每一筆分配。
    *   Spark 的 `UnifiedMemoryManager` 把 execution 與 storage 記憶體統一管理並設硬上限；當某個 task 要不到記憶體時，`TaskMemoryManager` 會反過來呼叫同一 task 內其他 `MemoryConsumer` 的 `spill(size, trigger)`，由它們協作式 (cooperative) 地溢寫釋放記憶體——亦即記帳與 spill 觸發是綁在一起的協作機制。
*   **對 arneb 的鐵律**：拋棄「讓 Operator 自願回報 `try_grow`」的鬆散作法。`arneb` 的記憶體記帳 (Memory Pool) 必須盡可能攔截所有 Arrow Buffer 的生成。當記憶體撞到天花板時，Hash Join、Aggregate、Sort 等 Stateful 算子必須啟動溢寫磁碟 (Spill-to-disk) 機制（例如按 Bucket 細粒度溢寫，而非整批溢寫）。

---

## 6. 極限 I/O 下推與延遲物化 (Extreme Pushdown & Late Materialization)

「最快的查詢就是不讀取不需要的資料。」

### 6.1 下推與延遲 (Pushdown & Late Materialization)
*   **共通現象**：盡可能將過濾條件推給最底層的掃描器 (Scanner) 或儲存引擎。
*   **實踐**：
    *   DataFusion 的 `TableProviderFilterPushDown`。
    *   Trino 的 Connector SPI 支援下推。
    *   ClickHouse 的 `optimizePrewhere` (先讀過濾欄位，過濾後再讀取其他欄位)。
    *   DuckDB 最佳化器內建的 `LATE_MATERIALIZATION` Pass。
    *   Spark DataSource V2 的 `SupportsPushDown*` 介面家族（`SupportsPushDownFilters`/`SupportsPushDownV2Filters`、`SupportsPushDownRequiredColumns`、`SupportsPushDownAggregates`、`SupportsPushDownLimit`/`SupportsPushDownTopN`、`SupportsPushDownOffset`…），讓資料源各自宣告能接手哪些下推。
*   **對 arneb 的鐵律**：
    1.  **Row-Group Pruning**：充分利用 Parquet 檔案層級的 min/max 統計資訊，在讀檔前就直接剪去 (Skip) 不符合的 Row-Group。
    2.  **Late Materialization**：對於帶有龐大字串的寬表 JOIN，在 Scan 階段先只讀取 Join Key 和 Filter 欄位，等確認 Match 之後，再回去把又大又肥的 Payload 欄位拉出來（或者利用 Arrow 的 `take` 運算子）。這將是拯救網路頻寬與記憶體的終極武器。

---

## 7. 執行期自適應 (Runtime Adaptivity)

靜態成本模型一旦在規劃期猜錯（選錯 build side、shuffle 分區數寫死、沒看出資料傾斜），整個查詢就會錯到底。最新一代引擎的共通趨勢是：不要只信規劃期的估算，而是把執行到一半的「真實統計」餵回去修正後續的執行。

### 7.1 拿執行期統計回頭修正計畫 (Re-plan on Runtime Statistics)
*   **共通現象**：在 stage/shuffle 邊界把已物化資料的真實大小與行數量出來，用它取代規劃期的估算，調整尚未執行的部分。
*   **實踐**：
    *   Spark 的 **AQE (`AdaptiveSparkPlanExec`)** 是最完整的範例——每當一個 query stage 物化完成，就用該 stage 輸出的真實統計「re-optimize 剩餘的查詢並重新 plan」（class doc 原文），實際動作包含把 sort-merge join 換成 broadcast join、`CoalesceShufflePartitions` 依真實量合併 shuffle 分區、`OptimizeSkewedJoin` 切開傾斜分區；且新計畫成本不劣於舊計畫才採用 (`newCost ≤ origCost`) 作為安全護欄。
    *   對照之下，Trino 的 **dynamic filtering** 是較輕量的一手：用 build side 的值域在執行期把 filter 推給 probe side 的 scan，**但不會改變計畫形狀**（不會把 join 演算法或分區數換掉）。
*   **對 arneb 的鐵律**：arneb 已有 Selinger DP join reorder + partition-aware cost + NDV 估算，但**完全沒有執行期再優化**——這正是 q08 選錯 build side、`partition_count` 寫死、broadcast 因可能給錯結果而被停用等問題的共同根源。可直接照搬的關鍵設計：(1) 以 **REMOTE exchange / fragment 邊界**作為天然的「重優化檢查點」（與 Spark 的 QueryStage 邊界同構）；(2) 在 fragment 完成時，讓 Flight exchange **回報已物化分區的真實大小/行數**餵回成本模型；(3) 嚴守 **新計畫成本不劣於舊才採用**的安全護欄，避免再優化反而變糟。補上這一條，等於補上靜態成本模型一切誤判的安全網。
