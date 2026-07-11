# Apache Spark 分散式計算框架與 Spark SQL 架構解析（經本地原始碼核實版）— 給 arneb 自建引擎工程師

本文件深入解析 Apache Spark 的核心架構、設計權衡與關鍵原始碼模組，並針對正在以 Rust 構建分散式 SQL 查詢引擎 `arneb`（基於 `tokio` + `arrow-rs`）的工程師，提供具體的架構借鏡與設計決策指引。Spark 與 Trino/arneb 有一個根本差異：它**不是**低延遲互動式 MPP SQL 引擎，而是一個**通用、容錯的分散式計算框架**（以 RDD lineage 為核心），Spark SQL 只是建在這個框架「之上」的一層關聯式前端——這個層次關係貫穿全文，是理解 Spark 一切設計取捨的鑰匙。

本文每一項關鍵事實都已對照本地 checkout 的 `apache/spark`（commit `072994d33c042ed60f28af1c11cc2c4584162638`，short `072994d3`，dated 2026-06-14），並附上 `相對路徑:行號`（相對 repo root `/Users/bochengyang/formosa-ventures/repos/spark`）。技術名詞、class 名、package 路徑、方法名、設定鍵一律保留英文原文。各章末附「→ 對 arneb 的啟發」。本文的啟發與 `common.md`（四引擎共通哲學綱領）呼應——特別是「嚴格分層生命週期」「向量化批次」「非阻塞背壓」「兩階段聚合的可序列化中間狀態」「統一記帳 + spill」「下推與延遲物化」這六條鐵律——並在具體之處標出 Spark 與該綱領的對齊或張力。詳細的核實方法、引用慣例與仍存疑點見文末「驗證方法與來源」一節。

---

## 1. 專案定位與設計哲學

Apache Spark 的定位與 Trino/arneb 有一個根本差異：它**不是**一個「低延遲互動式 MPP SQL 引擎」，而是一個**通用、容錯的分散式計算框架**（以 RDD lineage 為核心），Spark SQL 只是建在這個框架「之上」的一層關聯式前端。這一層次關係直接寫在模組職責裡——`sql/README.md:8` 一句點明 `sql/core` 的角色是「translating Catalyst's logical query plans into Spark **RDDs**」（把 Catalyst 邏輯計畫翻成 Spark RDD），亦即 SQL 最終是被「降階」成 RDD DAG，交給通用排程器跑。

### 1.1 兩層結構：通用計算框架 + Spark SQL 前端

```
        ┌──────────────────────────── Spark SQL（sql/*）────────────────────────────┐
        │  SQL / DataFrame                                                           │
        │      │ Catalyst（TreeNode 樹改寫：analyze → optimize → physical plan）     │
        │      │ Tungsten（whole-stage codegen、UnsafeRow、columnar scan）          │
        │      ▼                                                                     │
        │  RDD[InternalRow]   ← QueryExecution.toRdd（生命週期終點）                  │
        └──────────────────────────────────┬─────────────────────────────────────────┘
                                            ▼
        ┌──────────────────────── 通用計算框架（core/）────────────────────────────┐
        │  RDD（lineage / 依賴圖）→ DAGScheduler（依 shuffle 邊界切 stage）          │
        │  → TaskScheduler → Executor（thread-per-task）                              │
        │  容錯靠 lineage 重算 + shuffle 檔物化（FetchFailed → 重跑遺失的 stage）     │
        └────────────────────────────────────────────────────────────────────────────┘
```

- `core/`（`SparkContext`，`core/src/main/scala/org/apache/spark/SparkContext.scala:78`）的 class doc 自述為「Main entry point for Spark functionality. A SparkContext represents the connection to a Spark cluster, and can be used to create **RDDs, accumulators and broadcast variables**」——可見 core 的抽象單位是 RDD，不是 SQL。
- Spark SQL（`sql/core`）把關聯式查詢「編譯」成 RDD，最後由 `QueryExecution.toRdd`（`sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala:392`）產出 `RDD[InternalRow]`，再交給 core 的 `DAGScheduler`/`TaskScheduler` 執行。

### 1.2 容錯模型：lineage + shuffle 物化（與 Trino/arneb 對立）

這是 Spark 最值得 arneb 借鏡、也最容易被寫錯的一點。Spark 的容錯**不是**靠重跑整個查詢，而是：

1. **shuffle 是 disk-materialized 的**。`DAGScheduler` 的 class doc（`core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala:87-91`）明說 stage 在 shuffle 邊界被切開，並「introduce a barrier（where we must wait for the previous stage to finish to fetch outputs）」；其中 `ShuffleMapStage`「writes map output files for a shuffle」——map 端把分桶結果寫成本地檔，reduce 端再 fetch。
2. **FetchFailed → 依 lineage 重算遺失的 stage**。同一份 doc（`DAGScheduler.scala:106-110`）：「If the TaskScheduler reports that a task failed because a **map output file from a previous stage was lost**, the DAGScheduler **resubmits that lost stage**. This is detected through a CompletionEvent with **FetchFailed**, or an ExecutorLost event.」也就是說，遺失的只是「產生那份 shuffle 檔的 stage」，靠 RDD lineage 重算，已成功物化的上游不必重跑。
3. **stage 內的一般失敗**由 `TaskScheduler` 重試該 task 數次（`DAGScheduler.scala:77-79` doc：「Failures *within* a stage that are not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task a small number of times before cancelling the whole stage.」）。

對照之下，Trino/arneb 的 pipelined exchange 是「記憶體串流、任一 task 失敗即整個查詢失敗」。Spark 用「物化換容錯」，犧牲延遲換批次穩定性——這是兩類引擎的分水嶺（詳見 §7）。

### 1.3 執行模型：row-at-a-time + whole-stage codegen（不是向量化解譯）

另一個極易寫錯之處：**Spark 預設的 operator 間執行單位是「單列 `InternalRow`」，不是向量化批次**。它把一串支援 codegen 的 operator **融合成單一 JVM 函式**、在一個 tight loop 裡逐列處理——`WholeStageCodegenExec` 的 class doc（`sql/core/src/main/scala/org/apache/spark/sql/execution/WholeStageCodegenExec.scala:616-617`）原文：「WholeStageCodegen compiles a subtree of plans that support codegen together into **single Java function**.」

Spark 的 columnar/向量化路徑（`ColumnVector`/`ColumnarBatch`，`sql/catalyst/src/main/java/org/apache/spark/sql/vectorized/ColumnarBatch.java`；`VectorizedParquetRecordReader`，`sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/VectorizedParquetRecordReader.java`）**只用於 scan 讀取**（以及 columnar cache、Arrow/Pandas UDF），不是 operator 之間的傳遞單位。證據在 `FileSourceScanExec.supportsColumnar`（`sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala:704-711`）：只有在 whole-stage codegen 啟用、欄位不太多、且檔案格式 `supportBatch` 時才輸出 columnar；而一旦下游 operator 是 row-based，計畫就插入 `ColumnarToRowExec`（`sql/core/src/main/scala/org/apache/spark/sql/execution/Columnar.scala:67`）把 batch 拆回逐列。所以 Spark 的正解是「**columnar 讀進來、轉成 row、在 codegen 的 tight loop 裡逐列跑**」，這跟 Trino `Page` / arneb Arrow `RecordBatch` 那種「operator 間傳欄式批次」是不同的執行哲學（與 `common.md` §2「向量化批次是唯一解」形成有意思的張力：Spark 用 codegen 攤平 per-row 開銷以「逐列但無虛擬呼叫」逼近向量化的效果，而非真的全程欄式）。

### 1.4 Spark vs arneb 定位對照表

| 維度 | Apache Spark | arneb |
|---|---|---|
| 本質 | 通用容錯分散式計算框架（RDD）+ 其上的 Spark SQL | 專用分散式 SQL 引擎（Trino 替代品）|
| 主要場景 | 批次 / ETL / 機器學習，秒到分鐘級 | 低延遲互動式聯邦查詢 |
| operator 間單位 | 單列 `InternalRow`（whole-stage codegen tight loop）| 欄式 `RecordBatch`（Arrow，向量化）|
| 執行模型 | RDD DAG，`doExecute(): RDD[InternalRow]` 惰性建圖 | pull-based async stream（`SendableRecordBatchStream`，tokio）|
| 節點間 shuffle | **disk-materialized**（map 寫本地檔，reduce fetch）| 記憶體串流，**Arrow Flight RPC**（gRPC/HTTP2，近零拷貝）|
| 容錯 | **lineage 重算 + shuffle 物化**（FetchFailed → 重跑 stage）| pipelined-only，**無 FTE**，任一 task 失敗即查詢失敗 |
| executor 執行緒 | **thread-per-task**（`threadPool.execute(taskRunner)`）| 每 task 一個 tokio task + semaphore permit |
| 執行期再優化 | **有 AQE**（拿 runtime stats 回頭改物理計畫）| **無**（純靜態成本模型，Selinger DP）|
| 計畫框架 | Catalyst：同一棵 `TreeNode` 樹逐步重寫，`RuleExecutor` 跑 batch | LogicalPlan → 自家 optimizer + fragmenter |
| CBO | `CostBasedJoinReorder`，受 `cboEnabled && joinReorderEnabled` 開關 | Selinger DP join reorder + NDV 估算 |

> → 對 arneb 的啟發：arneb 反覆踩到的兩個結構性痛點，恰好對應 Spark 的兩個核心設計。
> 1. **exchange 在飽和下 silent-truncate**（consumer drop，後來用 `must_drain` 改成 fail-loud）——這正是「pipelined exchange 無物化、無重算」的代價。Spark 的 shuffle **物化到本地檔**＋FetchFailed **重算遺失 stage**（`DAGScheduler.scala:106-110`）給了一條清楚的演進路徑：若 arneb 要在大規模批次場景拿穩定性，需要的不是「再多堵一個 must_drain 洞」，而是引入「shuffle 邊界物化 + 失敗重算」的 FTE 模式（Trino 也是後來才補 FTE，這不是小工程，要有意識地當「第二套可插拔執行語意」做）。
> 2. **深層 join 的中間資料量大、materialize-then-forward 跨 stage 序列化是延遲牆**——Spark 本來就接受這個延遲（它是批次引擎），但用 **AQE** 在執行期把「真實 shuffle 統計」拿回來重算物理計畫（見 §3、§5.3、§7.6），把「中間資料爆掉」在跑的當下修正。arneb 的靜態成本模型曾選錯 build side（q08 builds 90M）、`partition_count` 寫死——這些都是「規劃時猜錯、執行時無從補救」的病，AQE 正是解藥。

---

## 2. 整體架構與核心組件

### 2.1 Driver / Executor 架構圖

Spark 是單一 Driver（持有 `SparkSession` + `SparkContext`，內含 `DAGScheduler`/`TaskScheduler`/`SchedulerBackend`）+ 多個 Executor 的架構。Driver 負責「把 SQL 編譯成 RDD DAG、切 stage、派 task」，Executor 負責「在自己的執行緒池上跑 task」。

```
   ┌──────────────────────────────── DRIVER（JVM 進程）─────────────────────────────────┐
   │  SparkSession（sql/core .../classic/SparkSession.scala:92）                          │
   │     │ .sql(text) → 解析 → Dataset.ofRows → QueryExecution                            │
   │     ▼                                                                                │
   │  QueryExecution（生命週期總指揮，QueryExecution.scala:67）                            │
   │     analyzed → optimizedPlan → sparkPlan → (AQE 包裹) → executedPlan → toRdd          │
   │     │                                                                                │
   │     ▼  RDD[InternalRow]                                                              │
   │  SparkContext（SparkContext.scala:86）                                               │
   │     ├─ DAGScheduler（依 shuffle 邊界切 ShuffleMapStage / ResultStage）               │
   │     │     SparkContext.scala:603  _dagScheduler = new DAGScheduler(this)              │
   │     ├─ TaskScheduler（把每個 stage 的 TaskSet 送到 cluster；TaskScheduler.scala:36）  │
   │     └─ SchedulerBackend（與 cluster manager 對接；SchedulerBackend.scala:29）        │
   └───────────────────────────────┬─────────────────────────────┬──────────────────────┘
                                    │ launchTask（serialized）     │ heartbeat / status
              ┌─────────────────────▼──────┐         ┌─────────────▼──────────────┐
              │       EXECUTOR A           │         │       EXECUTOR B            │
              │  Executor.threadPool       │         │  Executor.threadPool        │
              │  （newCachedThreadPool）   │         │  （thread-per-task）        │
              │   └ TaskRunner（Runnable） │◀──shuf──▶│   └ TaskRunner（Runnable）  │
              │      跑 RDD partition       │  fle    │      跑 RDD partition        │
              │  ShuffleManager → 本地檔    │ (disk)  │  ShuffleManager → 本地檔     │
              └──────────────┬─────────────┘         └──────────────┬──────────────┘
                             ▼  reduce 端 fetch 已物化的 map output files
                    [ 雲端物件儲存 / HDFS / 外部資料源 ]
```

**關鍵差異（對齊易錯點）**：Executor 上是 **thread-per-task**——`Executor.launchTask`（`core/src/main/scala/org/apache/spark/executor/Executor.scala:551`）對每個 task 建一個 `TaskRunner extends Runnable`（`Executor.scala:687,691`）並 `threadPool.execute(tr)`（`Executor.scala:563`），而 `threadPool` 是 `Executors.newCachedThreadPool(...)`（`Executor.scala:307-313`）。**一個執行緒跑一個 task 到完成**，沒有 Trino 那種中央 time-slice 協作式排程（詳見 §10.2）。

### 2.2 組件 → package/class → 職責對照表（皆已核實存在）

| 組件 | Package / Class（檔案:行號）| 職責 |
|---|---|---|
| Session 入口（抽象）| `org.apache.spark.sql.SparkSession`（`sql/api/.../SparkSession.scala:63`，`abstract class`）| Classic 與 Connect 共用的對外介面（`sql`、`range`、`read`）|
| Session 入口（Classic 實作）| `org.apache.spark.sql.classic.SparkSession`（`sql/core/.../classic/SparkSession.scala:92`）| 持有 `sparkContext`、`sessionState`；`sql()` 進入點（`:528`）|
| 計算框架入口 | `org.apache.spark.SparkContext`（`core/.../SparkContext.scala:86`）| 連到 cluster，建 RDD，持有三個 scheduler |
| 生命週期總指揮 | `org.apache.spark.sql.execution.QueryExecution`（`sql/core/.../QueryExecution.scala:67`）| 串接 analyzed→optimized→sparkPlan→executedPlan→toRdd |
| SQL 解析 | `AbstractSqlParser` + `AstBuilder`（`sql/catalyst/.../parser/AbstractSqlParser.scala:34`；ANTLR4 `SqlBaseParser.g4`）| SQL 字串 → `LogicalPlan`（`parsePlan`，`:94`）|
| 語意分析 | `org.apache.spark.sql.catalyst.analysis.Analyzer`（`sql/catalyst/.../analysis/Analyzer.scala:304`）| unresolved → resolved `LogicalPlan`（rule batches，`:506`）|
| 邏輯最佳化 | `org.apache.spark.sql.catalyst.optimizer.Optimizer`（`sql/catalyst/.../optimizer/Optimizer.scala:51`）| RBO + CBO 規則（`ReorderJoin`/`ColumnPruning`/`PushDownPredicates`/`CostBasedJoinReorder`）|
| Rule 框架 | `RuleExecutor` + `Batch`/`Strategy`/`Once`/`FixedPoint`（`sql/catalyst/.../rules/RuleExecutor.scala:125,150,156,162`）| 以 batch 跑規則到 fixed point；`Once` 要 idempotent |
| 物理計畫策略 | `SparkPlanner`（`sql/core/.../execution/SparkPlanner.scala:31`）+ `QueryPlanner`（`sql/catalyst/.../planning/QueryPlanner.scala:55`）| LogicalPlan → `SparkPlan`（strategies + `planLater`）|
| 物理計畫節點 | `SparkPlan`（`sql/core/.../execution/SparkPlan.scala:65`）| `doExecute(): RDD[InternalRow]`（`:343`）|
| 執行期準備 | `QueryExecution.preparations`（`QueryExecution.scala:752`）| 插 exchange/sort、codegen、AQE 包裹 |
| Exchange 插入 | `EnsureRequirements`（`sql/core/.../exchange/EnsureRequirements.scala:51`）| 依分佈/排序需求插 `ShuffleExchangeExec`（`:129`）|
| whole-stage codegen | `WholeStageCodegenExec`（`sql/core/.../WholeStageCodegenExec.scala:643`）| 把 codegen 子樹融成單一 Java 函式 |
| 自適應執行 | `AdaptiveSparkPlanExec`（`sql/core/.../adaptive/AdaptiveSparkPlanExec.scala:70`）/ `InsertAdaptiveSparkPlan`（`.../InsertAdaptiveSparkPlan.scala:44`）| 執行期再優化（QueryStage + 真實統計）|
| Stage 排程 | `DAGScheduler`（`core/.../scheduler/DAGScheduler.scala:124`）| 依 shuffle 邊界切 `ShuffleMapStage`/`ResultStage`，FetchFailed 重算 |
| Task 排程 | `TaskScheduler`（`core/.../scheduler/TaskScheduler.scala:36`，trait）| 把每個 stage 的 TaskSet 送到 cluster、重試、抑制 straggler |
| Cluster 對接 | `SchedulerBackend`（`core/.../scheduler/SchedulerBackend.scala:29`，trait）| 與 cluster manager（Standalone/YARN/K8s）溝通取資源 |
| Task 執行 | `Executor` + `TaskRunner`（`core/.../executor/Executor.scala:249,687`）| thread-per-task 跑 RDD partition |
| Shuffle | `SortShuffleManager` / `ShuffleWriter`（`core/.../shuffle/...`）| map 端寫本地排序檔，reduce 端 fetch |
| 資料單位 | `InternalRow`（row）/ `ColumnarBatch`（`sql/catalyst/.../vectorized/ColumnarBatch.java`，僅 scan/cache/UDF）| 列式為主，欄式僅用於讀取層 |

### 2.3 模組切分（沿用 `sql/README.md` 的權威說法）

Spark SQL 拆成五個子專案（`sql/README.md:6-16`，原文摘錄）：

- **API（`sql/api`）**：「some public API like DataType, Row, etc. This component can be shared between **Catalyst and Spark Connect client**.」——這也是為何 `SparkSession` 的抽象介面在 `sql/api`（`SparkSession.scala:63`），而 Classic 實作落在 `sql/core/.../classic/`（`:92`）：同一份 API 同時服務本地 Classic 與遠端 Connect。
- **Catalyst（`sql/catalyst`）**：「An **implementation-agnostic** framework for manipulating **trees of relational operators and expressions**.」——只管 `TreeNode` 樹的改寫（Analyzer/Optimizer/Parser/QueryPlanner 基底都在此），不綁定執行引擎。
- **Execution（`sql/core`）**：「A query planner / execution engine for translating Catalyst's logical query plans into **Spark RDDs**.」——`SparkPlanner`、`SparkPlan`、`QueryExecution`、AQE、Exchange、whole-stage codegen 全在這。
- **Hive Support（`sql/hive`）** 與 **HiveServer/CLI（`sql/hive-thriftserver`）**：Hive 相容層與 JDBC/ODBC server。
- **`core/`**（不在這五個之內，是底層框架）：RDD、`SparkContext`、三個 scheduler、Executor、shuffle。

> → 對 arneb 的啟發：
> 1. **「framework / SQL 前端」分層**值得對照。arneb 是「專用 SQL 引擎」，沒有 RDD 這層通用抽象——這在低延遲上是優勢（少一層降階）。但 Spark 把 `sql/catalyst`（implementation-agnostic 的樹改寫）與 `sql/core`（綁 RDD 的執行）切乾淨，使得 Catalyst 可被 Spark Connect 重用、規則可獨立測試。arneb 的 `planner` crate 已是類似切法，值得守住「optimizer 規則只操作 LogicalPlan、不碰執行細節」這條界線（呼應 `common.md` §1「Optimizer 接收到的 LogicalPlan 絕對不能再有歧義」）。
> 2. **executor 執行緒模型是 arneb 的核心痛點所在**。Spark 是直白的 thread-per-task（`Executor.scala:563`），靠 cluster 的核心數做並行，沒有「permit 持有整個生命週期」的問題——因為 task 跑完就還執行緒。arneb 的「tokio task 持有 semaphore permit 跑完整個 operator 生命週期，與 stream back-pressure 不相容 → deadlock / exchange stall」恰恰是 Spark 模型不會有的（Spark task 是同步阻塞跑到完，沒有 async back-pressure 的反向耦合）。這提醒：arneb 的 deadlock 不是「分散式引擎都會有」，而是「pull-based async + 持有式 permit」這個特定組合的產物，是可以從排程模型層面根治的（`common.md` §3.1 的鐵律亦同：別讓 tokio task 持有 permit 在 stream 內死等）。

---

## 3. 查詢生命週期：從 SQL 字串到結果

### 3.1 總覽圖

```
 SQL string（SparkSession.sql(text)，classic/SparkSession.scala:528）
   │  tracker.measurePhase(PARSING)
   │  sessionState.sqlParser.parsePlanWithParameters(...)（ANTLR4 → unresolved LogicalPlan）
   ▼
 Dataset.ofRows（classic/Dataset.scala:110）
   │  qe = sessionState.executePlan(logicalPlan)（SessionState.scala:142）→ new QueryExecution
   ▼
 ┌──────────────── QueryExecution（QueryExecution.scala:67，一串 lazy val）──────────────┐
 │ (1) analyzed       :211  analyzer.executeAndCheck(...)        → resolved LogicalPlan    │
 │ (2) commandExecuted:223  eagerlyExecuteCommands(analyzed)     → DDL/命令當場執行         │
 │ (3) withCachedData :305  cacheManager.useCachedData(...)      → 套用 cache 重寫          │
 │ (4) optimizedPlan  :329  optimizer.executeAndTrack(...)       → optimized LogicalPlan    │
 │ (5) sparkPlan      :347  QueryExecution.createSparkPlan(...)  → 物理 SparkPlan           │
 │         └ planner.plan(ReturnAnswer(plan)).next()（:820；SparkPlanner strategies）       │
 │ (6) executedPlan   :372  prepareForExecution(preparations,...)→ 準備好的 SparkPlan        │
 │         └ preparations（:752）: InsertAdaptiveSparkPlan（AQE 包裹）, EnsureRequirements   │
 │            （插 exchange，:764）, CollapseCodegenStages（codegen 融合，:778） …            │
 │ (7) toRdd          :392  executedPlan.execute()               → RDD[InternalRow]          │
 └──────────────────────────────────────┬──────────────────────────────────────────────────┘
                                         ▼ action（collect/count…）觸發
 SparkContext.runJob（SparkContext.scala:2481）→ dagScheduler.runJob（:2496）
   │  DAGScheduler 依 shuffle 邊界切 stage（ShuffleMapStage / ResultStage）
   ▼
 TaskScheduler → SchedulerBackend → Executor.launchTask（thread-per-task）→ 結果回 Driver
```

### 3.2 七個 lazy val：惰性、可分階觀察的生命週期

`QueryExecution` 的 class doc（`QueryExecution.scala:60-62`）自述為「The **primary workflow** for executing relational queries using Spark. Designed to allow easy access to the **intermediate phases**」。它的精妙之處在於：**每個階段都是一個 `lazy val`，彼此鏈式依賴、按需觸發、各自只算一次**。

1. **analyzed**（`:211`）：`lazyAnalyzed`（`:192`）在 `ANALYSIS` phase 內呼叫 `analyzer.executeAndCheck(sqlScriptExecuted, tracker)`（`:200`），把 unresolved 的 `LogicalPlan` 解析成 resolved（綁定 table、欄位、型別）。失敗時 `tracker.setAnalysisFailed`（`:206`）。
2. **commandExecuted**（`:223`）：`lazyCommandExecuted`（`:215`）依 `CommandExecutionMode` 處理——對 DDL/命令（如 `CreateTableAsSelect`），`eagerlyExecuteCommands`（`:236`）**當場執行**並包成 `CommandResult`（`:248`）。這是 Spark「命令 eager、查詢 lazy」的分界。
3. **withCachedData**（`:305`）：`lazyWithCachedData`（`:289`）先 `assertAnalyzed()`+`assertSupported()`，再 clone 計畫（`:296`，註解明說「avoid sharing the plan instance between different stages」），交給 `cacheManager.useCachedData`（`:300`）套用快取重寫。
4. **optimizedPlan**（`:329`）：`lazyOptimizedPlan`（`:311`）在 `OPTIMIZATION` phase 內呼叫 `sessionState.optimizer.executeAndTrack(withCachedData.clone(), tracker)`（`:319`），跑 Catalyst 的 RBO+CBO batch。
5. **sparkPlan**（`:347`）：`lazySparkPlan`（`:335`）在 `PLANNING` phase 內呼叫 `QueryExecution.createSparkPlan(planner, optimizedPlan.clone())`（`:342`）。其實作（`:815-821`）只是 `planner.plan(ReturnAnswer(plan)).next()`——**取 planner 回傳的第一個候選物理計畫**（原始碼註解 `:818` 坦承「we use next() … but we will implement to choose the best plan」，目前並未真的列舉挑最佳）。
6. **executedPlan**（`:372`）：`lazyExecutedPlan`（`:353`）在 `PLANNING` phase 內呼叫 `QueryExecution.prepareForExecution(preparations, sparkPlan.clone())`（`:361`）。這一步用一連串物理規則「準備」計畫（見 §3.4）。
7. **toRdd**（`:392`）：`lazyToRdd`（`:378`）以 `new SQLExecutionRDD(executedPlan.execute(), ...)`（`:379`）把物理計畫轉成 `RDD[InternalRow]`。`SparkPlan.execute()`（`SparkPlan.scala:197`）是 `final`，內部委派給各 operator 的 `doExecute(): RDD[InternalRow]`（`:343`）——**惰性建出 RDD DAG，此時尚未真正計算**。

> 注意 `lazyToRdd`/各階段用 `LazyTry`（`QueryExecution.scala:378`、`:192` 等）包裹，使得「失敗也只發生一次、結果被記住」，且 `clone()` 散落各階段（`:296`、`:319`、`:342`、`:361`），確保「analyzing/optimizing/planning 不共享同一個 plan 實例」——這是 Catalyst「同一棵 `TreeNode` 樹逐步重寫」模型下避免階段間互相污染的紀律。

### 3.3 SparkPlanner：strategies 而非 rule，配 `planLater` 遞迴

`createSparkPlan`（`:820`）呼叫的 `SparkPlanner.plan` 繼承自 `QueryPlanner.plan`（`sql/catalyst/.../planning/QueryPlanner.scala:59`）。其機制與 optimizer 的 rule batch 不同：

- `strategies`（`SparkPlanner.scala:38-58`）是一串有序策略，例如 `JoinSelection`（`:51`，決定 broadcast/sort-merge/shuffle-hash join）、`Aggregation`（`:48`）、`FileSourceStrategy`（`:45`）、`BasicOperators`（`:56`）等。
- `plan`（`QueryPlanner.scala:59-95`）對每個策略呼叫 `strategies.iterator.flatMap(_(plan))` 收集候選物理計畫（`:63`）；策略可用 `planLater(child)`（`GenericStrategy.planLater`，`:36`）丟一個佔位節點，之後由 `collectPlaceholders`（`SparkPlanner.scala:66`）找出、遞迴 `this.plan(logicalPlan)`（`QueryPlanner.scala:78`）把佔位換成實際子計畫。
- `prunePlans`（`SparkPlanner.scala:72`）目前是 no-op（註解 `:73` 直言「We will need to prune bad plans when we improve plan space exploration」），所以實務上就是「每層取第一個能成的策略」。

### 3.4 preparations：插 exchange、AQE 包裹、codegen 融合

`executedPlan`（步驟 6）跑的 `preparations`（`QueryExecution.scala:752-784`）是一串 `Rule[SparkPlan]`，順序很關鍵：

```
preparations（QueryExecution.scala:758-783）：
  InsertAdaptiveSparkPlan(...)        :758  ← AQE 包裹（若插入，AdaptiveSparkPlanExec 是 leaf，
                                              後續規則對它都變 no-op，:756-757 註解）
  CoalesceBucketsInJoin               :760
  PlanDynamicPruningFilters(...)      :761
  PlanSubqueries(...)                 :762
  RemoveRedundantProjects             :763
  EnsureRequirements()                :764  ← 依分佈/排序需求「插 ShuffleExchangeExec」
  InsertSortForLimitAndOffset         :766  （必須在 EnsureRequirements 之後）
  ReplaceHashWithSortAgg              :769
  RemoveRedundantSorts                :773
  …
  CollapseCodegenStages()             :778  ← 把 codegen 子樹融成 WholeStageCodegenExec
  ReuseExchangeAndSubquery            :782  （非 subquery 時）
```

- **EnsureRequirements（`:764`）= exchange 插入點**。其 class doc（`EnsureRequirements.scala:40`）：「ensures that … by **inserting [[ShuffleExchangeExec]]** Operators where required」。它比對每個 operator 的 `requiredChildDistributions`（`:59`）與子節點實際分佈，不符就在中間插 `ShuffleExchangeExec`（`:129`、`:133`）。這對應 arneb 的「fragment 邊界 = REMOTE exchange」，但 Spark 是在物理層用「分佈需求推導」自動插 shuffle，不是手動切 fragment（詳見 §7.5）。
- **prepareForExecution**（`:790-793`）用一個 `PhysicalRuleExecutor`（`:804`，`FixedPoint(1)` 而非 `Once`，註解 `:797-803` 解釋「preparation rules are not necessarily idempotent」，故用 `FixedPoint(1)` 跑一次但不做 idempotence 檢查）。

### 3.5 AQE：執行期再優化（其他引擎沒有的等價物）

`InsertAdaptiveSparkPlan`（`InsertAdaptiveSparkPlan.scala:44`）在 `shouldApplyAQE`（`:113`）且 `supportAdaptive`（`:129`）時，把整個計畫包進 `AdaptiveSparkPlanExec`（`:86`）。`AdaptiveSparkPlanExec` 的 class doc（`AdaptiveSparkPlanExec.scala:65-68`）描述其精髓：

> 「When one query stage finishes materialization, the **rest query is re-optimized and planned based on the latest statistics provided by all materialized stages**. Then we traverse the query plan again and create more stages if possible.」

具體迴圈（`getFinalPhysicalPlan`，`:284-398`）：

1. `createQueryStages`（`:284`）在 exchange 邊界把計畫切成 `QueryStageExec`（shuffle/broadcast stage）。
2. 非同步 `stage.materialize()`（`:309`）跑這些 stage，等 `events.take()`（`:334`）拿到完成事件——此時有了**真實的 shuffle 統計**（`ShuffleExchangeExec.mapOutputStatisticsFuture`，`ShuffleExchangeExec.scala:215`，回傳 `Future[MapOutputStatistics]`）。
3. 只要還沒到結果 stage，就 `reOptimize(logicalPlan)`（`:371`）重新最佳化「剩餘計畫」，比較新舊成本（`costEvaluator.evaluateCost`，`:374-375`），新計畫不更差就採用（`:376-378`）。
4. AQE 階段套用的物理規則（`queryStageOptimizerRules`，`:138-146`）包含 `CoalesceShufflePartitions`（`:142`，合併過小的 shuffle 分區）、`OptimizeSkewInRebalancePartitions`（`:141`，處理 skew）、`OptimizeSkewedJoin`（在 `queryStagePreparationRules`，`:132`）等——這些都是「**拿到真實資料量後才能做的決策**」。

對照其他四個引擎：Trino 只有 dynamic filtering（用 build side 的值在執行期過濾 probe side），**沒有「拿執行期統計回頭改物理計畫」**；arneb 也只有靜態成本模型。AQE 是 Spark 獨有的「規劃—執行—再規劃」回路（§5.3 詳述機制、§7.6 從分散式視角再看一次）。

### 3.6 從 RDD 到 stage：DAGScheduler 切 stage

`toRdd`（步驟 7）只是建出惰性的 `RDD[InternalRow]` DAG。真正觸發計算的是 action（`collect`/`count` 等），它走 `SparkContext.runJob`（`SparkContext.scala:2481`）→ `dagScheduler.runJob(rdd, ...)`（`:2496`）。`DAGScheduler`（`DAGScheduler.scala:124`）依 RDD 的 `ShuffleDependency` **在 shuffle 邊界切 stage**（doc `:87-91`）：`ShuffleMapStage`（寫 map output 檔）與最終的 `ResultStage`（執行 action）。每個 stage 的 task 由 `TaskScheduler`（`TaskScheduler.scala:36`，doc `:31-34`「get sets of tasks submitted to them from the DAGScheduler for each stage … sending the tasks to the cluster」）派到 Executor，由 thread-per-task 跑（§7.1、§10）。

> → 對 arneb 的啟發：
> 1. **【最高優先】AQE 是對 arneb 最重要的單一啟發**。arneb 的痛點清單裡，「靜態成本模型選錯 build side（q08 builds 90M）、`partition_count` 寫死、broadcast 歷史上給錯結果被停用」全部是「規劃時資訊不足」的病。Spark 的解法不是把靜態成本模型做得更準，而是**接受規劃時會猜錯、在執行期用真實統計修正**（`AdaptiveSparkPlanExec.scala:65-68`、`:371-378`）。arneb 已有 fragment 邊界（= REMOTE exchange），這正是天然的「QueryStage 切點」；可以在 fragment 完成、拿到真實輸出 row count/byte 後，對「尚未排程的下游 fragment」重跑成本模型——例如「小表實際只有 1 萬列 → 改成 broadcast 消掉那個 70s 的 lineitem shuffle」「shuffle 分區實際太碎 → coalesce」。這比繼續微調靜態 NDV 估算 ROI 高得多，且 arneb 的 Arrow Flight exchange 本就能在 fragment 邊界拿到統計。
> 2. **「命令 eager、查詢 lazy」的 `CommandExecutionMode` 分界**（`QueryExecution.scala:215-266`）值得借鏡——arneb 的 DDL/DML（CREATE/INSERT/DELETE）與 SELECT 應有明確的 eager/lazy 分流，避免 DDL 被當查詢一樣延後物化。
> 3. **生命週期做成「一串 lazy val + 階段間 clone」**（`QueryExecution.scala` 七個 lazy val + 各階段 `.clone()`）是極乾淨的設計：每階段只算一次、可單獨觀察（debug/EXPLAIN）、且不會互相污染計畫實例。arneb 若要做 `EXPLAIN ANALYZE` 或分階段 profiling，這種「lazy val 鏈」比命令式的 pipeline 更容易插點觀察。
> 4. **`EnsureRequirements` 的「分佈需求推導自動插 exchange」**（`EnsureRequirements.scala:40,59,129`）與 arneb「手動在 fragment 邊界放 exchange」是兩種風格。Spark 由 operator 宣告 `requiredChildDistribution`、框架自動補 shuffle，能避免「漏插」或「多插」exchange——arneb 的 `partition_count` 寫死、reshuffle 多餘，正是缺這層「分佈 property 推導」的症狀（與啟發 1 的 AQE 互補：`EnsureRequirements` 在靜態層插、AQE `CoalesceShufflePartitions` 在執行期修）。

---

## 4. SQL Parser 與 Analyzer / 語意分析

Spark Catalyst 與 Trino 在這一層的根本差異要先講清楚：**Trino 用一個獨立的 `Analysis` 容器累積語意資訊（型別、scope、table handle、lineage），而 plan 樹本身不變；Spark 則是把 parser 產出的 unresolved `LogicalPlan` 樹當成唯一資料結構，用一連串「解析 rule」反覆重寫同一棵 `TreeNode` 樹，直到沒有任何 unresolved 節點殘留為止**。Spark 沒有等價於 Trino `Analysis` 的旁路容器——語意資訊就「長」在 tree node 上（`UnresolvedAttribute` → `AttributeReference`、`UnresolvedRelation` → 具體 relation 節點），解析狀態由每個節點的 `resolved: Boolean` 標誌與整棵樹的固定點（fixed point）來表達。兩種風格殊途同歸於 `common.md` §1.1 的鐵律：Analyzer 之後必須產出「不再包含任何未解析 Identifier 的強型別 IR 樹」。

```
SQL 字串
  │  AbstractParser.parse()  (sql/api/.../parser/parsers.scala:59)
  ▼
ANTLR4 Lexer/Parser  (SqlBaseLexer.g4 / SqlBaseParser.g4)
  │  兩階段：SLL(快) → 失敗 fallback LL(全)
  ▼
ANTLR ParseTree (SqlBaseParser.QueryContext 等)
  │  AstBuilder visitor 走訪  (parser/AstBuilder.scala:67)
  ▼
unresolved LogicalPlan 樹  (UnresolvedRelation / UnresolvedAttribute / UnresolvedFunction ...)
  │  resolved == false
  ▼
Analyzer (RuleExecutor)  (analysis/Analyzer.scala:304)
  │  跑 Batch × Rule 到 fixed point，重寫「同一棵樹」
  ▼
resolved & typed LogicalPlan
  │  CheckAnalysis.checkAnalysis()  (analysis/CheckAnalysis.scala:306)
  ▼
analyzed plan（plan.setAnalyzed()）→ 交給 Optimizer
```

### 4.1 Parser：ANTLR4 文法 + AstBuilder visitor

**技術選型**：Spark 用 ANTLR4，文法拆成 lexer 與 parser 兩個 `.g4` 檔，放在 `sql/api` 模組（不是 `catalyst`）：

- `sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseLexer.g4`（721 行，已核實）
- `sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4`（2756 行，已核實；檔頭 `SqlBaseParser.g4:14` 註明此文法改編自 Presto 的 `SqlBase.g4`——與 Trino 同源）

文法的頂層規則層次（皆為實際觀察到的行號）：

| 規則 | 行 | 用途 |
|---|---|---|
| `singleStatement` | `SqlBaseParser.g4:184` | `(statement｜setResetStatement) SEMICOLON* EOF` |
| `statement` | `SqlBaseParser.g4:228` | 所有 DDL/DML/DQL 的分派點（帶 `#explain` 等 label）|
| `query` | `SqlBaseParser.g4:585` | 查詢主體 |
| `querySpecification` | `SqlBaseParser.g4:820` | `selectClause fromClause? whereClause? aggregationClause? havingClause? ...` |
| `selectClause` | `SqlBaseParser.g4:850` | `SELECT (hint)* setQuantifier? namedExpressionSeq` |
| `fromClause` | `SqlBaseParser.g4:919` | FROM 來源 |

**進入點與兩階段解析**：所有 ANTLR parser 的共同基底是 `AbstractParser`（`sql/api/src/main/scala/org/apache/spark/sql/catalyst/parser/parsers.scala:41`），核心 `parse[T]` 方法在 `parsers.scala:59`：它建一個 `SqlBaseLexer`（包在 `UpperCaseCharStream` 裡做大小寫不敏感）→ `CommonTokenStream` → `SqlBaseParser`，然後呼叫 `AbstractParser.executeWithTwoStageStrategy`（`parsers.scala:504`）。這個兩階段策略是效能關鍵：

```
第一階段 (parsers.scala:509-512):
  SparkParserBailErrorStrategy + PredictionMode.SLL   ← 快、但對歧義會 bail
        │ 拋 ParseCancellationException?
        ▼ 是
第二階段 (parsers.scala:514-520):
  tokenStream.seek(0) + parser.reset()
  SparkParserErrorStrategy + PredictionMode.LL         ← 慢、但能處理完整歧義並產生好錯誤訊息
```

對「絕大多數合法查詢」走 SLL 快路徑，只有少數需要完整 LL 預測的句法才付出第二趟成本。`parse` 的 `finally` 區塊（`parsers.scala:86` 起的註解明示）還會清掉 ANTLR 的無界快取，因為「ANTLR 用快取加速但快取不會被清，大量 SQL 會 OOM」。

**Concrete 的 catalyst 入口**：`CatalystSqlParser`（`parser/CatalystSqlParser.scala:22`，`object` 版在 `:27`）繼承 `AbstractSqlParser`（`parser/AbstractSqlParser.scala:34`）。`AbstractSqlParser` 把每個 public 解析方法對應到一條 grammar 規則 + 一個 AstBuilder visitor，例如：
- `parsePlan`（`AbstractSqlParser.scala:94`）→ `parser.compoundOrSingleStatement()` → `astBuilder.visitCompoundOrSingleStatement(ctx)`
- `parseExpression`（`:38`）→ `parser.singleExpression()` → `astBuilder.visitSingleExpression(ctx)`

**AstBuilder = visitor，建出 unresolved 節點**：`AstBuilder`（`parser/AstBuilder.scala:67`，繼承 `DataTypeAstBuilder`，全檔 7700 行）走訪 ANTLR ParseTree，產出 Catalyst 自家的 `LogicalPlan` / `Expression`。**關鍵：此階段對「名稱」一無所知，一律建出 unresolved 佔位節點**：
- 表名 → `UnresolvedRelation`（`createUnresolvedRelation`，被 `visitTableName` 在 `AstBuilder.scala:2636` 呼叫）
- 欄位參照 → `UnresolvedAttribute`（`visitColumnReference`，`AstBuilder.scala:4026`；fallback 在 `:3605`）
- 函式呼叫 → `UnresolvedFunction`（`visitFunctionCall`，`AstBuilder.scala:3747`，建構在 `:3789`）
- `*` → `UnresolvedStar`（`visitStar`，`AstBuilder.scala:3205`）

`SELECT ... FROM ... WHERE ... GROUP BY ...` 在 `withSelectQuerySpecification`（`AstBuilder.scala:1745`）被組成 plan：`Project`（`:1818`）疊在 `Filter`（WHERE）上，`GROUP BY` 則建成 `Aggregate`（`:1835`-`:1837`），全部由 unresolved 子節點構成。AST 與文法解耦：AstBuilder 產出的節點完全不依賴 ANTLR 的 context 型別（`QueryContext` 只在 visitor 簽章出現）。

> → 對 arneb 的啟發：arneb 用 sqlparser-rs 產 AST 再轉 LogicalPlan，本質與 Spark「文法 / AST 解耦」一致。真正可借鏡的是兩處工程細節：(1) **兩階段 SLL→LL 解析**——arneb 若未來自訂文法，對 happy path 用快策略、僅在歧義 fallback 完整策略，是免費的延遲降低；(2) **parser 快取會 OOM 的教訓**直接呼應 arneb 反覆踩到的「未追蹤 Arrow 配置撞 cgroup」痛點——任何「為了加速而設、卻無界、又不會被清」的快取（ANTLR ATN cache ↔ arneb 的 channel buffer / JoinHashMap hashbrown anon）都是 OOM 來源；Spark 的解法是「每次 parse 後在 `finally` 主動清快取」，arneb 對 per-operator 的 Arrow buffer 也該有等價的「生命週期結束即歸還 MemoryPool」紀律，而非寄望 allocator 自動回收（呼應 `common.md` §5 統一記帳鐵律）。

### 4.2 Analyzer：一個 RuleExecutor，把 unresolved 樹重寫到 fixed point

`Analyzer`（`analysis/Analyzer.scala:304`）繼承 `RuleExecutor[LogicalPlan]` 並 mixin `CheckAnalysis`。它持有 `catalogManager`，內部建 `RelationResolution` 與 `FunctionResolution`（`Analyzer.scala:312`-`:314`）——這就是它與 catalog 對話的橋。

**Rule / Batch / Strategy 三層結構**（在 `rules/RuleExecutor.scala`）：
- `Strategy`（`RuleExecutor.scala:137`）有兩種：`Once`（`:150`，`maxIterations = 1`）與 `FixedPoint(maxIterations, ...)`（`:156`）。
- `Batch(name, strategy, rules*)`（`RuleExecutor.scala:162`）把一組 rule 綁到一個 strategy。
- Analyzer 的 `fixedPoint`（`Analyzer.scala:414`）以 `conf.analyzerMaxIterations` 為上限、`errorOnExceed = true`、並把超限提示綁到設定鍵 `SQLConf.ANALYZER_MAX_ITERATIONS`。

**batches 清單**（`Analyzer.scala:506`）依序為：`earlyBatches`（含 `Substitution`/CTE、`Hints`、`Unresolve Relations` 等，`Analyzer.scala:481`-`:505`）→ 巨大的 **`Batch("Resolution", fixedPoint, ...)`**（`Analyzer.scala:507`）→ 之後一連串收尾 batch（`Remove TempResolvedColumn`、`Post-Hoc Resolution`、`Subquery`、`Cleanup` 等，`:583`-`:609`）。`Resolution` 這個 batch 內塞了數十條 rule（`:508`-`:582`），按固定順序排列，重點包括：

| Rule（皆在 `Analyzer.scala`）| 行 | 職責 |
|---|---|---|
| `ResolveRelations` | 列於 `:510`，定義在 `:1039` | `UnresolvedRelation` → 查 catalog 換成具體 relation / view |
| `ResolveReferences` | 列於 `:519`，定義在 `:1492` | `UnresolvedAttribute` → 綁到子節點 output 的 `AttributeReference` |
| `ResolveFunctions` | 列於 `:534`，定義在 `:2280` | `UnresolvedFunction` → 查函式登錄表換成具體表達式 |
| `ResolveAliases` / `ResolveSubquery` / `ResolveAggregateFunctions` ... | `:541`-`:556` | 別名、子查詢、聚合等 |
| `typeCoercionRules()` | 展開於 `:576` | 隱式型別轉換（見 §4.4）|

**為什麼是「同一棵樹反覆重寫」**：`RuleExecutor.execute`（`RuleExecutor.scala:215`）對每個 batch 跑一個 `while (continue)` 迴圈（`:244`）：用 `foldLeft` 把 batch 裡每條 rule 套到當前 plan（`:245`-`:248`），一條 rule 若改了樹（`!result.fastEquals(plan)`，`:250`）就記為 effective。迴圈停止條件有二：(1) `iteration > maxIterations`（`:286`，FixedPoint 超限會依 `errorOnExceed` 拋例外）；(2) **`curPlan.fastEquals(lastPlan)`——這趟沒有任何 rule 改動樹，即達到固定點**（`:312`-`:316`）。`Once` batch 在測試模式下還會跑 `checkBatchIdempotence`（`:305`-`:307`）確保「再跑一次不會再變」（這正是「Once batch 要 idempotent」）。

```
Resolution batch (FixedPoint):
  iteration 1: ResolveRelations 把 UnresolvedRelation→relation
               → relation 帶來 output 欄位
  iteration 2: ResolveReferences 現在才能把 UnresolvedAttribute 綁到 output
               → 解析後型別才確定
  iteration 3: typeCoercionRules 依型別插入 Cast
  ...
  iteration N: 整趟無變更 → fastEquals → fixed point → 收工
```

名稱解析有先後依賴（要先有 relation 的 output 才能解 column），所以「跑到固定點」不是優雅，而是**必要**：rule 之間用「同一棵樹的漸進收斂」隱式表達依賴，而非寫死的拓樸排序。

> → 對 arneb 的啟發：arneb 的 LogicalOptimizer 已是 rule-pass 風格，這裡最值得抄的是 RuleExecutor 的兩個硬約束：(1) **固定點偵測 = `fastEquals(lastPlan)` + `maxIterations` 雙保險**——arneb 任何 rule pass 都該有「無變更即停 + 迭代上限拋錯（對應 Trino `OPTIMIZER_TIMEOUT`）」，否則一條會反覆改寫的 rule 會無限迴圈或靜默吃 CPU；(2) **Once batch 的 idempotence 檢查**直接對應全域守則「無效就回傳原樣 / None」——arneb 的新 analyzer rule（如 `decorrelated_agg_to_window`）若不是冪等的，放進 fixed-point 迴圈就會抖動。Spark 用 `effective = !result.fastEquals(plan)` 量測每條 rule 是否真的改了樹，arneb 也可用同樣的 cheap structural-equality 來決定是否繼續迴圈、並順帶得到「哪條 rule 最常觸發 / 最耗時」的 per-rule profile（Spark 的 `queryExecutionMetrics`，`RuleExecutor.scala:253`-`:278`）。

### 4.3 名稱解析：Analyzer 對 SessionCatalog 的查詢

`ResolveRelations`（`Analyzer.scala:1039`）負責把 `UnresolvedRelation` 換成真實 relation。底層由 `SessionCatalog`（`catalog/SessionCatalog.scala:87`）提供查表能力，其 `lookupRelation`（`SessionCatalog.scala:1103`）做三件事：先判斷是不是 global temp view（`:1107`）、再判斷 temp view（`:1111`、`:1115`），否則才去 `externalCatalog.getTable(db, table)`（`:1112`）拿持久表的 `CatalogTable` metadata 並包成 relation（`getRelation`，`:1119`）。`SessionCatalog` 也管 current database（`currentDb`，`:312`；`getCurrentDatabase`，`:524`）與函式查詢（`lookupBuiltinOrTempFunction`，`:2578`）。

`ResolveReferences`（`Analyzer.scala:1492`）把 `UnresolvedAttribute` 綁到子節點 output 的某個 `AttributeReference`；它特別處理 self-join 的欄位歧義——`hasConflictingAttrs`（`:1513`）偵測子節點間 `ExprId` 衝突，**並刻意等 `DeduplicateRelations` 先消歧義**（`:1509`-`:1511` 註解），否則無法正確解析。`ResolveFunctions`（`Analyzer.scala:2280`）用 `resolveOperatorsUpWithPruning` 由下而上找 `UnresolvedFunction`/`UnresolvedFunctionName`，先查 builtin/temp 函式（`functionResolution.lookupBuiltinOrTempFunction`，`:2286`），查不到再展開 identifier 去 catalog 載入持久函式（`:2290`-`:2294`）。

> → 對 arneb 的啟發：Spark 把「current database / temp view / 持久表」的解析優先序明確寫在 `lookupRelation` 一個方法裡（temp view 蓋過持久表、global temp 走獨立 namespace），這正是 arneb `CatalogManager` 3-part 解析需要的清晰契約。更關鍵的是 `ResolveReferences` 對 **self-join 欄位歧義**的處理：它不在解析欄位時硬猜，而是先靠 `DeduplicateRelations` 給每個 relation 實例配發唯一 `ExprId` 再解析——這對應 arneb 記憶中「巢狀子查詢 / 重複欄名」的欄位解析含混（join_reorder 對 `has_duplicate_leaf_column_names` 直接 bail 的 q08 痛點）。Spark 的答案是「在 analyzer 階段就用穩定的 `ExprId` 把每個 column reference 解析到唯一目標」，arneb 若想讓 join reorder 不被重複欄名卡住，根因解法也是 leaf-origin / 唯一 id 追蹤，而非在規劃期繞開（呼應 Trino 的 `Scope`/`Symbol` 機制與 `common.md` §1.1）。

### 4.4 型別系統與隱式型別轉換（TypeCoercion）

Spark 的隱式型別轉換不是散落各處，而是被打包成一組 rule，再依 ANSI 模式二選一：`typeCoercionRules()`（`Analyzer.scala:474`）在 `conf.ansiEnabled` 時用 `AnsiTypeCoercion.typeCoercionRules`（`AnsiTypeCoercion.scala:77`），否則用 `TypeCoercion.typeCoercionRules`（`TypeCoercion.scala:49`）。兩者都是 `TypeCoercionBase` 的子物件。

非 ANSI 的 `TypeCoercion.typeCoercionRules`（`TypeCoercion.scala:49`-`:72`）是一串子 rule，包成一個 `CombinedTypeCoercionRule`：`PromoteStrings`、`DecimalPrecision`、`BooleanEquality`、`FunctionArgumentConversion`、`CaseWhenCoercion`、`Division`、`ImplicitTypeCasts` 等。核心邏輯是 `findTightestCommonType`（`TypeCoercion.scala:76`）——例如 `INT` 與 `DECIMAL` 取較寬者、兩個 `NumericType` 依 `numericPrecedence` 升到較高者（`:88`-`:92`）。這些 rule 一樣跑在 `Resolution` 的 fixed-point 迴圈裡，所以「插入 `Cast` → 改變型別 → 觸發下一條 coercion」會自然收斂。ANSI 模式（`AnsiTypeCoercion`，`:76`）規則更嚴、更貼近 SQL 標準，不做寬鬆的 string↔numeric 隱式推升。

> → 對 arneb 的啟發：Spark 把「ANSI / 非 ANSI 型別轉換」做成可整組替換的 rule 集合（同一個 `typeCoercionRules()` 介面、兩種實作），是乾淨的策略模式——arneb 若要同時支援 pgwire 的 PostgreSQL 語意與其他方言的型別規則，這種「一個介面、依設定選整組 rule」比在表達式求值器裡灑 `if ansi` 分支更可維護。另外 `findTightestCommonType` 的「最窄共同型別」是 binary op / CASE / UNION 型別統一的通用基礎，arneb 的隱式 cast 邏輯可直接對齊此語意以與 Trino/Spark 結果一致（對 TPC-H cell-diff 通過率有直接幫助）。

### 4.5 CheckAnalysis：解析完成後的驗證關卡

跑完所有 batch 後，`CheckAnalysis.checkAnalysis`（`CheckAnalysis.scala:306`）做最終驗證，成功才呼叫 `plan.setAnalyzed()`（`:327`）標記為 analyzed。它先 inline 所有 CTE（`:310`-`:312`）以還原可比對的 plan 形狀，再呼叫 `checkAnalysis0`（`:330`）對整棵樹巡檢。**驗證手法正是「樹上還有沒有 unresolved 節點」**：
- `plan.foreach` 由上而下先抓 relation 級錯誤：`case u: UnresolvedRelation`（`CheckAnalysis.scala:409`）→ `u.tableNotFound(...)`（`TABLE_OR_VIEW_NOT_FOUND`）；`case u: UnresolvedFunctionName`（`:414`）→ `unresolvedRoutineError`。
- 對每個 operator 的所有表達式 `foreachUp`：`case a: Attribute if !a.resolved`（`:535`）→ `failUnresolvedAttribute(..., "UNRESOLVED_COLUMN")`；殘留的 `Star`（`:538`）也報錯。

換句話說，Spark 不需要旁路容器來追蹤「哪些東西沒解析」——**只要重寫後的樹裡還有任何 `resolved == false` 的節點，CheckAnalysis 就 fail**。所有 unresolved 節點的 `resolved` 都恆為 `false`（`unresolved.scala:50` 的 `UnresolvedNode` trait；`UnresolvedRelation` 在 `:132`、`UnresolvedAttribute` 在 `:295`、`UnresolvedFunction` 在 `:385`），一旦 rule 把它換成具體節點，`resolved` 才會變 `true`。CheckAnalysis 失敗統一走 `failAnalysis(errorClass, ...)`（`:65`）拋帶 error class 的 `AnalysisException`。

整個語意分析的對外入口是 `Analyzer.executeAndCheck`（`Analyzer.scala:331`）：若 `plan.analyzed` 已是 true 就直接回（`:332`），否則跑 `runAnalysis()`（內部走 `HybridAnalyzer`，`:335`）。`QueryExecution` 的 `analyzed` lazy val（`QueryExecution.scala:211`）就是呼叫 `analyzer.executeAndCheck(...)`（`:200`）取得 analyzed plan，再往下交給 optimizer。

> → 對 arneb 的啟發：Spark 的「驗證 = 掃整棵樹找殘留 unresolved 標誌」是極簡又強健的設計——把「是否解析完成」做成節點本身的不變量（`resolved` 旗標），而非外部帳本，就不會有「帳本與樹不同步」的 bug。對照 arneb 反覆踩到的 silent-truncation / 靜默產生錯誤結果（exchange consumer drop、q21 非確定性 row-drop），核心教訓是一致的：**寧可在一個明確的關卡 fail loud（CheckAnalysis 拋 `AnalysisException` with error class），也不要讓未完成 / 不一致的狀態靜默流到下游**。arneb 已用 `must_drain` 把 exchange 的靜默截斷改成 fail-loud，與 CheckAnalysis 的精神同源；可再借鏡的是把「不變量檢查」做成跨階段的固定關卡（Spark 在每條 rule 後還可選跑 `validatePlanChanges`，`RuleExecutor.scala:257`-`:275`），而非只靠端到端結果比對才發現問題。值得注意的反差：Spark 是 JVM 單機 driver 上做這整套分析，沒有 arneb 的分散式 / async 約束，所以這層的設計可以幾乎原封不動借鏡，無需擔心 tokio task / semaphore / back-pressure 互動。

---

## 5. 查詢規劃與最佳化（Catalyst Optimizer + CBO + AQE）

Spark SQL 的規劃管線是一棵 `TreeNode` 樹被一連串 rule 逐步重寫：unresolved `LogicalPlan` → resolved → **optimized `LogicalPlan`**（本章主角）→ `SparkPlan`（物理計畫）。最佳化分成三個層次，重要性對 arneb 也由低到高遞增：

```
            optimized LogicalPlan
                    │
   ┌────────────────┴─────────────────────────────────────────┐
   │ (1) RBO — Catalyst RuleExecutor                            │  規劃期、規則為主
   │     defaultBatches: Batch[FixedPoint | Once] × N rules     │
   │     跑到 fixed point；Once batch 必須 idempotent           │
   ├────────────────────────────────────────────────────────────┤
   │ (2) CBO — CostBasedJoinReorder（FixedPoint(1) batch）       │  規劃期、成本為輔
   │     只在 spark.sql.cbo.enabled && joinReorder.enabled       │  （預設 OFF）
   │     且每個 item 都有 rowCount 時，用 Statistics 做 Selinger DP│
   └────────────────────────────────────────────────────────────┘
                    │  SparkPlanner（strategies）→ 初始 SparkPlan
                    ▼
   ╔════════════════════════════════════════════════════════════╗
   ║ (3) AQE — AdaptiveSparkPlanExec                             ║  ★ 執行期再優化
   ║     在 exchange 邊界切 QueryStage，跑完拿真實                ║  （預設 ON）
   ║     MapOutputStatistics 對「剩餘計畫」重新套 physical rule  ║  其他四引擎皆無
   ╚════════════════════════════════════════════════════════════╝
```

### 5.1 RBO 第一層：TreeNode 重寫、Batch 分組、RuleExecutor 跑 FixedPoint

Catalyst 的核心抽象是「對 `TreeNode` 樹反覆套 rule」。每條 rule 就是一個純函式 `LogicalPlan => LogicalPlan`：

- `Rule[TreeType]`（`sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/rules/Rule.scala:24`）的唯一抽象方法是 `def apply(plan: TreeType): TreeType`（`Rule.scala:35`）。**契約**：rule 若無事可做，必須回傳「結構等價的原樹」（通常就是 `return plan`）。這正對應 Trino pushdown 的 `Optional.empty()`「無效回原樣」契約——只是 Catalyst 用「回傳同一棵樹」表達，而非 `Option`。
- 每條 rule 有一個 `ruleName`（`Rule.scala:30`）與 `lazy val ruleId`（`Rule.scala:27`），後者讓 `transformDownWithPruning(...)` 能跳過「已被本 rule 處理過」的子樹，避免重複下推（CostBasedJoinReorder 也用到，見 §5.2）。

rule 不是散落執行的，而是以 **Batch** 分組，每個 Batch 綁一個執行 **Strategy**。讀 `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/rules/RuleExecutor.scala`：

```
RuleExecutor[TreeType]
 ├─ abstract class Strategy { def maxIterations: Int }      (:137)
 │    ├─ case object Once  { maxIterations = 1 }            (:150)  跑一次、且須 idempotent
 │    └─ case class FixedPoint(maxIterations, errorOnExceed, …)  (:156)  跑到收斂或上限
 ├─ case class Batch(name, strategy, rules: Rule*)          (:162)
 └─ def batches: Seq[Batch]                                 (:165)  由子類別定義
```

`def execute(plan)`（`RuleExecutor.scala:215`）是引擎心臟，邏輯精準如下：

1. **依序跑每個 Batch**（`:237`），Batch 之間是串列的。
2. Batch 內 `while (continue)` 迴圈（`:244`）：每輪用 `foldLeft` 把 batch 內所有 rule 依序套到 `curPlan`（`:245`–`:248`）。
3. **effective 判定**：`val effective = !result.fastEquals(plan)`（`:250`）——靠 `fastEquals` 比對「rule 跑完是否真的改變了樹」。有效才記 metric（`incNumEffectiveExecution`，`:253`）並透過 `tracker.recordRuleInvocation(...)`（`:281`）記錄每條 rule 的呼叫次數、耗時、是否有效。這套「effective rules 追蹤」是 Spark 規劃可觀測性的基礎（`QueryExecution.tracker` 會列出每條 rule 的累計耗時）。
4. **收斂偵測**：`if (curPlan.fastEquals(lastPlan))`（`:312`）代表本輪沒有任何 rule 再改變樹 → fixed point 達成，`continue = false`。FixedPoint batch 因此會反覆跑直到收斂或撞 `maxIterations`。
5. **maxIterations 保護**：`if (iteration > batch.strategy.maxIterations)`（`:286`）超過上限時，依 `errorOnExceed` / `Utils.isTesting` 決定拋 `RuntimeException`（`:299`）或 `logWarning`（`:301`），並提示「請把 `spark.sql.optimizer.maxIterations` 調大」（`:289`–`:294`）。`Optimizer` 的 `fixedPoint` 預設取 `conf.optimizerMaxIterations`（`Optimizer.scala:73`–`:76`）。

**Once batch 必須 idempotent，且這是會被檢查的硬契約：**

```scala
// RuleExecutor.scala:192
private def checkBatchIdempotence(batch: Batch, plan: TreeType): Unit = {
  val reOptimized = batch.rules.foldLeft(plan) { case (p, rule) => rule(p) }
  if (!plan.fastEquals(reOptimized)) {
    throw QueryExecutionErrors.onceStrategyIdempotenceIsBrokenForBatchError(...)  // :195
  }
}
```

它在 `batch.strategy == Once && Utils.isTesting && !excludedOnceBatches.contains(batch.name)` 時被呼叫（`RuleExecutor.scala:304`–`:307`）。語意是：**「跑一次就該收斂」的 Once batch，若再跑一次還會改變樹，就是 bug**，測試會直接拋例外。少數天生不冪等的 batch 列在 `excludedOnceBatches`（`Optimizer.scala:66`–`:71`：`PartitionPruning`、`RewriteSubquery`、`Extract Python UDFs`、`Infer Filters`）；正因如此，Spark 把某些原本是 `Once` 的 batch 刻意改成 `FixedPoint(1)`（語意上跑一次但**不**做冪等檢查），原始碼註解寫得很清楚——例如 `Batch("Subquery", FixedPoint(1), ...)`（`Optimizer.scala:212`–`:213`：「to enforce idempotence on it ... we change this batch from Once to FixedPoint(1)」）與 `Batch("Join Reorder", FixedPoint(1), CostBasedJoinReorder)`（`Optimizer.scala:246`–`:248`）。

`Optimizer.defaultBatches`（`Optimizer.scala:100`）就是這些 Batch 的清單，核心是 `operatorOptimizationRuleSet`（`Optimizer.scala:101`–`:162`）——一大包 RBO rule：`PushDownPredicates`、`ColumnPruning`、`PushProjectionThroughUnion`、`LimitPushDown`、`ConstantFolding`、`BooleanSimplification`、`PruneFilters`、`CollapseProject`…等，被包進兩個 `FixedPoint` batch（推 Infer Filters 之前與之後各跑一次，`Optimizer.scala:164`–`:174`）。`final override def batches`（`Optimizer.scala:540`）再依 `spark.sql.optimizer.excludedRules` / `nonExcludableRules` 過濾出實際要跑的 batch。`SparkOptimizer`（`sql/core/src/main/scala/org/apache/spark/sql/execution/SparkOptimizer.scala:31`）繼承 `Optimizer` 並加上資料源相關的 early pushdown（`earlyScanPushDownRules`：`SchemaPruning`、`V2ScanRelationPushDown`，`SparkOptimizer.scala:37`–`:47`）。

> → 對 arneb 的啟發：
> 1. **「rule 無效必回傳原樹」是 rule framework 的硬約束**——arneb 的 analyzer rule（如 `decorrelated_agg_to_window`）與 `PredicatePushdown` 若採類似 pass 架構，務必嚴守此契約，否則 fixed-point 迴圈會抖動或不收斂。Spark 用 `fastEquals` 偵測收斂、用 `maxIterations` 兜底、用 `checkBatchIdempotence` 在測試期主動抓「該冪等卻不冪等」的 rule——這三件事 arneb 可以照抄成 rule-framework 的測試基建（特別是 idempotence 檢查，能在 CI 就抓出「跑兩次結果不同」這類 plan-rewrite bug，比事後在 SF30 才發現 silent 行為差異便宜太多）。
> 2. **effective-rules 追蹤**（`recordRuleInvocation` 記每條 rule 是否真的改了樹、耗多久）正是 arneb 規劃期缺的可觀測性。arneb 現在靠 `EXPLAIN ANALYZE` / `--profile` 看執行期 per-operator 成本，但規劃期哪條 rule 在做事、哪條空轉、optimizer 是否逼近迴圈上限，目前沒有對應儀表；Spark 的 `QueryPlanningTracker` 是現成範本。

### 5.2 CBO 第二層：CostBasedJoinReorder（預設 OFF，需 stats，Selinger DP）

Spark 的 CBO 範圍其實**很窄**——核心就是 join 重排，由 `CostBasedJoinReorder`（`sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/CostBasedJoinReorder.scala:36`）一條 rule 承擔，且**預設關閉**。它的啟動門檻是三道 gate 全過：

```scala
// CostBasedJoinReorder.scala:38
def apply(plan: LogicalPlan): LogicalPlan = {
  if (!conf.cboEnabled || !conf.joinReorderEnabled) {  // :39  兩個開關都要 true
    plan                                                // 無效 → 回原樹（§5.1 的契約）
  } else { ... plan.transformDownWithPruning(_.containsPattern(INNER_LIKE_JOIN), ruleId) {...} }
}
```

- `spark.sql.cbo.enabled` 預設 **`false`**（`SQLConf.scala:4118`–`:4123`）。
- `spark.sql.cbo.joinReorder.enabled` 預設 **`false`**（`SQLConf.scala:4132`–`:4137`）。
- 即使開了，`reorder(...)`（`CostBasedJoinReorder.scala:58`）還要 `items.forall(_.stats.rowCount.isDefined)`（`:64`）——**每個 join item 都必須有 rowCount 統計**，否則整段放棄、回傳原樹（`:66`–`:67`）；item 數量還要落在 `2 < n <= conf.joinReorderDPThreshold`（`:63`，DP threshold 預設 **12**，`SQLConf.scala:4145`）。

統計從哪來？答案在 `Statistics`（`sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/Statistics.scala:55`）：

```scala
case class Statistics(
    sizeInBytes: BigInt,                                    // :56  物理大小
    rowCount: Option[BigInt] = None,                        // :57  估計列數
    attributeStats: AttributeMap[ColumnStat] = AttributeMap(Nil),  // :58  每欄統計
    isRuntime: Boolean = false)                             // :59  ★ 是否來自 AQE 執行期統計
```

`attributeStats` 的值是 `ColumnStat`（`Statistics.scala:95`），帶 `distinctCount`（NDV）、`min`/`max`、`nullCount`、`avgLen`/`maxLen`、`histogram`（`:96`–`:102`）。統計怎麼自底而上傳播，由 `LogicalPlanStats.stats`（`sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/statsEstimation/LogicalPlanStats.scala:33`）依 CBO 開關分流：

```scala
def stats: Statistics = statsCache.getOrElse {
  if (conf.cboEnabled) {
    statsCache = Option(BasicStatsPlanVisitor.visit(self))        // :35  完整基數估算
  } else {
    statsCache = Option(SizeInBytesOnlyStatsPlanVisitor.visit(self))  // :37  只估 sizeInBytes
  }
  ...
}
```

`BasicStatsPlanVisitor`（`statsEstimation/BasicStatsPlanVisitor.scala:25`）對每種 plan node 呼叫專屬 estimator，並在估不出來時 `fallback` 回 `SizeInBytesOnlyStatsPlanVisitor`（`:28`）：`visitFilter` → `FilterEstimation(p).estimate.getOrElse(fallback)`（`:55`–`:56`）、`visitJoin` → `JoinEstimation(p).estimate.getOrElse(fallback)`（`:77`–`:78`）、`visitProject`/`visitAggregate`/`visitUnion` 同理（`:85`–`:100`）。其中：
- `FilterEstimation`（`statsEstimation/FilterEstimation.scala:30`）依謂詞算選擇率（`calculateFilterSelectivity`，`:94`），對不支援的條件保守給 100% 選擇率（`:49`）；複雜情況用 histogram（`computeEqualityPossibilityByHistogram`，`:575`）。
- `JoinEstimation`（`statsEstimation/JoinEstimation.scala:31`）的 `estimateInnerOuterJoin`（`:55`）以 join key 的 NDV 推 inner join 基數，再依 join type 修正（`:68`–`:81`）。

DP 演算法本身是 **Selinger**（`JoinReorderDP`，`CostBasedJoinReorder.scala:143`；原始碼註解明確引用 1979 年 System R 論文，`:117`–`:119`）：從 level 0 的單表開始，逐層 build 2-way、3-way…n-way join，每層對「相同 item 集合」只保留成本最低的計畫，並剪掉沒有 join condition 的 cartesian product 候選（`:125`–`:138`）。成本模型刻意簡單——`Cost(card, size)`（`CostBasedJoinReorder.scala:388`，只有基數與位元組大小，**沒有 CPU/Network 維度**，原始碼註解 `:140`–`:142`：「physical costs for operators are not available currently」）；兩計畫比較用 row-ratio 與 size-ratio 的**加權幾何平均**（`betterThan`，`:370`–`:378`，權重來自 `conf.joinReorderCardWeight`）。

> → 對 arneb 的啟發：
> 1. **CBO 的範圍與信賴度要誠實**。Spark 連 join 重排 CBO 都**預設關閉**，且只在「每個 item 都有 rowCount」時才敢動手，否則整段放棄回原樹——這是一種「沒有可信統計就不要亂動」的保守紀律。arneb 反過來：它的 Selinger DP join reorder + partition-aware cost 是**靜態、無 gate、總是生效**的，而記憶中 q08 正是被靜態成本模型選錯 build side（builds 90M、probe 20K）、broadcast 歷史上甚至會給錯結果而被停用。Spark 的教訓是——**當統計可能不準時，CBO 的正確姿勢是「條件嚴格 + 估不準就 fallback 到只看 size」**（`BasicStatsPlanVisitor` 的 `getOrElse(fallback)` 模式），而不是硬套一個可能錯的 cost 數字去翻轉 build side。
> 2. **stats 與 size-only 兩條路分流**（`cboEnabled` ? `BasicStatsPlanVisitor` : `SizeInBytesOnlyStatsPlanVisitor`）值得照抄：arneb 的 NDV 估算可作為「有統計」路徑，缺統計時退回純 size 估算，避免在沒有 NDV 時 DP 拿著假基數亂排。
> 3. 但真正的結論是：**Spark 知道靜態 CBO 不可靠，所以把賭注押在 AQE（下一節）**。arneb 想根治 q08 選錯 build side，與其繼續加強靜態 cost model，不如看 §5.3——用執行期真實統計回頭改計畫，才是 Spark 對「估錯」的最終答案。
> 4. **rule batch 的排序因果要明確（arneb 已對齊，此為 regression guard 而非 bug 修復）**——Catalyst 明文規定「會用到 stats 的 rule 必須排在 pushdown 之後」（`Optimizer.scala:241`「Anything that uses stats must run after」、`:250`「relies on accurate stats... DSv2 relations only report stats after V2ScanRelationPushDown」），因為 filter/projection 下推後 leaf 基數才準。arneb **已經這麼做**：`crates/planner/src/analyzer/mod.rs:469-470` 的 doc 明寫「PredicatePushdown → JoinReorder, Pushdown precedes JoinReorder so the cost-based DP sees post-pushdown leaf cardinalities」，pipeline 順序為 TypeCoercion → PredicatePushdown（`:507`）→ … → JoinReorder。價值在 **regression guard**：若未來某 pass 重排把成本估算移到 PredicatePushdown 之前，leaf 基數會被高估（過濾還沒套）→ JoinReorder 可能選錯 build side。**須誠實標明：這不是現存 q08 bug 的修復**——q08 選錯 build side 已 root-cause 為 JoinReorder bail on duplicate leaf column names + dynamic-filter name-based injection（見 §12.7），與 pass 排序無關。

### 5.3 AQE 第三層（★ 重點）：執行期再優化——其他四引擎都沒有的能力

這是 Spark 相對 Trino / DuckDB / ClickHouse / DataFusion 最獨特的一手：**在查詢執行到一半時，拿真實 runtime 統計回頭重寫剩餘的物理計畫**。Trino 只有 dynamic filtering（執行期把 build 側的值域當 filter 推給 probe 側 scan），它**不會改變計畫形狀**；AQE 會真的把 sort-merge join 換成 broadcast join、把 200 個 shuffle 分區合併成 5 個、把傾斜分區切開。`spark.sql.adaptive.enabled` **預設 `true`**（`SQLConf.scala:1045`–`:1050`）。

#### 入口：在 exchange 邊界包上 AdaptiveSparkPlanExec

`InsertAdaptiveSparkPlan`（`sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/InsertAdaptiveSparkPlan.scala:44`）是一條 physical rule，把整棵 `SparkPlan` 包進一個 `AdaptiveSparkPlanExec` 節點（`:86`）。`shouldApplyAQE`（`:113`）的判定：計畫裡有 `Exchange`（`:116`）或子查詢（`:124`）才值得 adaptive——因為**有 exchange 才有「stage 邊界」可以切**。

#### 核心循環：切 stage → 物化 → 拿真實統計 → 重新規劃剩餘

`AdaptiveSparkPlanExec`（`sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala:70`）的 `withFinalPlanUpdate` → 主循環（`:276`–`:394`）是整個機制的心臟：

```
createQueryStages(currentPhysicalPlan, firstRun=true)          (:284)
        │  在每個 exchange 處切出一個 QueryStage（葉子先就緒的）
        ▼
while (!result.allChildStagesMaterialized) {                   (:288)
   currentPhysicalPlan = result.newPlan                        (:289)
   newStages.foreach { stage => stage.materialize() }          (:307–:309)  ← 真的去跑 shuffle map 階段
        │  (broadcast stage 排前面避免 timeout, :299–:304)
   val nextMsg = events.take()                                 (:334)       ← 阻塞等任一 stage 跑完
   ...
   if (!currentPhysicalPlan.isInstanceOf[ResultQueryStageExec]) {           (:351)
     val logicalPlan = replaceWithQueryStagesInLogicalPlan(...)            (:363)  已完成 stage 換成帶真實統計的 LogicalQueryStage
     val afterReOptimize = reOptimize(logicalPlan)             (:371)      ★ 對剩餘計畫重新規劃
     if (newCost < origCost || (newCost == origCost && plan 變了)) {       (:374–:378)
        currentPhysicalPlan = newPhysicalPlan                  (:383)      ← 只在新計畫不更差時採用
     }
   }
   result = createQueryStages(currentPhysicalPlan, firstRun=false)         (:393)  ← 再切下一批 stage
}
```

兩個關鍵點：

1. **真實統計從哪來**：每個 `ShuffleQueryStageExec`（`QueryStageExec.scala:198`）`materialize()` 後（`doMaterialize` = `shuffle.submitShuffleJob()`，`:212`），其 `mapStats: Option[MapOutputStatistics]`（`:231`–`:235`）就拿得到**每個 shuffle 分區的實際位元組數 `bytesByPartitionId`**——這是 map 端真的寫完磁碟檔之後的精確值，不是估的。`computeStats()`（`:84`）把它包成 `Statistics(isRuntime = true)`（對應 §5.2 的 `Statistics.isRuntime` 欄位），再 `replaceWithQueryStagesInLogicalPlan` 把已完成的子樹換成帶真實統計的 `LogicalQueryStage`，餵回 `reOptimize`。

2. **reOptimize 是「logical 重優化 + 重新物理規劃」的完整一輪**（`AdaptiveSparkPlanExec.scala:813`）：先 `logicalPlan.invalidateStatsCache()`（`:815`，丟掉舊估算）→ `optimizer.execute(logicalPlan)`（`:816`，用 `AQEOptimizer`）→ `planner.plan(...)`（`:817`，重新選物理算子）→ 套 `queryStagePreparationRules`（`:820`）。採不採用新計畫由 `costEvaluator` 把關（`:374`–`:378`）：`SimpleCostEvaluator.evaluateCost` 就是**數計畫裡的 `ShuffleExchangeLike` 個數**（`simpleCosting.scala:43`–`:46`，shuffle 越少成本越低；傾斜模式下再把 skew join 數編進高 32 位元優先比較，`:55`），新計畫必須 `newCost <= origCost` 才換——**「不會更差才採用」是 AQE 安全性的根本**。

#### AQE 實際能做的三件事（runtime physical rules）

`queryStageOptimizerRules`（`AdaptiveSparkPlanExec.scala:138`–`:146`）在每個新 stage 物化前套用，加上 `AQEOptimizer`（`sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AQEOptimizer.scala:31`）的 `DynamicJoinSelection` batch（`:44`），三大 runtime 能力：

| 能力 | rule（file:line） | 用什麼 runtime 統計 | 做什麼 |
|---|---|---|---|
| **合併 shuffle 分區** | `CoalesceShufflePartitions`（`CoalesceShufflePartitions.scala:34`） | 每分區實際位元組 `mapStats.bytesByPartitionId`（`:81`） | 把過多的小 shuffle 分區合併到逼近 `advisoryPartitionSize`（預設 64MB），用 `ShufflePartitionsUtil.coalescePartitions`（`:107`）。免去「分區數寫死 200」的兩難 |
| **拆解傾斜 join** | `OptimizeSkewedJoin`（`OptimizeSkewedJoin.scala:57`） | 各分區大小 + 中位數 | 某分區 > `median × FACTOR` 且 > 絕對門檻（`getSkewThreshold`，`:65`–`:67`）即判定傾斜，把該大分區切成多個子分區（`targetSize`，`:75`）平攤負載。受 `spark.sql.adaptive.skewJoin.enabled` 控制（`:258`） |
| **降級 / 轉 join 策略** | `DynamicJoinSelection`（`DynamicJoinSelection.scala:38`） | `mapStats`：空分區比例、各分區大小 | 一側 shuffle 後多數分區為空 → 加 `NO_BROADCAST_HASH` hint demote 掉廣播（`hasManyEmptyPartitions`，`:40`–`:44`）；每分區都夠小 → 加 `PREFER_SHUFFLE_HASH` hint 把 sort-merge join 換成 shuffle-hash join（`preferShuffledHashJoin`，`:47`–`:53`）；兩者皆滿足給 `SHUFFLE_HASH`（`:93`–`:98`） |

注意這些 rule 的觸發條件都明確檢查 `stage.isMaterialized && stage.mapStats.isDefined`（如 `DynamicJoinSelection.scala:60`–`:61`）——**沒有真實統計就不動手**，與 §5.2 CBO 的保守紀律一脈相承，只是這次拿的是「執行期實測值」而非「規劃期估算值」。

```
   初始物理計畫（規劃期靜態估算，可能估錯）
        SortMergeJoin
        ├─ Shuffle(stageA)   ← 跑完才發現實際只有 8MB（估算以為很大）
        └─ Shuffle(stageB)
                │  AQE: stageA 物化，mapStats 顯示 8MB
                ▼  reOptimize + DynamicJoinSelection
        BroadcastHashJoin     ← 執行期改成廣播小表，省掉一次 shuffle
        ├─ BroadcastExchange(stageA, 8MB)
        └─ stageB
```

> → 對 arneb 的啟發（**本章對 arneb 最重要的單一啟發**）：
> 1. **執行期再優化正面打中 arneb 的兩大規劃痛點**。記憶中 arneb 的靜態成本模型 (a) 在 q08 選錯 build side（builds 90M）、(b) `partition_count` 寫死。AQE 證明：這兩個問題的根治法不是「把靜態 cost model 調得更準」（Spark 連 CBO join reorder 都預設關），而是**「先用保守靜態計畫開跑，shuffle map 階段一物化就拿真實 `bytesByPartitionId` 回頭改計畫」**。arneb 的 Arrow Flight shuffle 在每個 fragment 邊界（= remote exchange）本來就有「上游 task 產出量」的真實資訊——這正是 AQE 的 `MapOutputStatistics` 等價物，目前被丟掉了。
> 2. **`CoalesceShufflePartitions` 直接對應 arneb 的「partition_count 寫死」痛點**。arneb 不必在規劃期猜分區數，可以像 Spark 一樣先給一個較大的上限、執行期用真實 per-partition 位元組數合併到逼近目標大小——這比靜態猜測穩健得多，也能順帶緩解「深層 join 中間資料量大、materialize-then-forward 跨 stage 序列化」的延遲牆（合併小分區 = 少很多跨 stage 往返）。
> 3. **`DynamicJoinSelection` demote broadcast 對應 arneb「broadcast 歷史上會給錯結果而被停用」**。arneb 因為怕 broadcast 給錯結果而完全停用它；AQE 的做法是——broadcast 不是規劃期賭，而是**執行期看到一側真的夠小（實測 `bytesByPartitionId` 都小）才轉成 broadcast，且整輪受 `SimpleCostEvaluator` 的「shuffle 數不增加才採用」把關**。arneb 重啟 broadcast 的安全路徑，正是這套「執行期實測 + cost gate + 不更差才採用」，而非靜態啟發式。
> 4. **AQE 的安全閥對 arneb 的可靠性問題尤其關鍵**。arneb 的 exchange 在飽和下曾 silent-truncate（consumer drop），AQE 提醒：每次計畫變更都過 `costEvaluator`（`newCost <= origCost` 才換，`AdaptiveSparkPlanExec.scala:376`）、stage 失敗會 `cleanUpAndThrowException`（`:347`）fail-loud——這跟 arneb 後來用 `must_drain` 把 silent-truncate 改成 fail-loud 是同一種「寧可吵也不要默默給錯」的工程紀律。
> 5. **但要清醒**：AQE 之所以能在執行期重切計畫而不怕重算成本，前提是 **Spark 的 shuffle 是 disk-materialized、可重放的**（map 端落地排序檔，計畫換了大不了 reduce 端換種讀法，map 輸出還在）。arneb 是 pipelined-only、記憶體串流 exchange、無 FTE——一旦上游 task 產出已串流走就無法重讀。所以 arneb 要引入 AQE 式再優化，最務實的切入點是**「只在 stage 邊界、上游尚未開始 consume 之前」**用剛拿到的 produce-side 統計調整下游（如合併分區、選 join 策略），而**不要**奢望 Spark 那種「整棵計畫隨時可重切」——那需要先補上 shuffle 物化/容錯這塊地基（這也正是 §7 會講的 Spark vs arneb 最根本的對立）。

---

## 6. 執行引擎模型（Tungsten / whole-stage codegen / UnsafeRow / 向量化 columnar）

> **本章最重要的一句話（避免最常見誤解）**：Spark 的**預設**執行模型是 **row-at-a-time + whole-stage codegen**——把一連串 operator 融合成**單一 generated Java 方法**，在 tight loop 中逐 `InternalRow` 處理，藉此消除 Volcano 模型的 per-row 虛擬呼叫開銷。這條路線和 Trino / DuckDB / ClickHouse / DataFusion / arneb 的「向量化批次解譯」是**不同的解法**。Spark 的 columnar / 向量化路徑（`ColumnVector` / `ColumnarBatch` / `VectorizedParquetRecordReader`）**主要用於 scan 讀取、columnar cache、Arrow/Pandas UDF 橋接**，**不是** operator 之間預設的傳遞單位。請勿把 Spark 描述成「預設向量化批次引擎」。

### 6.1 SparkPlan：兩條執行軸（row-based vs columnar）

`SparkPlan` 是所有物理 operator 的基底抽象類別（核實 `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlan.scala:65`：`abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable`）。它同時定義了兩條互斥的執行軸：

```
                       SparkPlan
        ┌──────────────────┴───────────────────┐
   row-based 軸                            columnar 軸
   execute(): RDD[InternalRow]            executeColumnar(): RDD[ColumnarBatch]
        │  (final, :197)                       │  (final, :232)
        ▼                                      ▼
   doExecute(): RDD[InternalRow]          doExecuteColumnar(): RDD[ColumnarBatch]
   (abstract, :343 — 子類必實作)          (:359 — 預設丟 internalError)
        │                                      │
   預設路徑（whole-stage codegen）        scan / cache / Arrow UDF 才走
```

- `def supportsColumnar: Boolean = false`（`SparkPlan.scala:92`）——**預設不支援 columnar**；`def supportsRowBased: Boolean = !supportsColumnar`（`:85`）。換言之，絕大多數 operator 走 row-based 軸。
- `final def execute(): RDD[InternalRow]`（`:197`）委派給抽象的 `protected def doExecute(): RDD[InternalRow]`（`:343`，子類必須 override）。
- `final def executeColumnar(): RDD[ColumnarBatch]`（`:232`）委派給 `protected def doExecuteColumnar(): RDD[ColumnarBatch]`（`:359`），而後者**預設直接拋 `SparkException.internalError(... has column support mismatch ...)`**（`:360`）——只有真正支援 columnar 的節點（如 scan）才會 override 它。
- **執行單位是 `RDD`**：`execute()` 回傳的是 `RDD[InternalRow]`，不是直接的資料流。每個 SparkPlan 子樹被編成 RDD 的 transformation，由 Spark Core 的 DAGScheduler 切成 stage、在 Executor 上以 task 跑（task = 一個 RDD partition），這是與 Trino/arneb「operator pipeline 直接搬 Page/RecordBatch」最根本的結構差異——Spark 的 operator pipeline 最終是落在 RDD lineage 上的。

> → 對 arneb 的啟發：arneb 的 `SendableRecordBatchStream` 是純 columnar、pull-based 的單一執行軸；Spark 卻同時維護 row-based（預設）與 columnar（scan/cache）兩軸，並用 `supportsColumnar` 在 planning 期決定走哪條。arneb 不需要這種二元性（它天生 Arrow 向量化），但 Spark 的設計提醒一件事：**「掃描層用 columnar、運算層用別的表示」是可以共存的工程選擇**。arneb 已是全程 Arrow，無需引入 row 軸——這裡的價值是反向確認 arneb 的單軸 columnar 已經跳過了 Spark 為了相容歷史 row 引擎而背的包袱（也符合 `common.md` §2「維持 RecordBatch 原貌、Zero-copy」的鐵律）。

### 6.2 Whole-Stage Codegen：把一串 operator 融成單一 JVM 方法

這是 Spark 對抗逐列開銷的核心手段。實際讀 `sql/core/src/main/scala/org/apache/spark/sql/execution/WholeStageCodegenExec.scala`：

**CodegenSupport trait（`:47`）— produce/consume 協定。** 支援 codegen 的 operator 實作 `trait CodegenSupport extends SparkPlan`，核心是兩個方法對：

- `final def produce(ctx: CodegenContext, parent: CodegenSupport): String`（`:94`）——觸發本 operator 產生「驅動迴圈」的程式碼骨架；它呼叫子類覆寫的 `protected def doProduce(ctx: CodegenContext): String`（`:128`，abstract）。`doProduce()` 的 javadoc 範例（`:106`–`:119`）就是一個 `while (hashmap.hasNext()) { row = hashmap.next(); ...; consume(...); if (shouldStop()) return; }` 迴圈。
- `final def consume(ctx: CodegenContext, outputVars: Seq[ExprCode], row: String = null): String`（`:160`）——把「本 operator 算出的欄位（`outputVars`）或一整列（`row`）」交給 parent；它最終呼叫 parent 的 `doConsume(...)`（trait 內 `def doConsume(...)` 預設於 `:352`）。
- `def inputRDDs(): Seq[RDD[InternalRow]]`（`:89`，註解明寫「Right now we support up to two RDDs」`:87`）——融合後的單一函式最多吃兩條輸入 RDD（對應 binary join）。

**produce/consume 的呼叫流**（class 頭部 javadoc 的 call graph，`WholeStageCodegenExec.scala:619`–`:636`）：

```
   WholeStageCodegen      Plan A            FakeInput        Plan B(不支援 codegen)
 ────────────────────────────────────────────────────────────────────────────
 -> execute()
     | doExecute() ----> inputRDDs() ----> inputRDDs() ----> execute()
     +-------------> produce()
                        | doProduce() ----> produce()
                        |                     | doProduce()
                        | doConsume() <----- consume()
  doConsume() <------ consume()
```

控制流自頂向下走 `produce`（拉資料），資料流自底向上走 `consume`（推欄位給 parent），**最後被攤平成單一沒有虛擬呼叫、沒有 iterator `next()` 邊界的 Java 方法**。

**WholeStageCodegenExec（`:643`）— 真正編譯與執行的節點。** `case class WholeStageCodegenExec(child: SparkPlan)(val codegenStageId: Int) extends UnaryExecNode with CodegenSupport`：

- `def doCodeGen(): (CodegenContext, CodeAndComment)`（`:673`）：對 child 呼叫 `child.asInstanceOf[CodegenSupport].produce(ctx, this)`（`:676`），把產出的程式碼塞進 `processNext()` 方法（`ctx.addNewFunction("processNext", ...)`，`:679`），組成一個 `final class GeneratedIteratorForCodegenStage$id extends BufferedRowIterator`（`:698`，類名見 `generatedClassName()` `:662`）。
- `override def doExecute(): RDD[InternalRow]`（`:738`）：呼叫 `doCodeGen()`（`:739`），再 `CodeGenerator.compile(cleanedSource)`（`:742`）編譯，最後把生成類包成 `WholeStageCodegenEvaluatorFactory`（`:774`），用 `rdds.head.mapPartitionsWithIndex {...}`（`:780`）或 `zipPartitions`（`:790`，雙輸入）跑在每個 partition 上。

**Fallback（容錯降級）——重要的工程細節：**

- **編譯失敗 fallback**：`catch { case NonFatal(_) if !Utils.isTesting && conf.codegenFallback => ... return child.execute() }`（`:744`–`:749`）——Janino 編譯爆掉時，記 warning 後**直接退回 child 的解譯式 `execute()`**。
- **方法過大 fallback**：若 `compiledCodeStats.maxMethodCodeSize > conf.hugeMethodLimit`（`:753`）則「too long generated codes and JIT optimization might not work ... whole-stage codegen was disabled」並 `return child.execute()`（`:761`）。`hugeMethodLimit` 預設 **65535**（`SQLConf.scala:2652`，即合法 Java 方法 bytecode 上限；註解建議在 HotSpot 上調成 8000 以利 JIT）。
- `def doExecuteColumnar(): RDD[ColumnarBatch] = child.executeColumnar()`（`:732`）：codegen **不支援 columnar 輸出**，遇到就退回解譯（`:733` 註解「Code generation is not currently supported for columnar output」）。

**何時插入 codegen — CollapseCodegenStages 規則（`:914`）：** `case class CollapseCodegenStages(...) extends Rule[SparkPlan]`，在物理計畫上跑：

- `apply(plan)`（`:988`）僅在 `conf.wholeStageEnabled && conf.codegenFactoryMode != NO_CODEGEN`（`:989`）時才 `insertWholeStageCodegen(plan)`。`wholeStageEnabled` 來自 `spark.sql.codegen.wholeStage`，**預設 true**（`SQLConf.scala:2557`）。
- `private def supportCodegen(plan: SparkPlan): Boolean`（`:925`）：只有 `CodegenSupport` 且 `plan.supportCodegen`、且**沒有 `CodegenFallback` 運算式**（`:927`，靠 `supportCodegen(e: Expression)` `:918` 判斷）、且**欄位數不過多**（`isTooManyFields`，輸出 `:929` / 輸入 `:931`）才 codegen。`isTooManyFields` 門檻來自 `conf.wholeStageMaxNumFields`（`WholeStageCodegenExec.scala:591`；conf `spark.sql.codegen.maxFields`，`SQLConf.scala:2597`）。
- `insertInputAdapter`（`:940`）：在**不支援 codegen 的子節點上方插入 `InputAdapter`**（`:511`，`extends ... with InputRDDCodegen`）當作「codegen 邊界的葉子」——它把一條普通 RDD iterator 包成 codegen 能消費的輸入（`InputRDDCodegen.doProduce` 產生 `while (input.hasNext()) { InternalRow row = (InternalRow) input.next(); ...consume... }`，`:494`–`:501`）。**`SortMergeJoinExec` / `ShuffledHashJoinExec` 的兩個 child 各自獨立 codegen**（`:945`–`:952`）——也就是 shuffle/sort 邊界天然切斷融合。

```
   物理計畫（CollapseCodegenStages 後，EXPLAIN 中以 * 標記）
   *(2) HashAggregate            ┐
   *(2) Project                  │  codegen stage 2：融成一個 Java 方法
   *(2) Filter                   │
   *(2) Scan parquet ───────────┘  (葉子；下游若 shuffle 則由 InputAdapter 斷開)
        │  Exchange (shuffle, 不可融合 → stage 邊界)
   *(1) HashAggregate(partial)   ┐  codegen stage 1
   *(1) Scan parquet ───────────┘
```

> → 對 arneb 的啟發：whole-stage codegen 是**逐列引擎為了不重寫成向量化、又要消除 per-row 虛擬呼叫**而走的路。arneb 本就是 Arrow 向量化 + 解譯式 `PhysicalExpr`（一次處理一整個 `RecordBatch` 的欄陣列），**已經在 batch 粒度攤平了 per-row 開銷，不必、也不該照抄 codegen**——引入 JVM 式的執行期 codegen 對 Rust + arrow-rs 是巨大複雜度且收益不明（arrow-rs 的 compute kernel 已是 SIMD 友善的緊湊迴圈）。真正值得抄的是 Spark 的**fallback 紀律**：codegen 失敗（編譯爆 / 方法過大）一律退回解譯且記 warning（`:746`、`:754`），而非 silent 出錯——這正對應 arneb 記憶中「broadcast 歷史上會給錯結果而被停用」「exchange 飽和 silent-truncate → 已用 must_drain 改成 fail-loud」的教訓：**任何「快路」都要有一條被驗證過、會大聲降級的慢路**。

### 6.3 Tungsten / UnsafeRow：off-heap 緊湊二進位列格式

`InternalRow` 在 codegen tight loop 裡傳遞的具體載體是 `UnsafeRow`——Tungsten 記憶體管理的核心資料結構。實際讀 `sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/UnsafeRow.java`：

- `public final class UnsafeRow extends InternalRow implements Externalizable, KryoSerializable`（`:63`）。class 頭部 javadoc（`:48`–`:62`）明寫三段式佈局：**`[null-tracking bit set] [values] [variable length portion]`**。

```
      ┌──────────── 一筆 UnsafeRow（raw bytes，非 Java 物件）─────────────┐
      │  null bitset      │  values region (每欄 8 bytes)  │ 變長區       │
      │  8-byte 對齊       │  fixed: 直接存值               │ 字串/binary │
      │  每欄 1 bit        │  varlen: 存 (offset<<32 | len) │ 的實際 bytes │
      └───────────────────┴────────────────────────────────┴─────────────┘
       calculateBitSetWidthInBytes   numFields * 8 bytes
```

- **null bitset 8-byte 對齊**：`public static int calculateBitSetWidthInBytes(int numFields) { return ((numFields + 63)/ 64) * 8; }`（`:71`）；`WORD_SIZE = 8`（`:65`）。
- **values region 每欄一個 8-byte word**：`private long getFieldOffset(int ordinal) { return baseOffset + bitSetWidthInBytes + ordinal * 8L; }`（`:120`）。fixed-length 原始型別（int/long/double…）**直接把值存進那個 word**（如 `setLong` → `Platform.putLong(baseObject, getFieldOffset(ordinal), value)`，`:226`）；變長/非原始型別則存「相對 offset + length 合成的一個 long」（`getFieldOffset(ordinal)` 存 `(cursor << 32) | length`，見 binary 寫入 `:317`），實際 bytes 放在變長區尾端。
- **底層是裸記憶體 + `sun.misc.Unsafe`**：欄位 `private Object baseObject; private long baseOffset;`（`:108`–`:109`），所有讀寫透過 `org.apache.spark.unsafe.Platform`（`import static ... Platform.BYTE_ARRAY_OFFSET`，`:46`；`Platform.getLong/putLong` 等）。`UnsafeRow` 自身只是「指向這塊記憶體的指標」（`:61` javadoc「Instances of UnsafeRow act as pointers」），`pointTo(Object baseObject, long baseOffset, int sizeInBytes)`（`:161`）即重新指向。
- **意義**：cache 友善（連續記憶體、no pointer chasing）、避 GC（資料在 byte[] 或 off-heap，不是一堆小 Java 物件）、序列化幾乎零成本（直接拷 bytes）。`isFixedLength`（`:78`）/ `isMutable`（`:93`）決定哪些欄可原地更新（aggregation buffer 用得到）。

**BytesToBytesMap — hash aggregation / join 的緊湊雜湊結構。** 讀 `core/src/main/java/org/apache/spark/unsafe/map/BytesToBytesMap.java`：

- `public final class BytesToBytesMap extends MemoryConsumer`（`:69`）。javadoc（`:50`–`:67`）：「append-only hash map where keys and values are contiguous regions of bytes」、power-of-2 大小、**quadratic probing with triangular numbers**、最多 2^29 keys、key 與 value **連續存在一起**（格式：record 長度 + key 長度 + key bytes + value bytes + 8-byte next 指標）。它本身是 `MemoryConsumer`（受 Spark 記憶體管理 + spill 控管，見 §8），且格式刻意與 `UnsafeExternalSorter` 相容（`:65`），可直接把 record 餵進排序器原地 spill。
- HashAggregate 透過 `UnsafeFixedWidthAggregationMap`（`sql/core/src/main/java/org/apache/spark/sql/execution/UnsafeFixedWidthAggregationMap.java:39`）包裝 `BytesToBytesMap`（`:59` `private final BytesToBytesMap map;`，建構於 `:95`）作為分組聚合的 hash table；`HashAggregateExec.createHashMap()`（`sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/HashAggregateExec.scala:160`）即 `new UnsafeFixedWidthAggregationMap(...)`（`:166`）。

> → 對 arneb 的啟發：這是本章對 arneb **最直接可借鏡**的一段。Spark 的 `UnsafeRow`（緊湊二進位列、null bitset + 8-byte 值區 + 變長區）和 `BytesToBytesMap`（key+value 連續、head + next 指標串接）**正對應 arneb 記憶中已完成的 `JoinHashMap` flatten**：把 `JoinHashMap` 從 hashbrown 的 `HashMap<K, Vec<...>>`（每 key ~47 B、未進記憶體池追蹤的 anon 配置，45M-key orders build ≈ 2GB）改成 head + next 鏈結的扁平陣列（~12 B/row）。Spark 早就驗證了「**hash 結構要 contiguous bytes + 指標串接、不要一堆小物件**」這條原則——arneb 的 flatten 走在同一條被工業驗證的路上。更進一步：Spark 把 `BytesToBytesMap` 設計成 `MemoryConsumer`（受統一記憶體管理 + 可 spill），這正對應 arneb 反覆踩到的痛點「**OOM 根因常是未追蹤的 Arrow / hashbrown anon 配置撞 cgroup**」——arneb 的 `JoinHashMap` 也應像 Spark 一樣**進 MemoryPool 計帳**，而不是讓它變成 untracked anon（呼應 `common.md` §5「記帳必須下沉到分配點」）。

### 6.4 向量化 columnar 路徑：scan / cache / Arrow 橋接（非預設運算單位）

Spark 確實有一條向量化 columnar 路徑，但要清楚它的**用途邊界**。

**ColumnVector / ColumnarBatch — 抽象。** 讀 `sql/catalyst/src/main/java/org/apache/spark/sql/vectorized/`：

- `public abstract class ColumnVector implements AutoCloseable`（`ColumnVector.java:58`）：單一欄的批次資料，提供 `getInt(int rowId)`（`:160`）、`getBoolean`（`:106`）等型別化存取與 `getChild(int)`（`:366`，樹狀支援 nested type）。javadoc 明寫「ColumnVector is expected to be reused during the entire data loading process, to avoid allocating memory again and again」（`:48`–`:50`）、「meant to maximize CPU efficiency」（`:52`）——**設計定位就是 data loading（讀取）期重用、CPU 友善**。
- `public class ColumnarBatch implements AutoCloseable`（`ColumnarBatch.java:30`）：`protected final ColumnVector[] columns;`（`:32`）+ `protected int numRows;`（`:31`），就是「一組欄向量 + 列數」——結構上與 Arrow `RecordBatch`、Trino `Page` 同構。關鍵：它提供 `rowIterator(): Iterator<InternalRow>`（`:61`，用 `ColumnarBatchRow` 把一列包成 `InternalRow`）——**這就是 columnar→row 的橋**，讓 row-based codegen 引擎能消費 columnar scan 的輸出。

**WritableColumnVector 與兩種記憶體模式。** 讀 `sql/core/src/main/java/org/apache/spark/sql/execution/vectorized/`：

- `public abstract class WritableColumnVector extends ColumnVector`（`WritableColumnVector.java:55`）：可寫入、可 `reserve(int requiredCapacity)`（`:127`）擴容、內建 dictionary 支援（`dictionary` 欄、`dictionaryIds`，`:110`–`:114`）。
- `public final class OnHeapColumnVector extends WritableColumnVector`（`OnHeapColumnVector.java:32`）：javadoc「A column backed by an in memory JVM array. ... stores the NULLs as a byte per value and a java array for the values」（`:28`–`:31`）；欄位是 `byte[] nulls`（`:63`）、`byte[] byteData`（`:66`）、`int[] intData`（`:68`）、`long[] longData`（`:69`）——純 JVM 陣列。
- `public final class OffHeapColumnVector extends WritableColumnVector`（`OffHeapColumnVector.java:32`）：欄位是 `long nulls; long data;`（`:70`–`:71`，**裸記憶體位址**）；`reserveInternal(int newCapacity)`（`:669`）以 `Platform.reallocateMemory(...)`（`:675` 起，依型別決定每元素 byte 數）配置 off-heap 記憶體——繞過 JVM heap、避 GC。

**VectorizedParquetRecordReader — columnar 的主要產地。** 讀 `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/VectorizedParquetRecordReader.java`：

- `public class VectorizedParquetRecordReader extends SpecificParquetRecordReaderBase<Object>`（`:67`），持有 `ColumnarBatch columnarBatch`（`:135`）與 `MemoryMode MEMORY_MODE`（`:150`，`useOffHeap ? OFF_HEAP : ON_HEAP`，`:165`）。
- `boolean nextBatch()`（`:393`）：每次填一批最多 `capacity` 列（`int num = Math.min(capacity, ...)`，`:401`），對每個欄向量 `columnReader.readBatch(num, ...)`（`:406`）批次解碼 Parquet column，最後 `columnarBatch.setNumRows(num)`（`:418`）。批次大小 `capacity` 來自 `spark.sql.parquet.columnarReaderBatchSize`，**預設 4096**（`SQLConf.scala:1705`–`:1710`）。
- `enableReturningBatches()`（`:386`）：可選擇「直接回傳整個 `ColumnarBatch`」而非逐列——但這條只在下游能吃 columnar 時才開。是否走向量化 reader 由 `spark.sql.parquet.enableVectorizedReader`（`SQLConf.scala:1660`）控制。

**ArrowColumnVector — 與 Apache Arrow 的橋接。** `public class ArrowColumnVector extends ColumnVector`（`sql/catalyst/src/main/java/org/apache/spark/sql/vectorized/ArrowColumnVector.java:39`），`import org.apache.arrow.vector.*`（`:20`），內部以 `ArrowVectorAccessor accessor`（`:41`）包一個 Arrow `ValueVector`（`getValueVector()` `:44`）。這條路用於 **Pandas/Arrow UDF** 與 Arrow-based 資料交換——Spark 把 Arrow buffer 直接當 `ColumnVector` 讀，零反序列化。

> → 對 arneb 的啟發：兩點具體價值。其一，`ColumnarBatch`（`ColumnVector[]` + numRows）↔ Arrow `RecordBatch` ↔ Trino `Page` **三者同構**，而 Spark 用 `ArrowColumnVector`（`ColumnVector.java:58` 的 Arrow-backed 子類）證明了「Arrow buffer 可零拷貝當作引擎的 columnar 內表示」——arneb 既然節點間 shuffle 已走 Arrow Flight（近零拷貝 `RecordBatch`），這條互通性是 arneb 的天然強項，無需自造格式。其二、也是反向教訓：Spark 的向量化只蓋住 scan / cache / Arrow UDF，**運算層仍是 row-based codegen**，所以它在深層 join/aggregate 鏈上享受不到「全程向量化」的好處；arneb 全程 Arrow 反而沒有這個 row↔columnar 來回轉換（`ColumnarBatch.rowIterator()` `:61`、`ColumnarToRow`）的成本。arneb 真正的延遲牆不在「要不要向量化」（已向量化），而在記憶中反覆出現的 **materialize-then-forward 跨 stage 序列化 + tokio task 持 semaphore permit 跑完整個 operator 生命週期與 stream back-pressure 不相容** 的結構問題——那是 §7、§10（分散式 / 並行模型）的議題，不是本章的執行表示問題。本章的結論很乾脆：**arneb 的 columnar 表示已對齊業界最佳實踐，不需從 Spark 抄任何執行表示；唯一要抄的是 `BytesToBytesMap` 式的緊湊雜湊（已做 flatten）與「快路必有大聲降級的慢路」紀律。**

---

## 7. 分散式執行（DAGScheduler / Stage / Shuffle / Exchange / 容錯）

Spark 的分散式執行有「兩層」：底層是 RDD + `DAGScheduler`（core 模組，把 job 依 `ShuffleDependency` 切成 stage、提交 task、處理容錯）；上層是 SQL 物理計畫（`SparkPlan`）透過 `ShuffleExchangeExec` / `EnsureRequirements` 把 shuffle 需求翻譯成 `ShuffleDependency` 餵給底層。本章由下而上講清楚這兩層，並標出與 Trino/arneb「記憶體串流 exchange」最根本的對立點：**Spark 的 shuffle 是落地（disk-materialized）且容錯的**。

> ⚠️ 寫作前先校正一個常見誤解（與 §6 一致）：Spark 的預設 operator 間傳遞單位**不是**向量化批次（不像 Trino 的 Page、DuckDB/arneb 的 Arrow RecordBatch）。Spark 預設是 **row-at-a-time + whole-stage codegen**——把一串 operator 融成單一 JVM 函式，用一個 tight `while` loop 逐列處理（見 `WholeStageCodegenExec.scala:113`–`119` 的 codegen 樣板註解：`while (hashmap.hasNext()) { row = hashmap.next(); ... }`）。向量化路徑（`ColumnVector`/`ColumnarBatch`、`VectorizedParquetRecordReader`）主要用於 scan 讀取、columnar cache 與 Arrow/Pandas UDF，不是 operator 間的預設傳遞單位。本章談的 shuffle / exchange 搬運的也是 row（`InternalRow`/`UnsafeRow`）。

### 7.1 RDD lineage + DAGScheduler：在 ShuffleDependency 邊界切 Stage

`DAGScheduler`（`core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala:124`）是整個分散式執行的「大腦」。它的類別頭註解把核心抽象講得很清楚（`DAGScheduler.scala:88`–`98`）：

- **Stage 在 shuffle 邊界切分**，shuffle 邊界引入一個 barrier（下游 stage 必須等上游 stage 全部完成、輸出落地後才能 fetch）。
- 兩種 stage：`ResultStage`（執行 action 的最終 stage）與 `ShuffleMapStage`（寫出 shuffle map output 檔的中間 stage）。
- Stage 常被多個 job 共用（RDD reuse 時）。

切分的具體機制是**沿 RDD lineage 走訪，遇到 `ShuffleDependency` 就斷開**：

- `getShuffleDependenciesAndResourceProfiles(rdd)`（`DAGScheduler.scala:801`）走訪 RDD 的 dependency；`case shuffleDep: ShuffleDependency[_, _, _] => parents += shuffleDep`（`:808`–`809`）把 shuffle 依賴收為「父 stage 邊界」，而**非** shuffle 的窄依賴（narrow dependency）則 `enqueue(dependency.rdd)`（`:811`）繼續往同一 stage 內走——這正是「shuffle 切 stage、其餘融進同一 stage」的判斷點。
- `createShuffleMapStage(...)`（`:574`）為一個 shuffle 依賴建 `ShuffleMapStage`：`numTasks = rdd.partitions.length`（`:582`，map 端 task 數 = 上游 RDD 分區數），`parents = getOrCreateParentStages(...)`（`:583`）遞迴建父 stage，最後 `mapOutputTracker.registerShuffle(...)`（`:599`）向 `MapOutputTracker` 註冊這個 shuffle 的 map 數與 reduce 分區數。
- `createResultStage(...)`（`:704`）為 job 的最終 RDD 建 `ResultStage`。
- 入口 `submitStage(stage)`（`:1540`）：先 `getMissingParentStages(stage)`（`:1553`、定義於 `:837`），若有未完成父 stage 就先遞迴 `submitStage(parent)`（`:1561`）並把自己放進 `waitingStages`（`:1563`）；父 stage 全 ready 才 `submitMissingTasks(stage, jobId)`（`:1558`）。

`submitMissingTasks`（`:1635`）把 stage 物化成一批 task 丟給 `TaskScheduler`：

- `partitionsToCompute = stage.findMissingPartitions()`（`:1665`）——**只算缺的分區**（容錯重算的關鍵，見 §7.4）。
- 把 `(rdd, shuffleDep)` 或 `(rdd, func)` 序列化後 **broadcast** 給 executor（`:1748`、`:1750`、`taskBinary = sc.broadcast(taskBinaryBytes)`，`:1760`）。
- 依 stage 型別建 `ShuffleMapTask`（`:1788`）或 `ResultTask`（`:1799`），最後 `taskScheduler.submitTasks(new TaskSet(...))`（`:1821`）。

**關鍵單執行緒不變式**：所有 stage/task 完成事件都在單一 event loop `DAGSchedulerEventProcessLoop`（`DAGScheduler.scala:3506`，繼承 `EventLoop`）裡循序處理（`doOnReceive`，`:3526`）。所以 DAGScheduler 處理 task 完成、accumulator 合併等不需要鎖（原始碼自註：`we only handle one task completion event at a time so we don't need to worry about locking`，`:1853`）。

```
   RDD lineage（DAG）            DAGScheduler 沿 lineage 走訪
   ┌────────────────────┐       遇 ShuffleDependency → 斷成 stage
   │ HadoopRDD (scan)   │
   │   │ narrow dep     │  ─┐
   │   ▼                │   │  同一 stage（窄依賴融進來）
   │ MapPartitionsRDD   │  ─┘
   │   │ ShuffleDep ════╪═════════  ← barrier：map output 落地
   │   ▼                │
   │ ShuffledRowRDD     │  ─┐
   │   ▼                │   │  下游 stage
   │ ResultRDD (action) │  ─┘
   └────────────────────┘

   ShuffleMapStage 0 ──(map output 寫本地檔)──▶ MapOutputTracker
                                                      │
   ResultStage 1 ◀──(reduce 端依 MapStatus fetch)─────┘
```

> → 對 arneb 的啟發：arneb 的 `PlanFragmenter` 在 **REMOTE exchange 邊界**切 fragment（每個 fragment = 一個 stage），與 Spark「在 `ShuffleDependency` 切 stage」是同構的——這部分 arneb 已對齊。但 Spark 多出一層**RDD lineage 作為血統紀錄**：每個 stage 知道自己的輸入 RDD 與「只需重算哪些分區」（`findMissingPartitions`）。arneb 目前是 pipelined-only、fragment 失敗即整查詢失敗，沒有等價的「血統 + 只重算缺分區」資料結構。若日後要做容錯執行（FTE），這個「stage = (RDD, 已完成分區集合)」的紀錄是必要前置。

### 7.2 Shuffle map 端：三種 writer，寫「本地排序資料檔 + index 檔」

map task 跑完後要把輸出按 reduce 分區寫到**本地磁碟**。`SortShuffleManager.registerShuffle(...)`（`core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala:90`）依依賴性質在三種 writer 中擇一（產出三種 `ShuffleHandle`），`getWriter(...)`（`:145`）再依 handle 實例化對應 writer：

| Writer | 選用條件（原始碼判斷） | 行為 |
|---|---|---|
| `BypassMergeSortShuffleWriter` (Java) | `shouldBypassMergeSort`：**無** map-side combine 且 `numPartitions <= spark.shuffle.sort.bypassMergeThreshold`（`SortShuffleWriter.scala:118`–`125`） | 每個 reduce 分區開一個檔直接寫、最後串接成單一檔；不在記憶體緩衝、不排序（`BypassMergeSortShuffleWriter.java:63`–`66`） |
| `UnsafeShuffleWriter` (Java) | `canUseSerializedShuffle`：serializer 支援物件重定位、**無** map-side combine、分區數 ≤ 16M（`SortShuffleManager.scala:227`–`245`） | Tungsten serialized 路徑，操作序列化後的 bytes（`UnsafeShuffleWriter.java:70`） |
| `SortShuffleWriter` (Scala) | 其餘情況（`BaseShuffleHandle`，`SortShuffleManager.scala:173`） | 走 `ExternalSorter`，**可** map-side combine / 排序，會 spill 到磁碟再合併 |

以最通用的 `SortShuffleWriter.write(...)`（`SortShuffleWriter.scala:65`）為例，落地過程是：

1. 建 `ExternalSorter`（依 `dep.mapSideCombine` 決定要不要帶 aggregator/ordering，`:66`–`77`），`sorter.insertAll(records)`（`:78`）——超過記憶體門檻會 spill。
2. `createMapOutputWriter(...)`（`:83`）開一個輸出寫；`sorter.writePartitionedMapOutput(...)`（`:85`）把各分區資料寫進**單一資料檔**。
3. `commitAllPartitions(...)`（`:86`）回傳每個 reduce 分區的 byte 長度 `partitionLengths`。
4. 產出 `MapStatus(blockManager.shuffleServerId, partitionLengths, mapId, ...)`（`:88`）回報給 driver。

資料檔 + index 檔的「物理檔案」由 `IndexShuffleBlockResolver` 管理，其類別註解寫得很白（`IndexShuffleBlockResolver.scala:46`–`52`）：「同一 map task 的所有 shuffle block 存在**單一合併資料檔**（`.data`），各 block 在資料檔中的 offset 存在**獨立的 index 檔**（`.index`）」。`writeMetadataFileAndCommit(...)`（`:391`）以 `this.synchronized` 把資料檔與 index 檔的 rename **原子化提交**（`:412`–`414`），index 檔內容是「每個 block 的 offset，加上檔尾的最終 offset」（`:382`–`383`），供 reduce 端 `getBlockData` 定位每個 block 的起訖。

`MapStatus`（`core/src/main/scala/org/apache/spark/scheduler/MapStatus.scala:42`）是 map 端輸出的**位置 + 大小**摘要：`location: BlockManagerId`（`:44`/`:149`），`getSizeForBlock(reduceId)`（`:54`）。分區數大時用 `HighlyCompressedMapStatus`（`:194`）壓縮儲存（門檻見 `MapStatus.scala:70`–`88`）。這些 `MapStatus` 集中由 driver 端的 `MapOutputTrackerMaster`（`MapOutputTracker.scala:708`）保管——`registerMapOutput(...)`（`:867`）登記、`getNumAvailableOutputs(...)`（`:964`）回報已就緒分區數（`ShuffleMapStage.isAvailable` 即靠它，`ShuffleMapStage.scala:84`、`:89`）。

```
   map task i:
     records ──► ExternalSorter（記憶體 + spill）──► 排序、按 reduce 分區分桶
       │
       ├─ 寫 shuffle_<id>_<mapId>_0.data  ← 單一資料檔（所有 reduce 分區串接）
       └─ 寫 shuffle_<id>_<mapId>_0.index ← 各分區 offset（原子提交）
                                  │
                       MapStatus{location, partitionLengths} ──► MapOutputTrackerMaster
```

> → 對 arneb 的啟發：這是 Spark 與 arneb（及 Trino）**最根本的對立點**，務必看清：arneb 的 shuffle 走 Arrow Flight RPC、RecordBatch 在記憶體中近零拷貝串流、上游 task 直接把 batch 推給下游 task。Spark 反過來——map task **先把輸出排序、落地成本地 `.data`/`.index` 檔**，task 結束後資料還在磁碟上，reduce 端事後才來拉。代價是延遲（多一趟落地 + 重讀）與磁碟 IO；換來的是「中間結果可重讀、可重複 fetch」，這正是 §7.4 容錯的物理基礎。arneb 的 OutputBuffer 是記憶體 buffer，消費者一斷（consumer drop）資料就沒了（記憶中曾 silent-truncate，現已用 `must_drain` 改成 fail-loud）；Spark 的對應物是落地檔，消費者重來只要重 fetch 即可。**若 arneb 要演進出容錯執行，落地式 shuffle（哪怕只在指定的 stage 邊界、可選開啟）是繞不開的代價交換**——延遲換可重算。

### 7.3 Shuffle reduce 端：跨網路 fetch，受 maxBytesInFlight 節流

reduce task 透過 `BlockStoreShuffleReader.read()`（`core/src/main/scala/org/apache/spark/shuffle/BlockStoreShuffleReader.scala:72`）讀上游所有 map 寫出的、屬於自己這個 reduce 分區的 block。核心是建 `ShuffleBlockFetcherIterator`（`:73`）跨網路 / 本地拉 block；若 join/agg 需要排序，再套 `ExternalSorter`（`:116`–`118`，當 `dep.keyOrdering.isDefined`）。

`ShuffleBlockFetcherIterator`（`core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala:86`）的設計重點是**流量節流**（類別註解 `:54`、`:69`–`70`）：

- `maxBytesInFlight`（`:93`）：任一時刻飛行中的遠端 block 總量上限；`maxReqsInFlight`（`:94`）：併發請求數上限。
- `targetRemoteRequestSize = max(maxBytesInFlight / 5, 1)`（`:112`）——刻意把單一請求做小，「以允許同時向最多 5 個節點平行 fetch」（`:109`–`112` 註解）。
- `partitionBlocksByFetchMode(...)`（`:392`）把 block 分成本地（`fetchLocalBlocks`，`:740`/`:580`）與遠端（切成多個 `FetchRequest`），`sendRequest`（`:264`）發送並維護 `bytesInFlight` / `reqsInFlight`（在 `next()` 拿到結果時遞減，`:835`–`843`）。
- block 拉回時會偵測資料損毀（解壓檢查、checksum），損毀則重 fetch 一次，二次仍壞才拋 `FetchFailedException`（`next()`，`:824`–`875`、`throwFetchFailedException`，`:1250`）。

```
   reduce task r:
     向 MapOutputTracker 問：shuffle s 的 reduce 分區 r 在哪些 MapStatus？
              │
     ShuffleBlockFetcherIterator（節流：bytesInFlight ≤ maxBytesInFlight，
                                  併發 ≤ maxReqsInFlight，單請求 ~ /5 以平行拉 5 點）
       ├─ 本地 block：直接讀本地 .data 檔
       └─ 遠端 block：HTTP/Netty fetch ← 上游 executor 的 shuffle 檔
              │
       損毀 → 重 fetch；二次仍壞 → throw FetchFailedException（→ §7.4）
```

> → 對 arneb 的啟發：
> 1. **節流是「拉取端的主動流控」**：Spark 的 reduce 端用 `bytesInFlight ≤ maxBytesInFlight` 限制飛行中資料、`targetRemoteRequestSize = maxBytesInFlight/5` 兼顧平行度與單點壓力。arneb 反覆踩到的痛點正是流控失靈——tokio task 持有 semaphore permit 跑完整個 operator、與 stream back-pressure 不相容，導致 exchange stall / OutputBuffer 死等。Spark 這種「拉取端按位元組量主動限流、且請求顆粒度刻意調小以平行拉多點」的模型，是治 arneb 飽和下 stall 的可借鏡結構（拉取者控節奏，而非持有 permit 等推送；呼應 `common.md` §3.1 非阻塞背壓鐵律）。
> 2. **fetch 失敗是可恢復事件而非致命錯**：因為來源是落地檔，二次重拉、甚至向別處重算都可行（見 §7.4）；arneb 的記憶體 buffer 一旦來源消失就只能整查詢失敗。

### 7.4 容錯：FetchFailed → 依 lineage 重算 missing stage

這是 Spark 分散式執行最值得 arneb 借鏡的一段。當 reduce 端拉不到某個 map output（上游 executor 掛了、shuffle 檔遺失），`ShuffleBlockFetcherIterator` 拋 `FetchFailedException`（`core/src/main/scala/org/apache/spark/shuffle/FetchFailedException.scala:35`）。它在建構時就 `TaskContext.get().setFetchFailed(this)`（`:59`），確保即使 user code 攔截包裝了這個例外，executor 仍能辨識是 fetch failure（`:30`–`33` 註解，SPARK-19276）。它最終轉成 `FetchFailed` 這個 `TaskFailedReason`（`toTaskFailedReason`，`:61`）回報給 driver。

driver 的 `handleTaskCompletion` 收到 `FetchFailed(bmAddress, shuffleId, _, mapIndex, reduceId, ...)`（`DAGScheduler.scala:2395`）後：

1. 找出失敗的下游 stage `failedStage` 與產生該 shuffle 的上游 `mapStage`（`:2396`–`2397`）。
2. 若不是過期的 stage attempt（`:2399`），把 `failedStage` 標記為 failed（`markStageAsFinished(..., willRetry = !shouldAbortStage)`，`:2431`）。
3. **把遺失的 map output 從 tracker 註銷**：非 barrier 且 `mapIndex != -1` 時 `mapOutputTracker.unregisterMapOutput(shuffleId, mapIndex, bmAddress)`（`:2446`）——這一步讓 `mapStage` 重新變成「該分區 missing」。
4. 把 `failedStage` 與 `mapStage` 一起放進 `failedStages`（`:2486`–`2487`），透過 `scheduleResubmit()`（`:2517`）延遲一小段時間再 `resubmitFailedStages()`（`:1244`）——延遲是為了讓「一個 executor 掛掉引發的大量 FetchFailed」被單次重提交吸收（`:2505`–`2512` 註解）。
5. 若整台 host 的 shuffle 都可能遺失（external shuffle service 開啟 / host 被 decommission），則 `removeExecutorAndUnregisterOutputs(...)`（`:2542`）連帶註銷整批。

`resubmitFailedStages()`（`:1244`）對每個 failed stage 重跑 `submitStage(stage)`（`:1253`）。回到 §7.1：`submitStage` → `getMissingParentStages` 發現 `mapStage` 有分區 missing（因為剛被 `unregisterMapOutput`）→ 先重提交 `mapStage` 只重算**那些缺的 map 分區**（`submitMissingTasks` 的 `stage.findMissingPartitions()`，`:1665`；`ShuffleMapStage.findMissingPartitions` 走 `MapOutputTracker`，`ShuffleMapStage.scala:92`–`96`），map 重算完後 `submitWaitingChildStages`（`:1263`）再放下游。

stage 不會無限重試：`submitStage` 內 `getNextAttemptId >= maxStageAttempts` 就 `abortStage`（`DAGScheduler.scala:1546`–`1551`，門檻取自 `spark.stage.maxAttempts` / `spark.stage.maxConsecutiveAttempts`）。

```
   reduce task fetch 不到 map output（上游 executor 掛 / 檔遺失）
        │  throw FetchFailedException → TaskContext.setFetchFailed
        ▼
   driver: handleTaskCompletion case FetchFailed (DAGScheduler.scala:2395)
        ├─ markStageAsFinished(failedStage, willRetry=true)   (:2431)
        ├─ mapOutputTracker.unregisterMapOutput(...)          (:2446) ← 缺口被記下
        └─ failedStages += {failedStage, mapStage}; scheduleResubmit() (:2517)
        ▼
   resubmitFailedStages (:1244) → submitStage → getMissingParentStages
        ▼
   只重算 mapStage 缺的那幾個 map 分區（findMissingPartitions），
   完成後再放下游 stage —— 已完成的分區不重算
```

> → 對 arneb 的啟發：這是 arneb **pipelined-only（無 FTE）的演進路徑藍圖**。arneb 目前任一 task 失敗即整查詢失敗、深層 join 的大量中間資料 materialize-then-forward 跨 stage 序列化是延遲牆。Spark 給出的可借鏡結構是三件套：(1) **落地式 shuffle**（§7.2）讓中間結果可重讀；(2) **`MapOutputTracker` 式的「哪些分區已就緒」帳本**，使重算能精準到分區（`findMissingPartitions`），而非整 stage 重跑；(3) **失敗事件統一回到單執行緒 driver 大腦做決策**（解註銷 → 重提交 → 只算 missing），並有重試上限避免無限重算。代價就是延遲——落地 + barrier 等待 + 重讀，這恰是 arneb 現在用串流換來的低延遲所放棄的。實務上的折衷是「可選的容錯 shuffle」：只在指定昂貴 stage 邊界落地、其餘維持 Flight 串流。

### 7.5 SQL 層：ShuffleExchangeExec 引入 shuffle、EnsureRequirements 推導分佈

上層 SQL 物理計畫怎麼變出 `ShuffleDependency`？答案是 `ShuffleExchangeExec`（`sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala:190`）。它持有 `outputPartitioning: Partitioning`（`:191`），其 `shuffleDependency` lazy val（`:245`）呼叫 `prepareShuffleDependency(...)`（`:341`），最終 `new ShuffleDependency[Int, InternalRow, InternalRow](...)`（`:537`）——**SQL 的一個 `ShuffleExchangeExec` 物理節點 == 底層的一個 `ShuffleDependency` == DAGScheduler 的一道 stage 邊界**。`doExecute()`（`:264`）回傳 `ShuffledRowRDD(shuffleDependency, ...)`（`:267`），把上層接回下層 RDD 世界。

**誰決定要不要插 exchange / sort？** 是 `EnsureRequirements`（`sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala:51`）這條物理規則。它對每個 operator 問兩個需求：

- `requiredChildDistribution`：子節點資料該怎麼分佈（例如 join 兩邊要 co-partition）。
- `requiredChildOrdering`：子節點資料該怎麼排序（例如 sort-merge join 要排序）。

核心邏輯在 `ensureDistributionAndOrdering(...)`（`:56`）：對每個 child，**若其 `outputPartitioning` 已 `satisfies(distribution)` 就不動**（`:71`「If non-KeyedPartitioning already satisfies, no changes needed」），否則才補：

- 需要 broadcast → 插 `BroadcastExchangeExec(mode, child)`（`:127`）。
- 需要重分區 → 插 `ShuffleExchangeExec(distribution.createPartitioning(numPartitions), child, ...)`（`:129`、`:133`）。
- 多個有 `ClusteredDistribution` 需求的 child（如 join 兩邊）要彼此 **co-partition**：`createShuffleSpec(...)`（`:162`）比對兩邊 shuffle spec，挑「平行度較好（分區數較大）」者，盡量只 shuffle 必要的一邊（`:166`–`:194` 註解，例 `HashPartitioning(5) <-> HashPartitioning(6)` 只重洗左邊到 6）。
- 排序需求不滿足才補 `SortExec(requiredOrdering, global = false, child)`（`:296`）。

join 物理算子各自宣告需求（決定 §7.5 補什麼）：

| Join 算子 | requiredChildDistribution | 含義 |
|---|---|---|
| `BroadcastHashJoinExec` | build 側 `BroadcastDistribution(mode)`、stream 側 `UnspecifiedDistribution`（`BroadcastHashJoinExec.scala:67`–`:74`） | 小表廣播、大表不動 |
| `ShuffledHashJoinExec` / `SortMergeJoinExec`（皆 `ShuffledJoin`） | 兩邊 `ClusteredDistribution(leftKeys)` / `ClusteredDistribution(rightKeys)`（`ShuffledJoin.scala:57`–`:67`） | 兩邊按 join key hash 重分區，同 key 落同分區 |
| `SortMergeJoinExec`（額外） | `requiredChildOrdering = requiredOrders(leftKeys) :: requiredOrders(rightKeys)`（`SortMergeJoinExec.scala:94`） | 兩邊還要按 key 排序 |

`BroadcastExchangeExec`（`BroadcastExchangeExec.scala:124`）的 build 在 `relationFuture`（`:177`）裡：`child.executeCollectIterator()`（`:186`）把整個小表 collect 到 driver，超過 `maxBroadcastRows`（`:164`、檢查於 `:188`）或 `maxBroadcastTableSizeInBytes`（`:210`–`:211`）就拋錯拒絕廣播，否則 `sparkContext.broadcastInternal(relation, serializedOnly = true)`（`:220`）廣播出去（分發機制見 §10.3）。

```
   物理計畫（含 SortMergeJoinExec，兩邊需 ClusteredDistribution + ordering）
        │  EnsureRequirements.ensureDistributionAndOrdering
        ▼
   child.outputPartitioning.satisfies(ClusteredDistribution)?
        ├─ 是 → 不插（重用既有分佈，省一道 shuffle）
        └─ 否 → 插 ShuffleExchangeExec(HashPartitioning(keys, n))
                   再依 requiredChildOrdering 補 SortExec
        ▼
   ShuffleExchangeExec.shuffleDependency → new ShuffleDependency → DAGScheduler 切 stage（回到 §7.1）
```

> → 對 arneb 的啟發：`EnsureRequirements` 正是 arneb 規劃層**最缺的那塊「分佈推導（partitioning-property derivation）」**——對應記憶中 Q05/Q09 因缺 broadcast 與 partition-property 推導而 OOM / 慢。Spark 的模型是：每個算子用 `requiredChildDistribution` / `requiredChildOrdering` 宣告需求，規則層比對「子節點現有 `outputPartitioning` 是否 `satisfies`」，**已滿足就不插 exchange、不滿足才補最小代價的一道**，且兩邊不一致時只重洗較小一邊。arneb 有 Selinger DP + partition-aware cost + NDV，但缺的正是這套「需求-滿足-差補」的 property 推導框架（DataFusion 的 `EnforceDistribution`、Trino 的 PropertyDerivations 同源）。此外 `BroadcastHashJoinExec` 的 build 側 `BroadcastDistribution` + `BroadcastExchangeExec` 的 row/byte 上限**主動拒絕廣播過大表**——對應 arneb 歷史上因 broadcast 會給錯結果而整個停用 broadcast join；Spark 證明 broadcast 是可控的（有明確上限 + AQE 執行期再決定，見 §7.6），不必因噎廢食。

### 7.6 AQE（從分散式視角再看一次）：拿執行期統計回頭改物理計畫

前述 §7.5 的 `EnsureRequirements` 是**規劃期靜態**決策；Spark 還有一個其他四個對照引擎（Trino/DuckDB/ClickHouse/DataFusion）都沒有等價物的機制：**AQE（Adaptive Query Execution）= 執行期再優化**。實作在 `AdaptiveSparkPlanExec`（`sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala:70`）；機制細節已在 §5.3 完整拆解，此處從分散式 shuffle 統計的角度補一個視角。

機制核心（`getFinalPhysicalPlan` 的主迴圈，`:284`–`:394`）：

1. 把物理計畫**沿 exchange 邊界切成 `QueryStage`**：`createQueryStages(...)`（`:284`、`:551`），逐一 `stage.materialize()`（`:309`）執行。
2. 一個 stage 跑完，拿到**真實的執行統計** `MapOutputStatistics`（即 §7.2 收集的 `MapStatus` 彙總；事件 `StageSuccess`，`:316`、`:338`）。
3. 用真實統計**回頭重新優化剩餘計畫**：`replaceWithQueryStagesInLogicalPlan(...)`（`:364`）把已完成的部分換成 `LogicalQueryStage`，`reOptimize(logicalPlan)`（`:371`、定義於 `:813`）重跑 logical 優化 + 重新 planning。
4. **只有新計畫成本不劣於舊計畫才採用**：比較 `costEvaluator.evaluateCost(...)`（`:374`–`:375`），`if (newCost < origCost || ...)` 才 `currentPhysicalPlan = newPhysicalPlan`（`:376`–`:383`）。
5. 回到 1，對下一批 stage 重複，直到 `result.allChildStagesMaterialized`（`:288`）。

具體的執行期改寫由 `queryStageOptimizerRules`（`:138`–`:146`）與 prep 規則（`:111`–`:133`）負責，最關鍵的幾條：`CoalesceShufflePartitions`（`:142`，依真實資料量合併過小的 shuffle 分區，規劃期猜的分區數常太多）、`OptimizeSkewedJoin` / `OptimizeSkewInRebalancePartitions`（`:132`、`:141`，偵測傾斜分區並切分），以及 AQE 規則族「把執行後發現夠小一邊的 shuffle join 降級轉成 broadcast join」（配合 §7.5 的 `BroadcastExchangeExec` 上限；註：本次 checkout 未逐一定位該降級規則的 file:line，見文末 uncertainty）。

> → 對 arneb 的啟發：**這是 Spark 給 arneb 最重要的單一啟發**（§3、§5.3、§12.1 反覆強調）。arneb 有 Selinger DP join reorder + partition-aware cost + NDV 估算 + PredicatePushdown，但**完全沒有執行期再優化**——靜態成本模型一旦猜錯就錯到底：記憶中 q08 因靜態估計選錯 build side（builds 90M）、`partition_count` 寫死、broadcast 因可能給錯結果而被停用。這些**全都是 AQE 能在執行期用真實統計修正的問題**：「選錯 build side / 該不該 broadcast」→ AQE 用 stage 跑完的真實大小決定 shuffle→broadcast 降級（且有 row/byte 上限把關，§7.5）；「partition_count 寫死太多/太少」→ `CoalesceShufflePartitions` 依真實量合併；「傾斜」→ `OptimizeSkewedJoin` 切分。關鍵設計可直接照搬：(1) **以 exchange 邊界為天然的「重優化檢查點」**——arneb 的 fragment 邊界本就是 REMOTE exchange，與 Spark 的 QueryStage 邊界同構，已具備切點；(2) **以真實統計（已物化分區的大小/行數）餵回成本模型**，arneb 的 Flight exchange 完全可在 fragment 完成時回報這些；(3) **新計畫成本不劣於舊才採用**（`newCost ≤ origCost`），是安全護欄，避免再優化反而變糟。Trino 只有 dynamic filtering，沒有「拿執行期統計回頭改物理計畫」——arneb 若補上這一條，等於補上靜態成本模型一切誤判的安全網。

### 7.7 Executor 執行緒模型：thread-per-task（非 Trino time-slice）

最後校正一個易混淆點。Spark executor 上的 task 執行是 **thread-per-task**：`Executor` 的 `threadPool` 由 `Executors.newCachedThreadPool(...)`（`core/src/main/scala/org/apache/spark/executor/Executor.scala:313`）建立，每個 task 包成 `TaskRunner extends Runnable`（`:687`–`:691`），由 `threadPool.execute(tr)`（`:563`）丟進池子——**一個執行緒跑一個 task 直到完成**，task 是排程與容錯的單位（§10.2 與 Trino 對照）。更值得借鏡的是 `TaskRunner` 是該 task 的**單一 owner object**：它在 `run()` 內建立 `TaskMemoryManager`（`:822`）、反序列化 `Task`（`:849`）、`setTaskMemoryManager`（`:852`）、跑 `task.run`（`:888`），並在 `finally` 主動斷言記憶體全數歸還——`taskMemoryManager.cleanUpAllAllocatedMemory()`（`:899`）若 `freedMemory > 0 && !threwException`（`:901`）就判定 `Managed memory leak detected`（`:902`），依 `spark.unsafe.exceptionOnMemoryLeak`（`:904`）拋 internalError 或 logWarning。亦即 task 結束 = 一道 **fail-loud 的「記憶體必須全歸還」離場關卡**。

> → 對 arneb 的啟發：別把 Spark 想成 Trino 那種「中央 time-slice 協作式排程器」——它不是。Spark 是 thread-per-task（執行緒池一執行緒跑一 task 到完），靠 OS/JVM 排程公平性。arneb 的「每 tokio task 持有 semaphore permit 跑完整個 operator」其實**結構上更接近 Spark 的 thread-per-task**，而非 Trino 的 time-slice。所以 arneb 的 exchange stall / deadlock 問題，與其改成 Trino 式 time-slice，不如先借鏡 Spark 在 **shuffle 拉取端的位元組量主動流控**（§7.3）與 **落地式 shuffle 解除 producer/consumer 生命週期耦合**（§7.2）——permit 死等的根因之一是「消費者必須在線」，而 Spark 落地後消費者不必在線，從根上解掉這種耦合。

> → 對 arneb 的啟發（task owner object + 離場關卡）：arneb 的 fragment/task 應有一個對應 `TaskRunner` 的**單一 owner**，集中持有並負責歸還該 fragment 的 memory / cancel / metrics / failure；更關鍵是把「**此 fragment 是否已歸還所有 pooled 記憶體**」做成 task 生命週期結束的**固定 fail-loud 離場關卡**（對應 `cleanUpAllAllocatedMemory` → `freed>0` 即報 leak，`Executor.scala:899-904`）。這直攻記憶中反覆出現的「未追蹤 Arrow / `JoinHashMap` hashbrown anon 撞 cgroup」OOM——與 §12.3 的 unmanaged-memory 追蹤**互補而非重複**：§12.3 是**入場決策層**（配置前先扣帳），此處是**離場斷言層**（task 結束時帳不平就吵）。下一步驗證項：把「fragment 結束斷言 pooled 記憶體全歸還」做成 arneb 實際的 fail-loud 關卡。

---

## 8. 記憶體管理、資源控管與 spill-to-disk

Spark 的記憶體模型與 Trino 有兩個根本差異：(1) Spark 把**執行（execution）**與**儲存（storage / cache）**兩種用途放進同一個池子裡並允許彼此動態借用；(2) Spark 的 spill 是**協作式（cooperative）**——記憶體壓力來時，由 `TaskMemoryManager` 反向呼叫各 `MemoryConsumer` 的 `spill()` callback，請它們主動把資料吐到磁碟，而非被動等 OOM。這正是 `common.md` §5「統一記帳 + spill」鐵律的工業範本。下圖是整個記憶體子系統的骨架：

```
                 MemoryManager (abstract)
                 ├─ onHeapExecutionMemoryPool   ┐
                 ├─ offHeapExecutionMemoryPool   │ 4 個 pool
                 ├─ onHeapStorageMemoryPool      │
                 └─ offHeapStorageMemoryPool    ┘
                        ▲ (唯一子類)
                 UnifiedMemoryManager
                 「execution ⇄ storage 軟邊界、可互借」
                        ▲
                        │ acquireExecutionMemory(bytes, taskAttemptId, mode)
                        │
   TaskMemoryManager (每個 task 一個)  ──── allocatePage() / page table
        │  consumers: HashSet<MemoryConsumer>
        │  記憶體不足 → 對其他 consumer 呼叫 spill(size, trigger)  ← 協作式回收
        ▼
   MemoryConsumer (abstract)            例: ShuffleExternalSorter / Spillable
        spill(size, trigger): long       (子類各自實作把資料落地)
```

### 8.1 統一記憶體管理：execution / storage 軟邊界與動態借用

`MemoryManager`（`core/src/main/scala/org/apache/spark/memory/MemoryManager.scala:39`）是抽象基類，建構時就持有四個池：on/off-heap × execution/storage（`MemoryManager.scala:50`、`:52`、`:54`、`:56`，皆 `@GuardedBy("this")`）。它把抽象 API（`acquireStorageMemory`、`acquireExecutionMemory`、`releaseExecutionMemory`）留給唯一的具體子類 `UnifiedMemoryManager`（`acquireExecutionMemory` 宣告於 `MemoryManager.scala:117`–`:121`）。

`UnifiedMemoryManager`（`core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala:58`）的 class javadoc 把規則寫得很清楚（`UnifiedMemoryManager.scala:34`–`:56`）：

- execution 與 storage 共用一塊區域 = `(heap − 300MB) × spark.memory.fraction`（**預設 0.6**），其中 storage 預設占 `× spark.memory.storageFraction`（**預設 0.5**），故 storage region ≈ heap 的 0.3（config 定義於 `internal/config/package.scala:495` `MEMORY_FRACTION`、`:485` `MEMORY_STORAGE_FRACTION`，預留系統記憶體 `RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024`，`UnifiedMemoryManager.scala:264`）。
- **storage 可借 execution 的閒置記憶體**，但 execution 要回收時，會逐出（evict）被借走的快取區塊。
- **execution 也可借 storage 的閒置記憶體，但 execution 記憶體「永遠不會」被 storage 逐出**（javadoc 原文：`execution memory is *never* evicted by storage`，`UnifiedMemoryManager.scala:47`–`:48`）——這是刻意的不對稱，因為實作雙向逐出太複雜。

動態借用的核心是 `acquireExecutionMemory`（`UnifiedMemoryManager.scala:134`）。它定義一個內部函式 `maybeGrowExecutionPool`（`:160`），當 execution 不夠時，計算「可從 storage 回收的量」= `max(storagePool.memoryFree, storagePool.poolSize − storageRegionSize)`（`:166`–`:168`），呼叫 `storagePool.freeSpaceToShrinkPool(...)`（`:171`，會觸發 `memoryStore.evictBlocksToFreeSpace`），把空間從 storage pool 搬到 execution pool（`decrementPoolSize` / `incrementPoolSize`，`:173`–`:174`）。反向地，`acquireStorageMemory`（`:206`）在 storage 不夠時，從 execution 的閒置空間借（`:242`–`:245`），並在區塊本身就大於上限時 fail fast（`:227`–`:238`）。

值得注意的是這版 `UnifiedMemoryManager` 還引入了 **unmanaged memory** 追蹤（`UnifiedMemoryManager.scala:69`–`:107`、`:258`–`:425`）：對「自管記憶體、不走 Spark 池」的元件（javadoc 舉例 RocksDB state store、native library、off-heap cache）提供一個註冊 + 背景輪詢機制（`registerUnmanagedMemoryConsumer`、`startPollingIfNeeded`、`pollUnmanagedMemoryUsers`），輪詢得到的量會在 `computeMaxExecutionPoolSize()`／`acquireStorageMemory` 裡從可用記憶體扣掉（`:195`–`:200`、`:223`–`:225`），避免對「Spark 看不見的記憶體」過度配置。這正是「引擎承認自己有一塊追蹤不到的記憶體、於是主動把它納入決策」的設計。

```
acquireExecutionMemory(numBytes, taskAttemptId, mode):
   ┌─ executionPool, storagePool ← (依 mode 選 on/off-heap)
   │
   ├─ executionPool.acquireMemory(numBytes, taskAttemptId,
   │       maybeGrowPool = maybeGrowExecutionPool,   ← 不夠就逐出 storage 借空間
   │       computeMaxPoolSize = computeMaxExecutionPoolSize) ← 扣掉 unmanaged
   └─ 回傳實際 grant 到的 bytes（可能 < numBytes，呼叫端據此決定是否 spill）
```

> → 對 arneb 的啟發：Spark 把「快取記憶體」與「算子工作記憶體」放進**同一個閘門、軟邊界、動態借用**，且明確承認「execution 永不被逐出」這個不對稱，是一個成熟的取捨模型。更直接對應 arneb 痛點的是 unmanaged memory 追蹤：arneb 反覆的 OOM 根因正是「未追蹤的 Arrow 配置」（Filter/Project/Repartition 的 channel buffer、scan buffer、JoinHashMap 的 hashbrown anon）撞 cgroup 才觸發——這在 Spark 的語彙裡就是 unmanaged memory。Spark 的做法不是假裝它不存在，而是**註冊一個 consumer + 週期輪詢回報，並從可配置上限裡扣掉它**，讓決策層至少「知道有這塊」。arneb 若把每個會放大記憶體的算子（含 channel/scan buffer）都做成可被池子查詢用量的 consumer（哪怕只是估算回報），就能把「事後 RSS 探測」前移成「配置前先扣帳」，與 Trino 的「先 reserve 才 allocate」殊途同歸（`common.md` §5「記帳必須下沉到分配點」）。

### 8.2 Page-based off-heap 配置與 spill() 協作式回收 callback

`TaskMemoryManager`（`core/src/main/java/org/apache/spark/memory/TaskMemoryManager.java:56`）是每個 task 一個的記憶體中介，扮演「作業系統 page table」角色：用 13 bit 當 page number（`PAGE_NUMBER_BITS = 13`，`:61`）、51 bit 當 offset（`OFFSET_BITS = 51`，`:65`），把 64-bit 內部位址翻譯成 `baseObject + offset`，同時支援 on-heap 與 off-heap（`pageTable` `:90`、`allocatedPages` bitmap `:95`）。單頁上限 `MAXIMUM_PAGE_SIZE_BYTES = (2^31 − 1) × 8`（約 17GB，`:77`）；page 大小由 `MemoryManager.pageSizeBytes` 決定，預設依「核心數 / 安全係數 16」推算，落在 1MB ~ 64MB（`MemoryManager.scala:251`–`:271`）。

它持有 `consumers: HashSet<MemoryConsumer>`（`TaskMemoryManager.java:111`–`:112`），這是協作式 spill 的關鍵資料結構。配置流程 `acquireExecutionMemory(required, requestingConsumer)`（`:159`）：

1. 先向 `MemoryManager` 申請（`got = memoryManager.acquireExecutionMemory(...)`，`:168`）。
2. **若拿到的 `got < required`，就反向叫別的 consumer spill 來騰空間**（`:172` 起）。它用一個 `TreeMap` 把 consumers 依 `getUsed()` 排序（`sortedConsumers`，`:192`–`:200`），刻意把「正在申請的 consumer」的 key 設為 0、排到最後（`:195`）。
3. 用啟發式挑「記憶體量 ≥ 仍缺量的最小 consumer」（`ceilingEntry(required − got)`，`:205`）來 spill，沒有夠大的就挑最大的（`lastEntry()`，`:208`）；目標是「**最小化 spill 次數、又不要 spill 過量**」（`:177`–`:187` 的註解）。
4. 對選中者呼叫 `trySpillAndAcquire`（`:249`），裡面真正執行 `consumerToSpill.spill(requested, requestingConsumer)`（`:261`）並把釋放出來的記憶體重新申請（`:274`）。

`MemoryConsumer`（`core/src/main/java/org/apache/spark/memory/MemoryConsumer.java:32`）就是被回呼的抽象基類，其核心是抽象方法：

```java
// MemoryConsumer.java:84
public abstract long spill(long size, MemoryConsumer trigger) throws IOException;
```

javadoc（`:70`–`:83`）明文寫「此方法會在 task 記憶體不足時由 `TaskMemoryManager` 呼叫」，並特別警告「**為避免死鎖，spill() 內不可呼叫 acquireMemory()**」（`:76`）。`MemoryConsumer.spill()`（無參版，`:66`–`:68`）則是「建構期主動強制 spill」的入口（呼叫 `spill(Long.MAX_VALUE, this)`）。

更高層的算子（如 `ExternalSorter`、`ExternalAppendOnlyMap`）走 `Spillable`（`core/src/main/scala/org/apache/spark/util/collection/Spillable.scala:29`，繼承 `MemoryConsumer`）。它的 `maybeSpill(collection, currentMemory)`（`:86`）是「主動 spill」的決策點：每讀 32 個元素檢查一次，若 `currentMemory >= myMemoryThreshold` 就先嘗試把 threshold 加倍申請（`amountToRequest = 2 * currentMemory − myMemoryThreshold`，`:93`），申請不到才落地（`:91`–`:98`）；另外有「元素數超過 `spark.shuffle.spill.numElementsForceSpillThreshold`」的強制 spill 保險絲（`:87`–`:90`，config 於 `internal/config/package.scala:1615`）。而被動回收路徑 `override def spill(size, trigger)`（`:118`）則呼叫 `forceSpill()` 把整個 collection 倒出去，回報釋放的 bytes（`:124`–`:127`）。

```
記憶體壓力時的協作式回收回路：
  consumer A 要 allocatePage / acquireExecutionMemory  (缺 X bytes)
        │
        ▼
  TaskMemoryManager: got < required  →  挑 consumer B (用量略 ≥ X 的最小者)
        │  B.spill(X, trigger=A)            ← callback：請 B 主動落地
        ▼
  consumer B 把資料寫到本地 spill 檔、釋放記憶體、回報釋放量
        │
        ▼
  TaskMemoryManager 重新 acquire → 把空間交給 A
```

> → 對 arneb 的啟發：這正是 arneb 反覆踩到的那道牆的對照組。arneb 自己有 MemoryPool + Grace HashJoin/Sort/SemiJoin 的 spill，但 spill 的觸發是**算子自己看自己**（admission 時 fail-fast，或 grace build 自己估超了才落地），缺一個「全域協調者在記憶體吃緊時、反向叫『別人』先讓出來」的回路。Spark 的 `TaskMemoryManager.acquireExecutionMemory` → `consumer.spill(size, trigger)` 就是這個回路：申請者拿不到就觸發**其他** consumer 主動 spill，且用 `TreeMap` 啟發式挑「剛好夠大的最小者」來最小化 spill 次數。三個具體可借鏡點：(1) 把 spill 設計成「被別人觸發的 callback」而非「自己看自己」，這比 arneb 現行「事後 RSS 探測撞 cgroup」要早得多、平滑得多；(2) Spark 那條「**spill() 內絕不可再 acquireMemory()**」的死鎖鐵律，恰好對應 arneb「tokio task 持有 semaphore permit 跑完整個 operator、與 back-pressure 不相容 → deadlock」的結構性痛點——任何「回收記憶體的 callback」都必須是不再反向索取資源的、可立即完成的路徑；(3) `requestingConsumer` 的 key 被設為 0 排到最後，意味著「**先 spill 別人、最後才 spill 自己**」，這個公平性細節避免了申請者把自己剛建好的資料又倒掉的浪費。

### 8.3 ExecutionMemoryPool 的 1/2N ~ 1/N 公平配額

`ExecutionMemoryPool.acquireMemory`（`core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala:92`）保證「N 個 active task 時，每個 task 至少能拿到 1/2N、至多 1/N 的池子」（class javadoc `:33`–`:38`）。它用 `memoryForTask: HashMap[taskAttemptId, bytes]`（`:57`）追蹤每 task 用量，迴圈中重算 `maxMemoryPerTask = maxPoolSize / numActiveTasks`、`minMemoryPerTask = poolSize / (2 * numActiveTasks)`（`:128`–`:129`），拿不到 1/2N 就 `lock.wait()`、等別的 task 釋放時 `notifyAll()` 喚醒（`:139`–`:146`、`:106`）。這套機制取代了 Spark 1.6 之前的 `ShuffleMemoryManager`。storage 側的逐出由 `StorageMemoryPool.acquireMemory`（`StorageMemoryPool.scala:81`）呼叫 `memoryStore.evictBlocksToFreeSpace`（`:89`）完成，逐出會同步回呼釋放記憶體（`:91`–`:93`）。

> → 對 arneb 的啟發：Spark 在「同一 worker 上多個 task 競爭 execution 記憶體」時，用 1/2N 下限 + wait/notify 做公平配額，避免某個先跑的 task 把記憶體吃滿、逼後到者瘋狂 spill。arneb 的 worker 上多個 fragment task 並行時，目前缺這層「每 task 最低保障 + 動態重算」的仲裁；若未來把記憶體閘門做成全域單一閘門，可同步引入這種「依 active task 數動態調整每 task 配額」的公平機制，避免 deep-join 的大 build 把整個 worker 的記憶體獨占。

---

## 9. 儲存與資料來源抽象（DataSource V2）

Spark 的資料來源抽象稱為 **DataSource V2（DSv2）**，全部以 Java interface 定義在 `sql/catalyst/src/main/java/org/apache/spark/sql/connector/` 之下，分成三組：`catalog/`（catalog 與 table 抽象）、`read/`（讀取與下推）、`write/`（寫入）。它與 Trino 的 Connector SPI、arneb 的 `DataSource` trait 是同一個職責層，但 Spark 把 trait 切得更細——「能力（capability）」用一堆 `Supports*` 介面以混入（mixin）方式宣告，而非單一巨型介面。

```
 CatalogPlugin
   └─ TableCatalog            listTables / loadTable / createTable / alterTable
         loadTable(ident) → Table
                              ├─ name() / columns() / partitioning() / capabilities()
                              ├─ (mixin) SupportsRead  → newScanBuilder(options): ScanBuilder
                              └─ (mixin) SupportsWrite → newWriteBuilder(info):  WriteBuilder

 讀取路徑（batch）:
   ScanBuilder.build() → Scan.toBatch() → Batch
        Batch.planInputPartitions(): InputPartition[]   ← 切分（serializable）
        Batch.createReaderFactory():  PartitionReaderFactory
              .createReader(InputPartition):         PartitionReader<InternalRow>   逐列
              .createColumnarReader(InputPartition):  PartitionReader<ColumnarBatch> 欄式
                    PartitionReader: boolean next(); T get(); close()
```

### 9.1 Catalog 與 Table：能力以 mixin 宣告

`TableCatalog`（`sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/TableCatalog.java:51`，繼承 `CatalogPlugin`）提供 `listTables(namespace)`（`:113`）、`loadTable(ident): Table`（`:154`，並有帶版本/時間戳的 time-travel overload `:181`、`:193`）、`createTable`（`:252`）等。重點是 Spark 用 `capabilities()`（`:104`，回傳 `Set<TableCatalogCapability>`）把「catalog 支援哪些功能」做成可查詢的能力集。

`Table`（`catalog/Table.java:45`）只規定最小契約：`name()`（`:51`）、`columns()`（`:78`，取代已 deprecated 的 `schema()` `:70`）、`partitioning()`（`:85`）、`properties()`（`:92`）、`capabilities()`（`:99`，回傳 `Set<TableCapability>`）。讀寫能力**不在 `Table` 上**，而是靠 mixin：

- `SupportsRead`（`catalog/SupportsRead.java:33`，`extends Table`）只加一個方法 `ScanBuilder newScanBuilder(CaseInsensitiveStringMap options)`（`:42`）。
- `SupportsWrite`（`catalog/SupportsWrite.java:33`）只加 `WriteBuilder newWriteBuilder(LogicalWriteInfo info)`（`:39`）。

也就是說，一個唯讀來源只實作 `Table + SupportsRead`，引擎透過 `instanceof`/capability 檢查決定能不能對它做某件事——這是「介面隔離」做到極致（對應 SOLID 的 ISP）。

### 9.2 讀取路徑：Scan → Batch → InputPartition → PartitionReader

讀取分四層，每層職責單一：

| 介面 | 檔案:行號 | 角色 |
|---|---|---|
| `ScanBuilder` | `read/ScanBuilder.java:32` | 收下推、`build(): Scan`（`:33`）|
| `Scan` | `read/Scan.java:46` | `readSchema()`（`:52`）；`toBatch(): Batch`（`:79`，預設拋不支援）；另有 micro-batch / continuous stream 入口（`:98`、`:115`）|
| `Batch` | `read/Batch.java:30` | `planInputPartitions(): InputPartition[]`（`:42`）切分；`createReaderFactory()`（`:47`）|
| `InputPartition` | `read/InputPartition.java:38` | `extends Serializable`——切分描述要能序列化送到 executor（javadoc `:32` 強調 InputPartition 可序列化、PartitionReader 不必）|
| `PartitionReaderFactory` | `read/PartitionReaderFactory.java:38` | `createReader(InputPartition): PartitionReader<InternalRow>`（`:46`，逐列）；`createColumnarReader(...): PartitionReader<ColumnarBatch>`（`:54`，欄式，預設不支援）|
| `PartitionReader<T>` | `read/PartitionReader.java:39` | `extends Closeable`；`boolean next()`（`:46`）+ `T get()`（`:51`）的 pull 介面 |

兩個關鍵設計點：

1. **切分（InputPartition）由 connector 決定、且必須可序列化**：`Batch.planInputPartitions()` 回傳的陣列，每個元素是 driver 端產生、序列化後送到 executor、再由 factory 在 executor 端 `createReader` 變成實際讀取器。這把「task 的切分粒度」交給 connector（檔案/row group/partition），引擎只負責把它排到哪個 executor（見 §10）。
2. **逐列 vs 欄式是 reader 層的選項**：預設 `PartitionReader<InternalRow>` 是逐列，欄式 `PartitionReader<ColumnarBatch>` 是可選的 `createColumnarReader`。這對應到本文反覆強調的 Spark 事實——**Spark 的向量化/欄式主要活在 scan 讀取與 columnar cache，而非 operator 之間的預設傳遞單位**（operator 間預設是 row-at-a-time + whole-stage codegen 的 tight loop，§6）。

統計回報走 `SupportsReportStatistics`（`read/SupportsReportStatistics.java:33`，`extends Scan`，`Statistics estimateStatistics()` `:38`），`Statistics`（`read/Statistics.java:35`）給 `sizeInBytes()`/`numRows()`（皆 `OptionalLong`，`:36`–`:37`）。

### 9.3 Pushdown trait 家族與「無效就回原樣」契約

Spark 把每種下推做成獨立的 `Supports*` 介面（都 `extends ScanBuilder`，在 `ScanBuilder.build()` 之前由 optimizer 呼叫）：

| Trait | 檔案:行號 | 方法 | 「無效/部分」表示法 |
|---|---|---|---|
| `SupportsPushDownFilters` | `read/SupportsPushDownFilters.java:30` | `Filter[] pushFilters(Filter[])`（`:38`）/ `Filter[] pushedFilters()`（`:56`）| `pushFilters` **回傳「仍需 Spark 掃描後再評估」的 filter**；`pushedFilters` 把「已下推但仍需重評」也算進去（`:43`–`:51`）|
| `SupportsPushDownV2Filters` | `read/SupportsPushDownV2Filters.java:41` | `Predicate[] pushPredicates(Predicate[])`（`:64`）/ `pushedPredicates()`（`:88`）| 同上，且 javadoc 注明 `pushPredicates` **可能被呼叫多次**（iterative pushdown，`:33`、`:92`）|
| `SupportsPushDownRequiredColumns` | `read/SupportsPushDownRequiredColumns.java:31` | `void pruneColumns(StructType requiredSchema)`（`:43`）| 欄位裁剪（column pruning）|
| `SupportsPushDownAggregates` | `read/SupportsPushDownAggregates.java:48` | `boolean pushAggregation(Aggregation)`（`:67`）/ `supportCompletePushDown(...)`（`:57`，預設 `false`）| **回 false → Spark 會「再 GROUP BY 一次」**（partial pushdown，javadoc `:27`–`:38`）|
| `SupportsPushDownLimit` | `read/SupportsPushDownLimit.java:30` | `boolean pushLimit(int)`（`:35`）/ `isPartiallyPushed()`（`:41`，預設 `true`）| 回 false = 沒下推；`isPartiallyPushed` 表示是否仍需上層再 limit |
| `SupportsPushDownTopN` | `read/SupportsPushDownTopN.java:31` | `boolean pushTopN(SortOrder[], int)`（`:36`）/ `isPartiallyPushed()`（`:42`）| 排序 + limit |
| `SupportsPushDownOffset` | `read/SupportsPushDownOffset.java:30` | `boolean pushOffset(int)`（`:35`）| |
| `SupportsPushDownJoin` | `read/SupportsPushDownJoin.java:31` | `boolean pushDownJoin(...)`（`:58`）| 整段 join 下推（聯邦場景）|
| `SupportsPushDownTableSample` | `read/SupportsPushDownTableSample.java:30` | `boolean pushTableSample(...)`（`:36`）| TABLESAMPLE |

**「無效就回原樣」的契約用兩種型別表達**：

- `boolean` 類（limit/topN/offset/aggregate/join/sample）：connector 做不到就回 `false`，Spark 保留原本的上層算子。`supportCompletePushDown` 預設 `false`，且 `pushAggregation` 即使回 true，只要不是 complete，**Spark 仍會把 source 的輸出再 GROUP BY 一次**（`SupportsPushDownAggregates.java:27`–`:38` 的範例計畫）——意即「下推是優化、不是承諾完全代勞」。
- `Filter[]`/`Predicate[]` 類（filter/predicate）：`pushFilters` **回傳「需要掃描後再評估的 filter 陣列」**，等於把「我沒能完全消化的」原樣交還給 Spark；filter 被明確分三類——(1) 已下推不需重評、(2) 已下推但仍需重評（如 parquet row group filter）、(3) 不可下推（`SupportsPushDownFilters.java:43`–`:51`）。第 2 類「下推了但 Spark 仍重評」正是 arneb Parquet row-group pruning 的對應語意：pruning 只是少讀 row group，剩下的 row 仍要過 filter。

> → 對 arneb 的啟發：
> 1. **「無效就回原樣 / None」是強約束，Spark 用回傳型別把它寫死**——`boolean false`、或「回傳仍需重評的 filter 陣列」。arneb 的 `connectors` crate 實作 filter/projection/limit pushdown 時務必遵守同一契約：做不到就把算子原樣留給上層，否則 rule-based optimizer 會抖動。Spark 額外提醒 `pushPredicates` **可能被呼叫多次**（iterative pushdown，`SupportsPushDownV2Filters.java:33`），arneb 的 pushdown 規則若會重跑，必須保證冪等（與 §5.1 idempotence、Trino pushdown 的 `Optional.empty()` 契約同源）。
> 2. **能力用 mixin 宣告、不塞進單一巨型 trait**——arneb 目前 `DataSource` trait 較集中；若把「支援 filter pushdown / 支援 limit pushdown / 支援統計回報」拆成獨立 trait（`SupportsX`），引擎就能用 trait bound 在編譯期、或 capability 在執行期決定能對某來源做什麼，避免「呼叫了不支援的方法只能回 default」的尷尬。
> 3. **partial aggregate pushdown 模型直接可用**——arneb 已有 partial/final aggregate 切分；DSv2 的 `supportCompletePushDown` 回 false → 上層再 GROUP BY 的模型，正是把「下推到 source 的部分聚合」與「引擎側 final 聚合」接起來的標準介面，arneb 若做聯邦聚合下推可照搬（呼應 `common.md` §4「第一級別的可序列化中間狀態」）。
> 4. **InputPartition 必須可序列化、PartitionReader 不必**——arneb 的 per-file row-range split 已對齊「切分由 connector 決定」；但 Spark 明確區分「切分描述（送到 worker，要序列化）vs 讀取器（在 worker 端建，不送）」，這個分界對 arneb 把 scan split 經 Flight/RPC 描述送到 worker 端再實體化 reader 的設計是清楚的參照。`PartitionReader` 的 `next()`+`get()` pull 介面也與 arneb 的 `SendableRecordBatchStream` 同屬 pull-based。

---

## 10. 並行模型與排程

Spark 的並行模型有三個與 Trino/arneb 截然不同的關鍵點：(1) 排程與容錯的單位是 **task**，一個 task = 對一個 partition 的計算；(2) Executor 上是 **thread-per-task 跑到完成**，不是 Trino 的中央 time-slice 協作式排程；(3) task 排程帶 **資料本地性（locality）**，依 `PROCESS_LOCAL → NODE_LOCAL → NO_PREF → RACK_LOCAL → ANY` 的順序嘗試。

```
 Driver:
   DAGScheduler ──(TaskSet per stage)──▶ TaskScheduler (TaskSchedulerImpl)
                                              │ submitTasks → TaskSetManager
                                              │ resourceOffers(WorkerOffer[])
                                              ▼ 依 locality 由近到遠分派
                                         SchedulerBackend.reviveOffers()
                                              │ (launchTask RPC)
        ┌─────────────────────────────────────┴────────────────────────┐
        ▼                                                                ▼
   Executor A                                                       Executor B
     threadPool = Executors.newCachedThreadPool()                    (同左)
     launchTask → threadPool.execute(TaskRunner)                     每個 task
       TaskRunner.run(): task.run(...)  ← 一條 thread 跑到完成        一條 thread
     runningTasks: ConcurrentHashMap[taskId, TaskRunner]             跑到完成
```

### 10.1 叢集排程：TaskScheduler / TaskSetManager 與資料本地性

`TaskScheduler`（trait，`core/src/main/scala/org/apache/spark/scheduler/TaskScheduler.scala:36`）是 DAGScheduler 與底層資源之間的介面：`rootPool`（`:40`）、`start()`（`:44`）、`submitTasks(taskSet)`（`:55`）、`defaultParallelism()`（`:77`）。它把「拿到資源後怎麼跑」委派給 `SchedulerBackend`（trait，`scheduler/SchedulerBackend.scala:29`：`start()` `:32`、`reviveOffers()` `:39`、`killTask(...)` `:50`）——backend 抽象了 Standalone / YARN / K8s 等資源管理器。

唯一主要實作 `TaskSchedulerImpl`（`scheduler/TaskSchedulerImpl.scala:83`）持有 `backend`（`:178`）、`schedulableBuilder`（FIFO/FAIR 池，`:182`）、`CPUS_PER_TASK = conf.get(CPUS_PER_TASK)`（`:120`，即 `spark.task.cpus`，定義於 `internal/config/package.scala:723`–`:724`）。核心是 `resourceOffers(offers, ...)`（`:512`）：收到一批 `WorkerOffer`（每個帶 host / cores / resources），算出每 worker 可放幾個 task（`o.cores / CPUS_PER_TASK`，`:551`），對每個 TaskSet 從最近的 locality level 開始試著塞滿（`:564`–`:567`，註解明文寫出 `preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY`，`:566`）。分派完透過 `backend.reviveOffers()`/`launchTasks` 把 `TaskDescription` 送到 executor。

每個 stage 的一組 task 由 `TaskSetManager`（`scheduler/TaskSetManager.scala:56`）管理。`TaskLocality` 是個五值 enum（`scheduler/TaskLocality.scala:23`–`:25`：`PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY`）。TaskSetManager 在初始化時用 `computeValidLocalityLevels()`（`:1361`）算出「這個 TaskSet 實際有哪些 locality level」——只有當有 task 偏好某 executor 且該 executor 還活著，才加入 `PROCESS_LOCAL`（`:1364`–`:1366`）；同理 host → `NODE_LOCAL`（`:1368`–`:1370`）、rack → `RACK_LOCAL`（`:1375`–`:1377`）、`ANY` 永遠墊底（`:1379`）。`dequeueTaskHelper`（`:383`）依 `maxLocality` 由近到遠挑 task（`:401` PROCESS_LOCAL、`:404`–`:406` NODE_LOCAL、`:417`–`:422` RACK_LOCAL、`:426`–`:428` ANY），並有 **locality wait** 機制：在更近的 level 等一小段時間（`localityWaits`，`:268`）拿不到才降級到更遠的 level，用「短暫等待換更好的資料本地性」。

```
TaskSetManager.dequeueTaskHelper(maxLocality):
   PROCESS_LOCAL?  → 有偏好此 executor 的 task 就跑    (TaskSetManager.scala:401)
   NODE_LOCAL?     → 同 host 上的 task                  (:404)
   NO_PREF?        → 無偏好的 task                       (:411)
   RACK_LOCAL?     → 同 rack 的 task（少跨 rack 流量）   (:417)
   ANY             → 任意                                (:426)
   ── 配合 localityWait：在近的 level 等一下再降級
```

兩種叢集排程拓撲在 Spark 是「**靠 shuffle 物化容錯**」這個更根本的設計撐起來的：stage 之間的 shuffle 是 **disk-materialized**（map 端把排序後的輸出寫本地檔，reduce 端 fetch，見 §7.2），一旦 `FetchFailed`，DAGScheduler 依 lineage 重算丟失的 stage（§7.4）——task / stage 是天然的重試與容錯單位。這跟 Trino/arneb 的「記憶體串流 exchange、任一 task 失敗即整查詢失敗」是根本對立。

> → 對 arneb 的啟發：
> 1. **資料本地性是一級排程目標**——Spark 的 `computeValidLocalityLevels` + locality wait 把「優先把 task 排到資料所在 executor/host/rack」做成顯式的、可降級的階梯。arneb 的 coordinator 把 scan split 排到 worker 時，若 worker 與資料（MinIO/S3 region、或共置的本地檔）有親近度，借鏡這套「由近到遠、近的 level 短暫等待」能省下大量 exchange/拉取流量（與 Trino `TopologyAwareNodeSelector` 同源）。
> 2. **shuffle 物化 = 容錯的根**——這是 Spark 最值得 arneb 借鏡、但代價也最大的差異。arneb 目前 pipelined-only、無容錯執行（FTE），fragment 邊界 = REMOTE exchange，深層 join 的中間資料 materialize-then-forward 跨 stage 序列化是延遲牆，而 exchange 在飽和下還曾 silent-truncate（consumer drop，已用 must_drain 改成 fail-loud）。Spark 的對照組告訴我們：**把 exchange 落地（哪怕只是本地排序檔）就同時換到了「task 可重試、整查詢不必因單 task 失敗而死」**。arneb 不必整套照搬，但「exchange 邊界可選擇物化以換取重試能力」是清楚的演進方向——這也正是 Trino FTE 走的路。
> 3. **control-plane 並發進入點要有一致的鎖／序列化紀律**——與 §7.1 的 `DAGScheduler` 單執行緒 event loop 相反，`TaskSchedulerImpl` 是被**多種 thread 並發進入**的：其 class doc（`TaskSchedulerImpl.scala:58-64`）明列被 DAGScheduler event loop、RPC handler、offer-revival、task-result-getter 等執行緒呼叫，故 public API 以 `synchronized` 護 state，並明定**固定鎖順序**（`:58`「don't try to lock the backend while we are holding a lock on ourselves」）。對應 arneb：coordinator 的 `QueryTracker` / `NodeRegistry` / `NodeScheduler` 同樣被 heartbeat、Flight 完成事件、新查詢三個來源並發進入——記憶中的 silent-truncate / `consumer_gone` / `must_drain` 競態，根因之一就是缺這套一致的鎖序與單一狀態進入點。把「控制面狀態只能從固定序列化路徑變更」當鐵律，能從結構上消除這類競態。

### 10.2 Worker 端執行緒模型：thread-per-task 跑到完成

`Executor`（`core/src/main/scala/org/apache/spark/executor/Executor.scala:249`）的執行緒模型與 Trino 截然不同。它持有一個 **cached thread pool**：

```scala
// Executor.scala:307–313
private[executor] val threadPool = {
  val threadFactory = new ThreadFactoryBuilder()
    .setDaemon(true)
    .setNameFormat(s"$TASK_THREAD_NAME_PREFIX-%d")   // "Executor task launch worker-%d"
    .setThreadFactory((r: Runnable) => new UninterruptibleThread(r, "unused"))
    .build()
  Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
}
```

`launchTask`（`:551`）為每個 task 建一個 `TaskRunner`（`extends Runnable`，class 於 `:687`），放進 `runningTasks: ConcurrentHashMap[Long, TaskRunner]`（`:423`），然後 `threadPool.execute(tr)`（`:563`）。`TaskRunner.run()`（`:806`）內部直接 `task.run(taskAttemptId, attemptNumber, ...)`（`:888`–`:894`）——**一條 thread 從頭到尾把這個 task 跑完才釋放**，期間不讓給別的 task。這就是「thread-per-task、跑到完成」：並行度上限 = thread pool（受 `spark.executor.cores / spark.task.cpus` 約束的 slot 數）能同時跑幾條，而非中央排程器在多個 task 間切時間片。

對照 Trino：Trino 的 worker 是中央 `TaskExecutor`（`TimeSharingTaskExecutor`）持有固定數量的 runner thread，每個 split runner 跑一個 1 秒的 `SPLIT_RUN_QUANTA` 量子就讓出，靠 `MultilevelSplitQueue` 在所有 query 的 split 之間做協作式 time-slice 輪轉。Spark 沒有這層——**Spark 的「公平性」靠的是 task 切得夠小（一個 partition）+ thread pool slot 數**，而不是時間片搶占。

| 維度 | Spark | Trino |
|---|---|---|
| Worker 執行單位 | task（一個 partition） | split runner（Driver 的一次執行） |
| 執行緒模型 | thread-per-task，跑到完成（`threadPool.execute(TaskRunner)`，`Executor.scala:563`）| 中央 `TaskExecutor` time-slice，1 秒量子讓出 |
| 並行上限 | thread pool slot（`cores / task.cpus`）| runner thread 數，跨 query 共享 |
| 公平性來源 | task 粒度小 + slot 數 | `MultilevelSplitQueue` 時間片輪轉 |
| 容錯 | task/stage 可重試（shuffle 物化）| pipelined 下任一 task 失敗整查詢失敗 |

> → 對 arneb 的啟發（一條 Rust 專屬鐵律，與 §10.3 的 permit-deadlock **正交**）：Spark 把 task body 跑在**獨立的 thread-per-task 執行緒**（`Executor.scala:307-313` 的 `newCachedThreadPool` + `:563` 的 `threadPool.execute(TaskRunner)`），與 driver 的 event loop / RPC reactor 完全分離。arneb 在 tokio 上要當心相反的陷阱：**CPU-bound 的 Arrow compute kernel（join build/probe、aggregate、sort、大量 `take`/cast）若直接在 tokio 的 async worker 執行緒上同步跑，會卡住該 reactor 執行緒、餓死同 runtime 上的 I/O（Flight 收送、heartbeat）**，外觀就是「莫名的 exchange 變慢/停頓」。正解是把這類算子計算丟到 `spawn_blocking` 或專屬 Rayon thread pool，讓 tokio 的 async 執行緒只跑 I/O 與排程。**這與「permit 持有跨越 back-pressure await 點 → deadlock」（§10.3）是兩個不同問題**：一個是「該讓出時沒讓出」，一個是「該隔離的 CPU 工作卻塞進 reactor」，兩者都會讓串流莫名停滯，須分開處理。下一步驗證項：稽核 arneb 現況是否已用 `spawn_blocking`/Rayon 隔離算子計算。

### 10.3 TorrentBroadcast：BitTorrent 式的廣播分發

當 broadcast join 要把小表（或共享變數）散佈到所有 executor 時，Spark 用 `TorrentBroadcast`（`core/src/main/scala/org/apache/spark/broadcast/TorrentBroadcast.scala:60`）。class javadoc（`:38`–`:52`）明文說它是 **BitTorrent-like** 的實作，機制是：

1. **driver 切塊**：把序列化後的物件切成小 chunk，`writeBlocks`（`:139`）把每個 piece 以 `BroadcastBlockId` 存進 driver 的 `BlockManager`（`putBytes(..., MEMORY_AND_DISK_SER, tellMaster = true)`，`:172`），`numBlocks` 記錄塊數（`:100`）。
2. **executor 拉塊、且互相當種子**：`readBlocks`（`:189`）在每個 executor 上以 **`Random.shuffle` 打亂的順序**（`:195`，避免大家都先搶同一塊、造成熱點）逐塊取得——先試本地 `getLocalBytes`（`:201`），沒有才 `getRemoteBytes`（`:206`）從 **driver 或其他 executor** 拉。拿到後立刻 `putBytes(..., MEMORY_AND_DISK_SER, tellMaster = true)`（`:218`）存進自己的 BlockManager 並通報 master——**於是這個 executor 馬上也變成別人可以拉取的種子**。

javadoc 點出目的（`:51`–`:52`）：「**防止 driver 成為瓶頸**」——若是 driver 直接送 N 份給 N 個 executor，driver 的出向頻寬就是瓶頸；BitTorrent 式分發讓 executor 之間互相散佈，把分發負載攤平。

```
 driver: 物件 → 切成 piece0..pieceK，存自己的 BlockManager
              │
    ┌─────────┼─────────┐  (每個 executor 隨機順序拉塊)
    ▼         ▼         ▼
 exec A     exec B     exec C
  拉 p2 ⇄ 互相當種子 ⇄ 拉 p0
  拉完即 putBytes(tellMaster=true) → 成為新種子，分擔後續拉取
```

> → 對 arneb 的啟發：
> 1. **thread-per-task vs time-slice 的取捨要想清楚**——arneb 是 tokio task-per-fragment（async、pull-based），介於 Spark（OS thread-per-task、跑到完成）與 Trino（中央 time-slice 協作式）之間。arneb 反覆踩到的「tokio task 持有 semaphore permit 跑完整個 operator 生命週期、與 stream back-pressure 不相容 → deadlock / exchange stall / OutputBuffer 死等」，本質是**借了 Spark 的「占住資源跑到完成」語意、卻又用了 Trino 的「記憶體串流、需要協作讓出」執行模型**，兩者不相容。Spark 之所以能 thread-per-task 而不死鎖，是因為它的 task 之間靠 shuffle 物化解耦——上游 task 寫完檔就結束、下游 task 再開新 thread 讀檔，沒有「上下游同時在飛、互相 back-pressure」這回事。arneb 若維持 pipelined 串流，就更該往 Trino 的協作式讓出靠攏（permit 不該跨越會阻塞在 back-pressure 上的 await 點）；若想要 Spark 式的「占住跑完」，前提是 exchange 解耦（物化）。這是 arneb deadlock 的結構性根因，最值得想透的單一設計分歧（呼應 `common.md` §3.1）。
> 2. **broadcast 分發用 BitTorrent 式去中心化**——arneb 歷史上 broadcast join 被停用（會給錯結果、且是分散式工作）。當未來重啟 broadcast small-dim（記憶中是消除 q09 那種 179M lineitem shuffle 的潛在大槓桿）時，分發機制本身可借鏡 TorrentBroadcast：別讓 coordinator 一台把小表送 N 份給 N 個 worker（coordinator 出向頻寬瓶頸 + 單點），而是讓 worker 之間互相當種子、隨機順序拉塊散佈。arneb 走 Arrow Flight（gRPC/HTTP2、近零拷貝），天然適合做這種 peer-to-peer 的塊狀分發。
> 3. **task 粒度 + slot 數做公平性**——Spark 不靠搶占、靠「task 切得夠小」。arneb 的 fragment task 若粒度過大（deep-join 的單一大 build task），在 worker 上就會像 Spark 的大 task 一樣獨占一個 slot、拖累整體；把 fragment 切得更細、或引入 §8.3 那種「每 task 最低配額」的記憶體仲裁，是讓多查詢/多 fragment 公平共存的兩條互補路徑。

---

## 11. 程式碼地圖（關鍵目錄與模組職責對照）

下表把 Spark 的模組 / package（路徑皆已核實存在於本次 checkout，commit `072994d3`）對應到 arneb 的 crate（概念對照，非一對一實作對應）。arneb crate 集：`common`、`sql-parser`、`planner`、`execution`、`connectors`、`hive`、`protocol`、`scheduler`、`rpc`、`server`。

| Spark module / package（已核實路徑）| 對應 arneb crate（概念對照）| 職責 |
|---|---|---|
| `core/` → `org.apache.spark.SparkContext` | `server` / `scheduler` | 計算框架入口，連 cluster、建 RDD、持有三個 scheduler（Spark 特有的通用 RDD 抽象層，arneb 無對應）|
| `core/.../scheduler`（`DAGScheduler`、`TaskScheduler`、`TaskSchedulerImpl`、`TaskSetManager`、`SchedulerBackend`）| `scheduler`（`QueryTracker`、`NodeRegistry`、`NodeScheduler`）| 依 shuffle 邊界切 `ShuffleMapStage`/`ResultStage`、派 task、locality 排程、FetchFailed 重算 |
| `core/.../shuffle`（`SortShuffleManager`、`ShuffleWriter`、`IndexShuffleBlockResolver`、`BlockStoreShuffleReader`、`ShuffleBlockFetcherIterator`）| `rpc`（Flight exchange）/ `execution`（ExchangeExec / OutputBuffer）| **disk-materialized shuffle**：map 端寫 `.data`/`.index`、reduce 端節流 fetch（arneb 走記憶體串流 Flight，是最根本的對立點）|
| `core/.../memory`（`MemoryManager`、`UnifiedMemoryManager`、`TaskMemoryManager`、`MemoryConsumer`、`ExecutionMemoryPool`、`StorageMemoryPool`）| `execution`（`MemoryPool`、Grace HJ/Sort/SemiJoin spill）/ `server` | 統一記帳（execution⇄storage 軟邊界）+ 協作式 `spill()` callback + 1/2N 公平配額 + unmanaged memory 追蹤 |
| `core/.../storage`（`BlockManager`、`MapOutputTracker` 系列、`MapStatus`）| `rpc` / `execution` | shuffle block 的存放、定位、map output 統計帳本（`MapOutputTrackerMaster` = AQE 真實統計來源）|
| `core/.../broadcast`（`TorrentBroadcast`）| `rpc`（Flight）/ `execution`（broadcast join，歷史停用）| BitTorrent 式去中心化廣播分發，避免 driver 出向瓶頸 |
| `sql/api` → `org.apache.spark.sql.SparkSession`（abstract）| `protocol` / `server` | Classic 與 Connect 共用的對外 API 層 |
| `sql/api/src/main/antlr4/.../parser`（`SqlBaseLexer.g4`、`SqlBaseParser.g4`）| `sql-parser`（文法層）| ANTLR4 文法（與 Trino 同源於 Presto `SqlBase.g4`）|
| `sql/catalyst/.../parser`（`AbstractParser`、`AbstractSqlParser`、`CatalystSqlParser`、`AstBuilder`）| `sql-parser` | SQL 字串 → unresolved `LogicalPlan`（兩階段 SLL→LL 解析、visitor 建 unresolved 節點）|
| `sql/catalyst/.../analysis`（`Analyzer`、`CheckAnalysis`、`ResolveRelations`/`ResolveReferences`/`ResolveFunctions`、`TypeCoercion`/`AnsiTypeCoercion`）| `planner`（analyze / resolver 階段）| unresolved → resolved `LogicalPlan`（RuleExecutor fixed-point 重寫、`resolved` 旗標、隱式型別轉換、`AnalysisException` 關卡）|
| `sql/catalyst/.../catalog`（`SessionCatalog`）| `catalog`（CatalogManager，跨 crate）| current db / temp view / 持久表的解析優先序、函式查詢 |
| `sql/catalyst/.../optimizer`（`Optimizer`、`CostBasedJoinReorder`、`operatorOptimizationRuleSet`）| `planner`（LogicalOptimizer + Selinger DP join reorder）| RBO（pushdown/pruning/簡化）+ CBO（join 重排，預設 OFF、需 rowCount）|
| `sql/catalyst/.../rules`（`Rule`、`RuleExecutor`、`Batch`/`Strategy`/`Once`/`FixedPoint`）| `planner`（rule-pass 框架）| rule 框架：fixed-point + `fastEquals` 收斂 + `maxIterations` + Once idempotence 檢查 |
| `sql/catalyst/.../expressions`（`UnsafeRow`）+ `.../expressions/codegen`（`CodeGenerator`）| `execution`（`PhysicalExpr` 求值，向量化）| 表達式與 Tungsten 列格式 / codegen（arneb 走向量化解譯，不需 codegen）|
| `sql/catalyst/.../plans`（`LogicalPlan`、`Statistics`、`statsEstimation/*`）| `planner`（LogicalPlan + NDV/stats 估算）| 邏輯計畫節點與自底而上的統計傳播（`BasicStatsPlanVisitor` vs `SizeInBytesOnlyStatsPlanVisitor`）|
| `sql/catalyst/.../planning`（`QueryPlanner`、`GenericStrategy`）| `planner`（physical planning） | strategies + `planLater` 遞迴的物理計畫框架 |
| `sql/catalyst/.../vectorized`（`ColumnVector`、`ColumnarBatch`、`ArrowColumnVector`）| `execution`（Arrow `RecordBatch`）| 欄式批次抽象（與 Arrow `RecordBatch`、Trino `Page` 三者同構）|
| `sql/catalyst/.../connector`（`TableCatalog`、`Table`、`SupportsRead`/`SupportsWrite`、`read/*` pushdown trait、`PartitionReader`）| `connectors`（`DataSource` / `ConnectorFactory` trait）/ `catalog` | DataSource V2：mixin 能力宣告 + Scan→Batch→InputPartition→PartitionReader + 「無效回原樣」pushdown 契約 |
| `sql/core/.../classic`（`SparkSession`、`Dataset`）| `protocol` / `server` | Classic 模式的 session 實作與 `sql()` 進入點 |
| `sql/core/.../execution`（`QueryExecution`、`SparkPlan`、`SparkPlanner`、`SparkOptimizer`）| `execution`（`ExecutionContext`、PhysicalPlan）/ `planner` | 生命週期總指揮（七個 lazy val）+ 物理計畫節點與 RDD 降階 |
| `sql/core/.../execution/adaptive`（`AdaptiveSparkPlanExec`、`InsertAdaptiveSparkPlan`、`AQEOptimizer`、`CoalesceShufflePartitions`、`OptimizeSkewedJoin`、`DynamicJoinSelection`、`QueryStageExec`）| （arneb 無對應——最高優先借鏡點）| **AQE 執行期再優化**：QueryStage 切點 + 真實 `MapOutputStatistics` 回頭重寫剩餘物理計畫 |
| `sql/core/.../execution/exchange`（`ShuffleExchangeExec`、`EnsureRequirements`、`BroadcastExchangeExec`）| `execution`（ExchangeExec / RepartitionExec）/ `planner`（fragmenter）| 分佈需求推導自動插 shuffle/broadcast（arneb 缺的 partitioning-property 推導）|
| `sql/core/.../execution/joins`（`BroadcastHashJoinExec`、`ShuffledHashJoinExec`、`SortMergeJoinExec`、`ShuffledJoin`）| `execution`（hash join / semi join 等算子）| join 物理算子 + 各自宣告 `requiredChildDistribution`/`requiredChildOrdering` |
| `sql/core/.../execution/aggregate`（`HashAggregateExec`、`UnsafeFixedWidthAggregationMap`）| `execution`（partial/final aggregate）| 雜湊聚合 + `BytesToBytesMap` 緊湊雜湊（對應 arneb 的 JoinHashMap flatten）|
| `sql/core/.../execution/datasources`（`FileSourceScanExec`、`VectorizedParquetRecordReader`）| `connectors`（file / Parquet connector）| 檔案掃描 + 向量化 Parquet 讀取（columnar 只活在 scan 層）|
| `sql/core/.../execution`（`WholeStageCodegenExec`、`CollapseCodegenStages`、`Columnar`/`ColumnarToRowExec`）| `execution`（向量化 operator pipeline）| whole-stage codegen 融合 + row↔columnar 橋接（arneb 全程 Arrow，不需 row 軸）|
| `sql/core/.../execution/vectorized`（`WritableColumnVector`、`OnHeapColumnVector`、`OffHeapColumnVector`）| `execution`（Arrow array buffer）| 可寫欄向量的 on-heap / off-heap 兩種記憶體模式 |
| `sql/hive`、`sql/hive-thriftserver` | `hive`（HiveDataSource / HMS client）| Hive 相容層與 JDBC/ODBC server |
| `core/.../executor`（`Executor`、`TaskRunner`）| `execution` / `rpc`（worker 執行）| thread-per-task 跑 RDD partition（與 arneb tokio task + permit 對立）|

---

## 12. 對 arneb（Rust 自建引擎）的具體啟發與可借鏡

依重要性排序。前四項（AQE、容錯 disk shuffle、統一記帳協作式 spill、分佈推導）放最前面，因為它們正是 **arneb 缺、且 Spark 強**的點——也正好打中記憶中反覆出現的 q05/q08/q09/q21 痛點。

### 12.1 【最高優先】AQE 執行期再優化：靜態成本模型一切誤判的安全網

arneb 的規劃痛點清單——q08 靜態估計選錯 build side（builds 90M、probe 20K）、`partition_count` 寫死、broadcast 因可能給錯結果而被停用——**全部是「規劃時資訊不足」的病**。Spark 的解法不是把靜態 cost model 調得更準（它連 CBO join reorder 都預設關，見 §5.2），而是 **AQE：接受規劃時會猜錯，在執行期用真實統計回頭重寫剩餘的物理計畫**（`AdaptiveSparkPlanExec.scala:65-68`、`:284-394`、`:813`）。

可直接照搬的三個設計：

1. **以 exchange 邊界為天然的「重優化檢查點」**——arneb 的 fragment 邊界本就是 REMOTE exchange，與 Spark 的 QueryStage 切點（`createQueryStages`，`:284`）同構，已具備切點，目前只是把上游真實產出量丟掉了。
2. **以真實統計（已物化分區的大小/行數）餵回成本模型**——Spark 的 `MapOutputStatistics.bytesByPartitionId`（`QueryStageExec.scala:231-235`）是 map 端寫完磁碟後的精確值；arneb 的 Arrow Flight exchange 完全能在 fragment 完成時回報 per-partition row/byte。
3. **新計畫成本不劣於舊才採用**（`newCost ≤ origCost`，`AdaptiveSparkPlanExec.scala:376`；`SimpleCostEvaluator` 就是數 shuffle 個數）——這是安全護欄，避免再優化反而變糟，與 arneb 用 `must_drain` 改 silent-truncate 為 fail-loud 同源的「寧可吵也不要默默給錯」紀律。

三個 runtime 能力直接對應 arneb 痛點：「選錯 build side / 該不該 broadcast」→ `DynamicJoinSelection` 用實測大小決定 shuffle→broadcast 降級（且有 row/byte 上限把關）；「partition_count 寫死太多/太少」→ `CoalesceShufflePartitions` 依真實量合併；「傾斜」→ `OptimizeSkewedJoin` 切分。**務實切入點**（§5.3 啟發 5）：因 arneb 是 pipelined-only、記憶體串流、無 FTE，上游一旦串流走就無法重讀，所以**只在 stage 邊界、上游尚未開始 consume 之前**用剛拿到的 produce-side 統計調整下游，而不要奢望 Spark 那種「整棵計畫隨時可重切」（那需要先補 12.2 的地基）。

### 12.2 【最高優先】容錯 disk shuffle / FTE 演進路徑：exchange 落地換可重算

arneb 的 exchange 在飽和下曾 silent-truncate（consumer drop），且任一 task 失敗即整查詢失敗——這是「pipelined exchange 無物化、無重算」的必然代價。Spark 給出完整的演進藍圖（§7.2 / §7.4）：

1. **落地式 shuffle**（`SortShuffleManager` → `IndexShuffleBlockResolver` 寫 `.data`/`.index`，原子提交）讓中間結果可重讀、可重複 fetch。
2. **`MapOutputTracker` 式「哪些分區已就緒」帳本**，使重算能精準到分區（`findMissingPartitions`，`ShuffleMapStage.scala:92-96`），而非整 stage 重跑。
3. **失敗事件統一回單執行緒 driver 大腦決策**（`handleTaskCompletion` 的 `FetchFailed` 分支：解註銷 → 延遲 resubmit → 只算 missing，`DAGScheduler.scala:2395-2517`），並有重試上限（`maxStageAttempts`）避免無限重算。
4. **統一的 block 抽象作為地基**——上述「落地 / 帳本 / 重算」能成立，是因為 Spark 有一層 `BlockManager`（`storage/BlockManager.scala:177-178` class doc：「Manager running on every node... putting and retrieving blocks both locally and remotely into various stores (memory, disk, and off-heap)」）把 shuffle 檔、RDD cache、broadcast 統一成「有 id、可落 memory/disk/off-heap、本地拿不到才 `getRemoteBytes`（`:1327`）跨節點 fetch、由 master 帳本定位」的 block。arneb 目前 spill 是各算子自管本地檔、Flight 是另一套記憶體傳輸，**缺這層統一抽象**；做可重讀落地式 shuffle 的務實第一步，就是把「本地 spill 檔 + 遠端 fetch」收斂成單一 block store 介面，FTE 的「可重 fetch / 只重算缺分區」才有掛載點。注意此 block 層必須與 §12.3 的 MemoryPool **同源計帳**，否則又會變成一塊未追蹤記憶體。

代價是延遲（落地 + barrier 等待 + 重讀），正是 arneb 現在用串流換來的低延遲所放棄的。**實務折衷 = 可選的容錯 shuffle**：只在指定昂貴 stage 邊界落地、其餘維持 Flight 串流——把它當「第二套可插拔執行語意」做（Trino FTE 走的也是這條路），不是「再多堵一個 must_drain 洞」。附帶紅利：落地解除了 producer/consumer 的生命週期耦合（消費者不必在線），這正是 arneb permit 死等 deadlock 的根因之一（§7.7）。

### 12.3 【高】UnifiedMemoryManager + MemoryConsumer 協作式 spill / 統一記帳

arneb 的 OOM 根因是「只追蹤部分 operator」——Filter/Project/Repartition 的 channel buffer、scan buffer、JoinHashMap 的 hashbrown anon 撞 cgroup 才觸發。Spark 提供三件可借鏡（§8）：

1. **全域單一閘門 + execution/storage 軟邊界**（`UnifiedMemoryManager`），且明確承認「execution 永不被 storage 逐出」這個刻意不對稱。
2. **unmanaged memory 主動納入決策**（`registerUnmanagedMemoryConsumer` + 背景輪詢 + 從上限扣掉，`UnifiedMemoryManager.scala:69-107`）——這正是 arneb「未追蹤的 Arrow / hashbrown anon」的對應物。Spark 不假裝它不存在，而是註冊 consumer + 輪詢回報。arneb 應把每個會放大記憶體的算子（含 channel/scan buffer、`JoinHashMap`）都做成可被池子查詢用量的 consumer，把「事後 RSS 探測」前移成「配置前先扣帳」（呼應 `common.md` §5「記帳下沉到分配點」）。
3. **協作式 spill 回路 + 死鎖鐵律**（`TaskMemoryManager.acquireExecutionMemory` → 其他 consumer 的 `spill(size, trigger)`，`TaskMemoryManager.java:159-274`）——申請者拿不到就反向叫「別人」先 spill，用 `TreeMap` 啟發式挑「剛好夠大的最小者」（最小化 spill 次數），且 `requestingConsumer` 排到最後（先 spill 別人、最後才 spill 自己）。最關鍵的鐵律是 **`spill()` 內絕不可再 `acquireMemory()`**（`MemoryConsumer.java:76`）——這恰好對應 arneb「permit 持有跑完整個 operator → deadlock」的結構性痛點：任何「回收記憶體的 callback」都必須是不再反向索取資源、可立即完成的路徑。比 arneb 現行「算子自己看自己 + admission fail-fast」要早、要平滑。

### 12.4 【高】EnsureRequirements 分佈推導（解 Q05/Q09）

`EnsureRequirements`（`EnsureRequirements.scala:51,56,71,129`）正是 arneb 規劃層**最缺的那塊「分佈推導（partitioning-property derivation）」**——對應記憶中 Q05/Q09 因缺 broadcast 與 partition-property 推導而 OOM / 慢。Spark 的模型是：每個算子用 `requiredChildDistribution` / `requiredChildOrdering` 宣告需求（`BroadcastHashJoinExec`/`ShuffledJoin`/`SortMergeJoinExec` 各自宣告），規則層比對「子節點現有 `outputPartitioning` 是否 `satisfies`」——**已滿足就不插 exchange、不滿足才補最小代價的一道**，且兩邊不一致時只重洗較小一邊（`createShuffleSpec`，`:162-194`）。arneb 有 Selinger DP + partition-aware cost + NDV，但缺的正是這套「需求-滿足-差補」框架（DataFusion 的 `EnforceDistribution`、Trino 的 PropertyDerivations 同源）。此外 `BroadcastExchangeExec` 的 row/byte 上限**主動拒絕廣播過大表**（`:188`、`:210-211`）——對應 arneb 因 broadcast 會給錯結果而整個停用；Spark 證明 broadcast 是可控的（明確上限 + AQE 執行期再決定），不必因噎廢食。與 12.1 互補：`EnsureRequirements` 在靜態層插、AQE `CoalesceShufflePartitions` 在執行期修。

### 12.5 【中】rule-based optimizer 的不變式紀律 + 規劃可觀測性

每條 rule「無效必回傳原樹」（Catalyst 的 `Rule.apply` 契約 = Trino 的 `Optional.empty()`、arneb pushdown 的 None）；fixed-point 用 `fastEquals` 偵測收斂 + `maxIterations` 兜底（對應 Trino `OPTIMIZER_TIMEOUT`）；**Once batch 的 idempotence 檢查**（`checkBatchIdempotence`，`RuleExecutor.scala:192`）能在 CI 就抓出「跑兩次結果不同」的 plan-rewrite bug，比事後在 SF30 才發現 silent 行為差異便宜太多——arneb 的 `decorrelated_agg_to_window` 一類改寫尤其該加此測試。`stats` 與 `size-only` 兩條路分流（`cboEnabled ? BasicStatsPlanVisitor : SizeInBytesOnlyStatsPlanVisitor`），缺統計就 fallback 純 size，避免拿假基數亂排。另外 `recordRuleInvocation`（記每條 rule 是否真改了樹、耗時，`RuleExecutor.scala:281`）是 arneb 規劃期缺的可觀測性——`QueryPlanningTracker` 是現成範本。

### 12.6 【中】operator 內依記憶體狀態切換 in-memory / spillable，且 hash 結構走緊湊 bytes

`HashAggregateExec` 用 `UnsafeFixedWidthAggregationMap` 包 `BytesToBytesMap`（key+value 連續、head+next 指標串接，且本身是 `MemoryConsumer` 可 spill）——這正驗證了 arneb 已完成的 `JoinHashMap` flatten（hashbrown `HashMap<K, Vec<...>>` ~47B/key 改成 head+next 扁平陣列 ~12B/row）走在工業驗證的路上。下一步：讓 arneb 的 `JoinHashMap` 像 `BytesToBytesMap` 一樣**進 MemoryPool 計帳**（而非 untracked anon），並讓 HashAgg 有對等的 in-memory / spillable 切換。

### 12.7 【中】Analyzer 階段就把名稱解析定案（穩定 ExprId）

Spark 在 analyzer 用 `resolved` 旗標 + 穩定 `ExprId` 把每個 column reference 解析到唯一目標（`ResolveReferences` 靠 `DeduplicateRelations` 先給每個 relation 唯一 `ExprId` 再解析 self-join 欄位歧義，`Analyzer.scala:1492,1509-1513`）。這對應 arneb join_reorder 對 `has_duplicate_leaf_column_names` 直接 bail 的 q08 痛點——根因解法是 leaf-origin / 唯一 id 追蹤，而非在規劃期繞開（與 Trino `Scope`/`Symbol`、`common.md` §1.1「不再有歧義的強型別 IR」同源）。`CheckAnalysis` 的「掃整棵樹找殘留 unresolved 旗標」是極簡又強健的不變量檢查——把「是否解析完成」做成節點本身的不變量，而非外部帳本，就沒有「帳本與樹不同步」的 bug；arneb 可借鏡把跨階段不變量檢查做成固定關卡 fail-loud。

### 12.8 已對齊、無需改的部分

- **欄式向量化執行表示** → arneb 全程 Arrow `RecordBatch`，已對齊 `ColumnarBatch`/`Page`/`DataChunk`/`Block` 同構（`common.md` §2 鐵律）；且 arneb 跳過了 Spark 為相容歷史 row 引擎而背的 row↔columnar 來回轉換（`ColumnarToRowExec`）成本——**不需引入 row 軸、不需 whole-stage codegen**（arrow-rs compute kernel 已是 SIMD 友善緊湊迴圈，引入 JVM 式執行期 codegen 對 Rust 是巨大複雜度且收益不明）。
- **storage/compute 分離 + 資料來源抽象** → arneb 的 `connectors`/`catalog`/`DataSource` trait 已對齊 DSv2；pushdown 回傳 None / 原樣的契約要守住，pushdown 規則須冪等。
- **split 由 connector 切、引擎只分派** → arneb 的 per-file row-range split 已對齊 `InputPartition`「切分由 connector 決定」；可再借鏡「切分描述（送 worker，要序列化）vs 讀取器（worker 端建，不送）」的分界。
- **fragment 邊界 = remote exchange** → arneb fragmenter 已對齊 Spark「在 `ShuffleDependency` 切 stage」。
- **partial/final aggregate 切分** → arneb 已有；DSv2 `supportCompletePushDown=false → 上層再 GROUP BY` 的模型可作為聯邦聚合下推的標準介面（`common.md` §4 可序列化中間狀態）。
- **節點間 shuffle 用 Arrow Flight（近零拷貝）** → 相對 Spark 自訂序列化 + 落地檔的 CPU/IO 開銷，arneb 的 Flight（gRPC/HTTP2、原生二進位 RecordBatch）是天然優勢；唯一要補的是「可選落地以換重試」（12.2），而非放棄 Flight。

---

## 驗證方法與來源

- **核實基準**：本文件所有 `相對路徑:行號` 引用，均對照本地 checkout 的 `apache/spark`，commit **`072994d33c042ed60f28af1c11cc2c4584162638`**（short **`072994d3`**，dated **2026-06-14**，以 `git -C /Users/bochengyang/formosa-ventures/repos/spark rev-parse HEAD` 與 `show -s --format=%ci HEAD` 取得）。路徑相對 repo root `/Users/bochengyang/formosa-ventures/repos/spark`（例如 `sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala:392`）。
- **引用慣例**：技術名詞 / class 名 / package 路徑 / 方法名 / 設定鍵一律保留英文原文；行號為本次以 `grep -n` / `Read` 直接定位的實際行號。Spark 持續演進，若日後 checkout 版本變動，以實際程式碼為準。各章末附「→ 對 arneb 的啟發」，與 `common.md`（四引擎共通哲學綱領）的六條鐵律呼應、不衝突。
- **本文核心校正（對齊最常見誤解）**：Spark 的**預設** operator 間執行單位是 **row-at-a-time `InternalRow` + whole-stage codegen**（`WholeStageCodegenExec.scala:616-617` 原文「compiles a subtree of plans … into single Java function」），**不是**向量化批次；向量化 columnar（`ColumnVector`/`ColumnarBatch`/`VectorizedParquetRecordReader`）**僅用於 scan 讀取、columnar cache、Arrow/Pandas UDF**，由 `FileSourceScanExec.supportsColumnar`（`DataSourceScanExec.scala:704-711`）+ `ColumnarToRowExec`（`Columnar.scala:67`）佐證。請勿把 Spark 描述成「預設向量化批次引擎」。
- **仍無法 100% 確認的點（誠實標註）**：以下逐項為本次核實過程中、未能逐行目視或屬合理歸納 / 跨引擎對照訓練知識的點，引用時建議覆核。

  跨引擎對照 / arneb 背景（非本次 Spark checkout 核實）：
  1. §3.5 / §5.3 / §7.6 提到 Trino「只有 dynamic filtering、沒有拿執行期統計回頭改物理計畫」屬跨引擎對照的既有 `trino.md` 結論與訓練知識，非本次 Spark checkout 核實。§10.2 表格中 Trino 的 `TimeSharingTaskExecutor` 1 秒 `SPLIT_RUN_QUANTA`、`MultilevelSplitQueue` 等描述同樣引自既有 `trino.md`，僅作對照。
  2. arneb 的全部痛點（q08 builds 90M / `partition_count` 寫死 / broadcast 被停用 / exchange silent-truncate / `must_drain` / `JoinHashMap` flatten / hashbrown anon / tokio permit deadlock / Q05/Q09 OOM / grace spill「自己看自己」/「admission fail-fast」）來自任務提供的 arneb 背景脈絡，非本人核實的 arneb 原始碼。§8 spill 啟發中「grace build 自己估超了才落地」「admission 時 fail-fast」均為對 arneb 行為的推論。
  3. §1.3 / §6.4 ColumnVector.java / ColumnarBatch.java / VectorizedParquetRecordReader.java 已逐行 Read 確認關鍵欄位與方法，但「向量化僅用於 scan」的每一處實作邊界未全部追蹤；該結論主要由 `FileSourceScanExec.supportsColumnar` 與 `ColumnarToRowExec` 佐證。

  Parser / Analyzer（§4）：
  4. 所有行號皆於本地 checkout 以 `grep -n` / `Read` 實際觀察取得；`RuleExecutor.execute` 入口宣告引用為 `:215`（grep 命中），while 迴圈體 `:237-:325` 已 Read 核實，`:215` 方法簽章行未逐字目視（高度可信）。
  5. `SqlBaseParser.g4:919` 的 `fromClause` 來自 grep 命中，未對該規則本體逐字 Read（`querySpecification`/`selectClause` 已 Read 確認）。
  6. `QueryExecution.scala` 的 `analyzed` lazy val（`:211`）與 `executeAndCheck` 呼叫（`:200`）來自 grep 命中，未對該段完整 Read。
  7. `AnsiTypeCoercion` 較 `TypeCoercion` 嚴格、不做寬鬆 string↔numeric 推升——為對 `typeCoercionRules` 清單與 `findTightestCommonType` 差異的合理歸納，未逐條 Read `AnsiTypeCoercion` 全部子 rule 比對；屬推測成分。
  8. `AstBuilder.visitTableName:2636` 呼叫 `createUnresolvedRelation` 已 grep 確認；`createUnresolvedRelation` 的完整實作（將 multipart identifier 包成 `UnresolvedRelation` 或 `PlanWithUnresolvedIdentifier`）未逐行 Read，描述為合理摘要。
  9. `HybridAnalyzer.fromLegacyAnalyzer`（`Analyzer.scala:335`）顯示 Spark 已有 single-pass analyzer 與 legacy fixed-point analyzer 並存的 Hybrid 機制；本章聚焦 legacy（RuleExecutor fixed-point）路徑，single-pass 是否為預設、兩者如何切換未核實。
  10. §2.2 表格與 §4.2 對 `Analyzer.scala:506` 的 batches 僅完整觀察到開頭幾條規則（`ResolveRelations`/`ResolveReferences`/`ResolveFunctions`），未完整列舉所有 analysis batch，部分描述為概括。

  Optimizer / CBO / AQE（§5、§7.6）：
  11. `FilterEstimation.scala:49` 的「保守給 100% 選擇率」依原始碼註解推得，未逐行核實該分支的完整 fall-through 邏輯（引用的是註解所在行）。
  12. `CoalesceShufflePartitions` 的 `advisoryPartitionSize` 預設 64MB 與 min partition size 預設 1MB 取自原始碼註解（`CoalesceShufflePartitions.scala:52`、`:56`），未另開 `SQLConf.scala` 逐一核實 `ADVISORY_PARTITION_SIZE_IN_BYTES` 的 `createWithDefault` 確切數值。
  13. `OptimizeSkewedJoin` 受 `spark.sql.adaptive.skewJoin.enabled` 控制係讀 `SKEW_JOIN_ENABLED` 的 getConf（`:258`），未核實該 SQLConf key 字串與預設值（推測為 true）。
  14. AQE 預設 true 係 `ADAPTIVE_EXECUTION_ENABLED.createWithDefault(true)`（`SQLConf.scala:1050`）已核實；「Spark 3.2 起預設開啟」這類版本演進敘述為訓練知識，本文未斷言版本號、僅以當前 commit 的預設值為準。
  15. `DynamicJoinSelection` 的 `nonEmptyPartitionRatioForBroadcastJoin` / `ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD` 為 conf 取值（`:44`、`:49`），未核實其 SQLConf 預設值。`betterThan` 的 `joinReorderCardWeight`（`SQLConf.scala:4147` 起的 `JOIN_REORDER_CARD_WEIGHT`）確切數字未核實，本文僅描述為「加權幾何平均」未斷言權重數值。
  16. §7.6 提到 AQE「把原本 shuffle join 降級轉成 broadcast join」的具體規則類別未逐一定位 file:line（僅核實 `CoalesceShufflePartitions:142`、`OptimizeSkewedJoin:132`、`OptimizeSkewInRebalancePartitions:141` 三條在規則清單中的行號）；「shuffle→broadcast 降級」屬訓練知識中 AQE 的 `DemoteBroadcastHashJoin` / 動態 join 策略，未在本次 checkout 定位到對應規則檔的 file:line，建議補核 `sql/core/.../adaptive/` 下相關規則檔。

  執行引擎 / Tungsten（§6）：
  17. §6.2 `InputRDDCodegen.doProduce` 的 while-loop 程式碼字串引用自 `WholeStageCodegenExec.scala:494-501`，已 Read 確認；但生成程式碼的具體執行語意（`shouldStop` / `limitNotReached` 的完整分支）未逐行追完。
  18. §6.3 `UnsafeRow` 變長欄位「`(offset<<32 | len)` 合成 long」依 `setBinary` 寫入 `(cursor<<32)|len`（`:317`）與 javadoc `:56-59` 推得；個別型別（如 DecimalType、CalendarInterval）的精確寫入路徑未逐一核實。
  19. §6.4「是否走向量化 reader 由 `spark.sql.parquet.enableVectorizedReader` 控制」——已確認 `SQLConf.scala:1660` 存在 `PARQUET_VECTORIZED_READER_ENABLED`，但其在 `ParquetFileFormat`/`FileSourceScanExec` 決策點的完整觸發條件（如 schema 是否全 atomic 才啟用向量化）未逐檔核實。
  20. `BytesToBytesMap` 在 join（如 `ShuffledHashJoinExec`）的使用未核實——本文以 HashAggregate 的 `UnsafeFixedWidthAggregationMap` 路徑為主例；join 端是否直接用 `BytesToBytesMap` 還是 `HashedRelation`/`LongHashedRelation` 未核實，未對 join 端斷言具體類別。

  分散式 / shuffle（§7）：
  21. §7.1 末段 `DAGScheduler.scala:1853` 的「we only handle one task completion event at a time so we don't need to worry about locking」係出現在 `mergeTaskAccumulables`/`updateAccumulators` 一帶的方法註解（行號約 1847–1858 區間），`:1853` 為實際觀察行號但建議整段覆核。
  22. §7.5 `BroadcastHashJoinExec` 路徑 `sql/core/.../execution/joins/BroadcastHashJoinExec.scala`（grep 命中且 Read 於 `:67-75`）；`ShuffledHashJoinExec` 僅核實 case class 宣告與 `buildHashedRelation`，其 `requiredChildDistribution` 係繼承自 `ShuffledJoin`（已於 `ShuffledJoin.scala:57-67` 核實），未單獨於 `ShuffledHashJoinExec.scala` 內再宣告——表格將其與 `SortMergeJoinExec` 並列為 `ShuffledJoin` 為推論性歸納，建議覆核 `ShuffledHashJoinExec` 確實 `extends ShuffledJoin`。
  23. §7.2「分區數大時用 `HighlyCompressedMapStatus`（門檻見 `MapStatus.scala:70-88`）」的門檻常數（`spark.shuffle.minNumPartitionsToHighlyCompress` / 預設 2000）未逐字核實，僅核實 `object MapStatus` 的 apply 在 `:86`/`:88` 的分支。
  24. §7.2 / §7.3 的 `SortShuffleManager` / 三種 writer / `ShuffleBlockFetcherIterator` 節流以 grep/find 確認符號與檔案存在、並 Read 關鍵方法，但未逐行讀完 map 端寫檔與 reduce 端 fetch 的全部分支；「disk-materialized」結論主要由 `DAGScheduler` class doc（`:87-91`、`:106-110`）與 `IndexShuffleBlockResolver` class doc 佐證。
  25. §3.6 `SparkContext.runJob` 有多個多載（`SparkContext.scala:2481` 起一連串），本文引用 partitions+resultHandler 版本（`:2481→:2496`）；action（collect/count）究竟走哪一個多載未逐一追蹤，但都匯流到 `dagScheduler.runJob`。
  26. §7.7 `newCachedThreadPool` 的 `.asInstanceOf[ThreadPoolExecutor]` 在 `:313`；`TaskRunner` 建構子列於 `:687-691`（未展開全部行），建議覆核 `:563` `threadPool.execute(tr)` 的 `tr` 確為 `TaskRunner` 實例之上下文。

  記憶體 / 排程（§8、§10）：
  27. `UnifiedMemoryManager` javadoc 的 `spark.memory.fraction=0.6`、`storageFraction=0.5` 已從 javadoc（`:40-41`）與 config 定義（`:485`/`:495`）核實存在，但未逐一打開 `.createWithDefault` 確認數值常數；config 的 `.doc` 與 javadoc 一致，數值以 javadoc 為準。
  28. §10.1 `TaskSetManager` 的 `localityWait` 機制（在近的 level 短暫等待再降級）已見 `localityWaits` 欄位（`:268`）與 `dequeueTaskHelper` 分支（`:401` 等），但未逐行核實 `resourceOffer`（`:461`）內 localityWait 計時/降級的完整控制流，描述為機制層級概述。
  29. §10.2 `TaskRunner.run` 內 `task.run(...)` 位於 `:888`，但未逐行追完 `TaskRunner.run()` 從 `:806` 到 task 完成釋放 thread 的全部分支（deserialize / 結果回傳 / 例外處理）；「一條 thread 跑到完成」結論建立在 `threadPool.execute(tr)`（`:563`）+ Runnable 語意 + `task.run` 同步呼叫之上。
