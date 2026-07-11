# Trino 分散式查詢引擎架構解析（經本地原始碼核實版）— 給 arneb 自建引擎工程師

本文件深入解析 Trino（前身 PrestoSQL）的核心架構、設計權衡與關鍵原始碼模組，並針對正在以 Rust 構建分散式 SQL 查詢引擎 `arneb`（基於 `tokio` + `arrow-rs`）的工程師，提供具體的架構借鏡與設計決策指引。

本文每一項關鍵事實都已對照本地 checkout 的 `trinodb/trino`（`master`，commit `82c47e2be10`）原始碼，並附上 `相對路徑:行號`（相對 repo root）。2026-06-05 另以 latest `origin/master` commit `7aa62d08f3ef` 回查：核心架構與主要符號仍可對上，但本文 file:line 仍以原始核實 commit 為準，latest 行號可能已漂移。技術名詞、類別名、套件路徑一律保留英文。各章末附「→ 對 arneb 的啟發」。詳細的核實方法、引用慣例與仍存疑點見文末「驗證方法與來源」一節。

---

## 1. 專案定位與設計哲學

Trino 的定位是**高效能、低延遲的分散式 MPP（Massively Parallel Processing）SQL 查詢引擎**，專注於「對異質資料來源做聯邦式（federated）互動查詢」，而非自帶儲存的資料庫。核心設計哲學可從原始碼的組織方式直接讀出：

1. **儲存與計算分離（storage/compute separation）**：引擎本身不擁有業務資料。所有資料存取都透過 `core/trino-spi`（Service Provider Interface）抽象出去，由各 connector 實作。這是 Trino 最重要的架構決策，使計算節點可任意水平擴展。
2. **向量化、頁式（columnar / page-oriented）執行**：所有 operator 之間傳遞的單位是 `io.trino.spi.Page`（一批欄式資料），而非單列（row），中間結果儘量保留在記憶體並以串流方式在節點間傳遞，以達次秒級到秒級的互動回應。
3. **Push-based pipeline 執行模型**：由 `io.trino.operator.Driver` 主動把資料從上游 operator「取出再推入」下游 operator，搭配 `ListenableFuture` 做非阻塞讓出（yield）。
4. **以規則為主、成本為輔的最佳化**：`io.trino.sql.planner.iterative.IterativeOptimizer` 提供 Cascades 風格的 rule-based 框架，`io.trino.cost.*` 提供統計與成本估算供 CBO 規則決策。
5. **協調者 / 工作者（coordinator / worker）架構**：單一 coordinator 負責 parse/analyze/plan/schedule，多個 worker 負責執行 plan fragment。

從 `core/trino-main/src/main/java/io/trino` 的頂層 package 列表即可看出職責切分：

```
annotation  block   client   connector  cost     dispatcher  event
eventlistener  exchange  execution  failuredetector  json  likematcher
memory  metadata  node  operator  security  server  simd  spiller
split  sql  testing  tracing  transaction  type  util
```

### 設計權衡與已知缺點

- **記憶體易爆（OOM vulnerability）**：高度依賴記憶體，執行大表 Join 或高基數 Group By 時，若超出配額會殺掉查詢（拋 `EXCEEDED_MEMORY_LIMIT`）。雖然有 `spill-to-disk`，但會顯著降低效能。
- **互動式模式不容錯**：預設的 pipelined 排程下，任一 task 失敗整個 query 失敗。雖然新增了基於 exchange spooling 的 Fault-Tolerant Execution（FTE），在極大規模批次任務上其穩定性仍弱於 Spark。

> → 對 arneb 的啟發：Trino 把「資料存取」隔離在 `trino-spi` 一個獨立 crate-等價物中，與引擎核心完全解耦。arneb 已有 `connectors` / `catalog` crate 的對應切分，值得繼續強化的是：**SPI 應該是穩定、版本化的合約**，引擎核心永遠不直接 import connector 實作。`block` 與 `simd` package 的存在也提示欄式記憶體格式與 SIMD 是一等公民——arneb 採用 Apache Arrow 已天然具備此優勢。

---

## 2. 整體架構與核心組件

Trino 採典型的單一 Coordinator、多 Worker 架構：

```
                       ┌──────────────────────── COORDINATOR ─────────────────────────┐
   SQL (HTTP/JSON)     │                                                               │
   ───────────────────▶│  SqlParser ─▶ Analyzer ─▶ LogicalPlanner ─▶ IterativeOptimizer│
   /v1/statement       │  (trino-parser) (sql.analyzer)(sql.planner)  (CBO + rules)    │
                       │                              │                                │
                       │                              ▼                                │
                       │                       PlanFragmenter ──▶ SubPlan 樹           │
                       │                              │                                │
                       │              SqlQueryExecution / SqlQueryScheduler            │
                       │              NodeScheduler / SplitManager                     │
                       └───────────────┬──────────────────────────┬───────────────────┘
                                       │ HTTP task dispatch        │ (Heartbeat / Discovery)
                 ┌─────────────────────▼──────┐      ┌─────────────▼──────────────┐
                 │        WORKER A             │      │        WORKER B            │
                 │  SqlTaskExecution           │      │  SqlTaskExecution          │
                 │   └ TaskExecutor (中央池)   │      │   └ TaskExecutor (中央池)  │
                 │      └ Driver ── Operator   │◀────▶│      └ Driver ── Operator  │
                 │         (HashAgg/Join/...)  │ Exch │         (...)              │
                 │  LocalMemoryManager/Spiller │ ange │  LocalMemoryManager/Spill  │
                 │  ConnectorPageSource        │      │  ConnectorPageSource       │
                 └──────────────┬──────────────┘      └──────────────┬─────────────┘
                                ▼  DirectExchangeClient (HTTP pull pages)
                       [ Cloud Object Storage / External DBs ]
```

核心組件職責對照（package / class 皆已核實存在）：

| 組件 | Package / Class | 職責 |
|---|---|---|
| Parser | `io.trino.sql.parser.SqlParser` + `AstBuilder`（ANTLR4 `SqlBase.g4`）| SQL 字串 → AST（`Statement`）|
| Analyzer | `io.trino.sql.analyzer.Analyzer` / `StatementAnalyzer` / `Analysis` | 語意分析、名稱解析、型別檢查 |
| Logical Planner | `io.trino.sql.planner.LogicalPlanner` / `QueryPlanner` / `RelationPlanner` | AST → `PlanNode` 樹 |
| Optimizer | `io.trino.sql.planner.iterative.IterativeOptimizer` + `io.trino.cost.*` | Rule-based + Cost-based 改寫 |
| Fragmenter | `io.trino.sql.planner.PlanFragmenter` | Plan → `SubPlan`（PlanFragment 樹）|
| Query 編排 | `io.trino.execution.SqlQueryExecution` | 串接 analyze→plan→fragment→schedule |
| Scheduler | `io.trino.execution.scheduler.*`（`SqlQueryScheduler`、`NodeScheduler`）| stage/split/task 排程 |
| Task 執行 | `io.trino.execution.SqlTaskExecution` + `io.trino.execution.executor.TaskExecutor` | worker 端把 fragment 跑成 Driver |
| 執行單元 | `io.trino.operator.Driver` / `Operator` | push-based pipeline |
| 資料單位 | `io.trino.spi.Page` / `io.trino.spi.block.Block` | 欄式批次資料 |
| Exchange | `io.trino.operator.DirectExchangeClient` + `ExchangeOperator` | 跨 task 資料搬運 |
| 記憶體 | `io.trino.memory.*`（`MemoryPool`、`QueryContext`、`LowMemoryKiller`）| 記憶體配額追蹤 |
| Spill | `io.trino.spiller.*` | 落地磁碟 |
| SPI | `io.trino.spi.connector.*` | 資料來源抽象 |

> **核實要點（修正兩版草稿）**：`TaskExecutor` 介面實際位於 `core/trino-main/src/main/java/io/trino/execution/executor/TaskExecutor.java:27`，**並非** agy 草稿所稱的 `io.trino.operator`。它有兩個實作：時間片共享的 `TimeSharingTaskExecutor`（`io/trino/execution/executor/timesharing/TimeSharingTaskExecutor.java:85`）與每 driver 一執行緒的 `ThreadPerDriverTaskExecutor`（`io/trino/execution/executor/dedicated/ThreadPerDriverTaskExecutor.java`）。Discovery Service 通常內嵌於 coordinator，worker 定期心跳註冊。

---

## 3. 查詢生命週期：從 SQL 字串到結果

`io.trino.execution.SqlQueryExecution` 是 coordinator 端的總指揮，其 `start()` 驅動的流程為：

```
SQL string  (Client HTTP POST 至 /v1/statement;
             由 io.trino.server.protocol.{ExecutingStatementResource,Query} 接收)
  │
  ├─(1) SqlParser.createStatement()            → AST (Statement)
  │
  ├─(2) Analyzer.analyze()                      → Analysis (tables, types, lineage)
  │
  ├─(3) LogicalPlanner.plan() [doPlanQuery()]   → Plan (PlanNode 樹, 已套用 PlanOptimizers)
  │        └ IterativeOptimizer 等 optimizer 串
  │
  ├─(4) registerDynamicFilteringQuery()         → 註冊 dynamic filter
  │
  ├─(5) PlanFragmenter.createSubPlans()          → SubPlan 樹 (PlanFragment)
  │
  ├─(6) 選擇 QueryScheduler                      → PipelinedQueryScheduler 或
  │        EventDrivenFaultTolerantQueryScheduler (依 retry policy)
  │
  └─(7) scheduler.start()                        → 分派 task 到 worker，啟動執行
           狀態機: PLANNING → STARTING → RUNNING → FINISHED
```

執行各 Stage 的 Task 在 worker 端由 `LocalExecutionPlanner` 把 fragment 編譯成 pipeline，實例化為 `Driver` 後交給 `TaskExecutor` 排程；下游 task 透過 `DirectExchangeClient` 從上游 worker 的 HTTP 端點拉取 `Page`。最頂層 Stage 將結果寫入 coordinator 的 output buffer，Client 不斷 GET 分批拉取，直到 `QueryState` 轉 `FINISHED`。

> Trino 用 `AtomicReference` 持有 scheduler，以支援併發的 cancel / status 查詢，並把「query 狀態機」與「scheduler 選擇」明確分離。

> → 對 arneb 的啟發：注意 Trino **依 retry policy 切換不同 scheduler 實作**（pipelined vs 容錯）的策略模式——把「執行語意」做成可插拔策略，而非寫死。arneb 的 `crates/scheduler/QueryTracker` 已是狀態機；可借鏡此「策略模式 + AtomicReference 併發取消」的拆分。

---

## 4. SQL Parser 與 Analyzer / 語意分析

### 4.1 Parser

- **技術選型**：ANTLR4 文法檔 `SqlBase.g4`。**核實修正**：文法檔現位於 `core/trino-grammar/src/main/antlr4/io/trino/grammar/sql/SqlBase.g4`（package `io.trino.grammar.sql`），而非舊草稿所稱的 `trino-parser` 內。
- **進入點**：`core/trino-parser/src/main/java/io/trino/sql/parser/SqlParser.java`，`createStatement()` 將 SQL 字串解析為 `io.trino.sql.tree.Statement`。
- `io.trino.sql.parser.AstBuilder` 走訪 ANTLR parse tree，建構 Trino 自家 AST 節點。`ErrorHandler` / `ParsingException` 負責錯誤回報。
- **設計重點**：把「文法定義（ANTLR）」與「AST 表示」解耦，AST 不依賴 ANTLR 的節點型別。

### 4.2 Analyzer（`io.trino.sql.analyzer`）

| Class | 職責 |
|---|---|
| `Analyzer` | 總協調者，驅動整個語意分析流程 |
| `StatementAnalyzer` | 處理各種 SQL statement，做語意驗證與轉換 |
| `Analysis` | 分析結果的容器（型別、解析後 metadata、column lineage、update type）|
| `ExpressionAnalyzer` | 運算式型別檢查與語意驗證、隱式型別轉換（Implicit Type Coercion）|
| `Scope` | 名稱解析與變數可見範圍管理 |
| `AggregationAnalyzer` | 驗證聚合函式與 GROUP BY 語意 |

- **型別推導與轉換**：Trino 內建強型別系統（`io.trino.spi.type.Type`）。`StatementAnalyzer` 遍歷樹時做隱式轉換，例如 `INT` 與 `DOUBLE` 比較會自動插入 `Cast` 把 `INT` 轉 `DOUBLE`。
- **Metadata 交互**：頻繁呼叫 `io.trino.metadata.Metadata` 介面（橋接各 connector），取得表的中繼資料。
- **產物**：`Analysis` 物件記錄每個 Expression 的 resolved type、選中的 table handle、join 關聯列等，是 `LogicalPlanner` 的基礎。

> → 對 arneb 的啟發：Trino 把 **`Scope`（名稱解析作用域）做成一級概念**，而非散在 planner 各處。對於 CTE、子查詢、相關子查詢的正確名稱解析，這種顯式 Scope 樹是關鍵。arneb 若在巢狀子查詢遇到欄位解析含混（記憶中的 RowConverter index mismatch 一類問題），值得參考 Trino 在 analyzer 階段就把每個 column reference 解析到唯一目標、產出富 metadata 的 `Analysis`，而非延後到 plan rewrite 才解析。

---

## 5. 查詢規劃與最佳化

```
              Analysis
                 │
                 ▼
       ┌───────────────────┐
       │  LogicalPlanner   │ ───> 產生 Initial PlanNode 樹
       └───────────────────┘
                 │
                 ▼
       ┌───────────────────┐      ┌───────────────────┐
       │ IterativeOptimizer│<────>│  StatsCalculator  │ Cardinality / Nulls / NDV / Min-Max
       │  (Memo + Rule)    │      └───────────────────┘
       └───────────────────┘      ┌───────────────────┐
                 │           <────>│   CostCalculator  │ CPU / Memory / Network
                 ▼                 └───────────────────┘
       ┌───────────────────┐
       │   PlanFragmenter  │ ───> 切分 PlanFragments (Stages)
       └───────────────────┘
```

### 5.1 Logical / Physical Plan

- `io.trino.sql.planner.LogicalPlanner` 把 `Analysis` 轉成 `PlanNode` 樹；`RelationPlanner` / `QueryPlanner` / `PlanBuilder` 逐步建構。
- `io.trino.sql.planner.Symbol` 是 plan 中的符號參照，`SymbolAllocator` 配發唯一 Symbol。
- 常見 `PlanNode`：`TableScanNode`、`ProjectNode`、`FilterNode`、`JoinNode`、`AggregationNode`、`ExchangeNode`。
- **重要差異**：Trino 並沒有截然二分的「logical vs physical 兩種樹」，而是**同一棵 `PlanNode` 樹，透過 optimizer rule 逐步加入 physical 屬性**（如 `ExchangeNode`、partitioning），與某些教科書式 logical/physical 分離設計不同。

### 5.2 IterativeOptimizer（rule-based, Cascades 風格）

實際讀 `core/trino-main/src/main/java/io/trino/sql/planner/iterative/IterativeOptimizer.java`：

- 以 **`Memo`** 結構記錄 plan 的各種替代方案（group / equivalence）：`Memo memo = new Memo(...)`（`IterativeOptimizer.java:115`）。
- 探索迴圈：`exploreGroup()`（`:151`）/ `exploreNode()`（`:171`）——`exploreNode` 對單一節點反覆套規則直到無新變更；`exploreGroup` 遞迴探索子節點（`:272`），子節點變動時重試父節點，達成串聯（cascading）最佳化。
- Rule 觸發：`ruleIndex.getCandidates(node)`（`:182`）取得候選 Rule；成功套用後 `memo.replace(group, ...)`（`:194`）替換 Memo group 內對應的節點。
- Pattern 比對：`transform()`（`:212`）先以 `rule.getPattern().capturedAs(nodeCapture)`（`:215`）做結構捕捉，再 `rule.apply()`。
- 逾時保護：`checkTimeoutNotExhausted()`（`:324` / `:379`），超時拋 `TrinoException`（status `OPTIMIZER_TIMEOUT`，import 於 `:60`），並報出耗時前幾名規則。
- 統計：`RuleStatsRecorder`（`:40`）、`PlanOptimizersStatsCollector` 記錄每條規則的呼叫次數、成功率、耗時。

優化分類：

- **RBO（Rule-Based）**：謂詞下推、投影剪枝、表達式簡化等。
- **CBO（Cost-Based）**：Join Reordering、Broadcast vs Partitioned Join 的物理選擇。

### 5.3 Cost-Based Optimizer 與統計（`io.trino.cost`）

| Class | 職責 |
|---|---|
| `StatsCalculator` | 對各 plan node 計算統計（row count、column stats）|
| `PlanNodeStatsEstimate` | 單一 plan node 的統計估計 |
| `SymbolStatsEstimate` | 每欄統計（null 比率、值域、distinct 數 NDV）|
| `CostCalculator` | 計算 plan node 執行成本，量化 CPU / Memory / Network |
| `StatsAndCosts` | 整棵 plan 的統計＋成本聚合容器 |
| `CachingStatsProvider` / `CachingCostProvider` | 快取避免重算 |
| `TableScanStatsRule` / `FilterStatsCalculator` / `JoinStatsRule` / `AggregationStatsRule` | 各 operator 的統計推導規則 |

- **統計獲取**：`StatsCalculator` 透過 `ConnectorMetadata.getTableStatistics()` 取得葉節點基礎統計，**自底而上**逐層傳播。
- **基數估算**：`FilterStatsCalculator` 依謂詞選擇率推算 FilterNode 之後的行數。
- **Join Reordering**：表數較少時用 DP 列舉連通子圖的連接順序，避免無效 cross join；超過閥值（受 `optimizer.max-reordered-joins` 一類設定限制）時改用啟發式，防止規劃時間爆炸。

> **核實修正**：Cost 模型細節以 `io.trino.cost.CostCalculator` 一族為準；agy 草稿提到的「`io.trino.cost.Cost` 公式」「DPHyp 演算法」「`join-multi-clause-independence-factor`」屬訓練知識推測，未在本次核實範圍逐字確認，本文僅保留已確認的 class 名與「DP + 啟發式」這一層級的描述。

> → 對 arneb 的啟發：
> 1. **無限迴圈防護是 rule framework 的硬約束**（見第 9 章 pushdown 契約）——arneb 若採類似 rule pass，務必嚴守「無效就回傳原樣 / None」，並加 optimizer 逾時保護（對應 `OPTIMIZER_TIMEOUT`）。
> 2. arneb 已有 Selinger DP join reorder + partition-aware cost、PredicatePushdown、NDV 估算——正對應 `JoinStatsRule` / `FilterStatsCalculator`。可借鏡 Trino 把 **stats 與 cost 分成 `StatsCalculator` / `CostCalculator` 兩層**，並用 `CachingStatsProvider` 快取；重複 explore 時明顯省時。
> 3. Trino 不搞「兩棵樹」而是「一棵樹漸進物化 physical 屬性」，可降低 arneb logical→physical 轉換的對應複雜度。

---

## 6. 執行引擎模型（向量化、push-based、Driver / Operator）

### 6.1 資料單位：Page / Block

`io.trino.spi.Page` 是執行期的資料單位（核實 `core/trino-spi/src/main/java/io/trino/spi/Page.java`）：

```
                    ┌───────────────── Page ─────────────────┐
                    │  [Block 0]     [Block 1]     [Block 2] │
                    │   (INT)        (VARCHAR)      (BIGINT) │
                    ├────────────┬──────────────┬────────────┤
                    │    12      │    "Apple"   │   100293   │  ← position 0
                    │    45      │    "Banana"  │   492812   │  ← position 1
                    └────────────┴──────────────┴────────────┘
```

- 欄位：`private final Block[] blocks`（`Page.java:50`）、`private final int positionCount`（`Page.java:51`，列數）、`private volatile long sizeInBytes = -1`（`Page.java:52`，快取大小）。
- 方法：`getBlock(int channel)`（`:124`）、`getPositionCount()`（`:93`）、`getChannelCount()`（`:88`）、`getSizeInBytes()`（`:97`）、`getRetainedSizeInBytes()`（`:115`）。
- **零拷貝邏輯切片**：`getRegion(positionOffset, length)`（`:142`）遍歷各 Block 呼叫 `block.getRegion(...)`，再以 `wrapBlocksWithoutCopy(...)`（`:45`）包裝成新 `Page`，不複製實體資料。
- **記憶體緊湊化**：`compact()`（`:172`）回收 Block 底層冗餘記憶體；對 `DictionaryBlock` 會呼叫 `DictionaryBlock.compactRelatedBlocks`（`:189`）合併關聯字典。

`io.trino.spi.block.Block` 代表單一欄的一組資料，常見實作：

- `IntArrayBlock` / `LongArrayBlock`：扁平原始型別陣列，記憶體連續，cache 友善。
- `VariableWidthBlock`（變長字串，底層 `Slice` 封裝 `byte[]`）。
- `DictionaryBlock`：字典編碼，只存原始 Block 引用 + `int[]` 索引，實現零拷貝過濾與傳遞。
- `RunLengthEncodedBlock`（RLE）：整欄常數時只存單一值。

### 6.2 Operator 框架（`io.trino.operator`）

- `Operator` 是所有 operator 的基底介面，關鍵方法：`addInput(Page)`（餵入）、`getOutput()`（取出 Page）、`finish()`（告知無更多輸入）、`isFinished()`、`isBlocked()`、`needsInput()`。
- `OperatorContext` / `OperatorFactory` / `OperatorStats` 提供執行情境、工廠與統計。
- **重要差異**：Trino 不使用傳統 Volcano 的 pull-based `next()` 迭代器，而是 **push-based**——上游 `getOutput()` 出 Page，下游 `addInput()` 收。

### 6.3 Driver：push-based pipeline 的引擎

實際讀 `core/trino-main/src/main/java/io/trino/operator/Driver.java`：

- **時間片限制**：`process(Duration maxRuntime, int maxIterations)`（`Driver.java:283`）取得時間／迭代上限；於排程開始前以 `driverContext.getYieldSignal().setWithDelay(maxRuntimeInNanos, ...)`（`:301`）設定逾時觸發器；迴圈中 `if (System.nanoTime() - start >= maxRuntimeInNanos || iterations >= maxIterations) break`（`:311`）即主動讓出。亦提供 `processForDuration()`（`:267`）/ `processForNumberOfIterations()`（`:272`）。
- **非阻塞 / yield 模型**：operator 阻塞時以 `firstFinishedFuture(blockedFutures)`（`:465` / `:656`）組合多個 future，註冊監聽器；只要任一 operator 解除阻塞**或 memory pool 發出 revoke 請求**即完成 blocked future，通知 `TaskExecutor` 重新排程。`process()` 在 future 未完成時直接 return 該 future，讓出執行緒。
- **非可重入排他鎖**：自訂 `DriverLock`（內含 JVM `ReentrantLock`，`Driver.java:769`）。`acquire` 時 `checkState(!lock.isHeldByCurrentThread(), "Lock is not reentrant")`（`:797`、`:808`）明確要求不可重入。
- **中斷控制**：task 結束時 `interruptCurrentOwner()`（`:178`、`:848`）會先檢查 `currentOwnerInterruptionAllowed`（`:785`、`:850`），避免在不可中斷的關鍵操作中誤中斷。
- Driver 也管理 **memory revocation**（配合 spill）。

> **關鍵核實結論（修正 agy 草稿）**：Driver / split runner **不持有任何 `Semaphore` 或 permit 跑完整個生命週期**（Driver.java 全檔無 `Semaphore` / `acquire()` / permit）。它每次只跑「一個時間片」就讓出。實際的時間片量子定義在 `core/trino-main/src/main/java/io/trino/execution/executor/timesharing/PrioritizedSplitRunner.java:49`：`public static final Duration SPLIT_RUN_QUANTA = new Duration(1, TimeUnit.SECONDS);`，並由 `split.processFor(SPLIT_RUN_QUANTA)`（`:187`）驅動 Driver。agy 草稿所稱「250 毫秒」**有誤**，正確為 **1 秒**量子（並同時受 iteration 上限約束）。

### 6.4 Operator 實作範例：HashAggregationOperator

實際讀 `core/trino-main/src/main/java/io/trino/operator/HashAggregationOperator.java`，`addInput()` 中依 step / 設定首次選用三種 builder（`HashAggregationOperator.java:365`–`410`）：

- `SkipAggregationBuilder`（partial agg 被 controller 停用時降級，`:370`）。
- `InMemoryHashAggregationBuilder`（partial 階段、未啟用 spill、或有不支援 spill 的聚合函數時，`:372`–`374`）。
- `SpillableHashAggregationBuilder`（其他常規狀況，支援記憶體感知 spill，`:395`）。

- builder 內以 `GroupByHash` 做分組、每個聚合一個 accumulator / `Aggregator`。
- **spill**：受 `spillEnabled`（`:253`/`:307`）與 `isSpillable()`（`:425`，需所有 `AggregatorFactory::isSpillable`）控制；以 `startMemoryRevoke()`（`:431`）/ `finishMemoryRevoke()` 管理回收，超門檻時由 `SpillerFactory` 建 spiller。
- **partial vs final**：`io.trino.sql.planner.plan.AggregationNode.Step` enum 區分（import 於 `:31`）；global agg 無輸入時 `getGlobalAggregationOutput()` 產生 identity 值（如 `COUNT=0`）。

> → 對 arneb 的啟發：
> 1. **push-based + ListenableFuture yield** 是 Trino 能在固定執行緒池上跑多個 query 的關鍵——operator 不擁有執行緒，Driver 是被 `TaskExecutor` 分時排程的。arneb 用 tokio async stream（`SendableRecordBatchStream`）本質上是 pull-based；arneb 記憶中多次踩到「**semaphore permit 持有整個 task 生命週期 vs stream back-pressure 不相容**」的 deadlock——這正是 pull-based async 模型與 Trino push-based 協作式排程的根本差異。Trino 的 Driver **每跑一個 1 秒量子就 yield 回 TaskExecutor 重排**，天然避免長期占用。arneb 要根治此類 deadlock，可考慮引入「協作式 time-slice yield」而非「permit 持有」式併發控制（記憶中的「Phase A：刪 `task_manager` semaphore」正是正確方向）。
> 2. **同一 operator 內依記憶體狀態切換 in-memory / spillable builder** 值得照抄——arneb 的 Grace HJ / SemiJoin spill 已朝此方向；HashAgg 也應有對等的 `SpillableHashAggregationBuilder`，且 partial/final 的 identity 邊界要明確（COUNT=0）。
> 3. Page = `Block[]` + positionCount，與 Arrow RecordBatch 幾乎同構——arneb 用 Arrow 已對齊，無需自造。

---

## 7. 分散式執行（coordinator/worker、Stage/Fragment、Exchange）

### 7.1 PlanFragmenter：在 REMOTE Exchange 邊界切分

實際讀 `core/trino-main/src/main/java/io/trino/sql/planner/PlanFragmenter.java`：

- 進入點 `createSubPlans()`（`PlanFragmenter.java:126`/`138`）：實例化內部 `Fragmenter` visitor（`:245`，繼承 `SimplePlanRewriter<FragmentProperties>`）走訪 plan 樹。
- **切分邊界 = REMOTE ExchangeNode**：`visitExchange` 遇到 `exchange.getScope() == REMOTE`（`ExchangeNode.Scope.REMOTE` import 於 `:88`，判斷於 `:510`）時，為每個 source 呼叫 `buildSubPlan(...)`（`:532`/`:556`）建子 fragment，並把 exchange 換成 **`RemoteSourceNode`**（`new RemoteSourceNode(...)`，`:547`；import 於 `:48`）——這就是 fragment 邊界。
- root fragment 由 `buildFragment(...)`（`:284`）產出。
- **stage 數量上限**：`sanityCheckFragmentedPlan(...)`（`:177`）呼叫 `subPlan.sanityCheck()`（`:179`），若 `fragmentCount > maxStageCount`（`:181`，取自 `getQueryMaxStageCount(session)`，`:172`）則拋例外。
- **partitioning 協調**：`PartitioningHandle` 封裝分佈語意（single-node、coordinator-only、hash partitioning）；`FragmentProperties.setDistribution()` 用 metadata 的 `getCommonPartitioning()` 協調衝突需求。`PartitioningHandleReassigner`（`:800`）在 fragment 推導出的 partitioning 與下游 scan 不相容時，呼叫 `metadata.applyPartitioning(...)`（`:832`）把 partitioning 下推到 TableScan。

```
   邏輯 plan (含 REMOTE ExchangeNode)
            │  PlanFragmenter.createSubPlans()
            ▼
   ┌─ SubPlan(Fragment 0, root, coordinator) ─┐
   │     RemoteSourceNode ──────────────┐     │
   └───────────────────────────────────┼─────┘
                                        ▼
                     SubPlan(Fragment 1, hash-partitioned)
                            RemoteSourceNode ──┐
                                               ▼
                            SubPlan(Fragment 2, source/leaf)
                                   TableScanNode (splits)
```

每個 `PlanFragment` 被 scheduler 實體化成一個 **Stage**，Stage 在多 worker 上跑成多個 **Task**。

### 7.2 Exchange：跨 task 資料搬運

`core/trino-main/src/main/java/io/trino/operator/DirectExchangeClient.java`（pipelined 模式）：

- `addLocation(TaskId, URI)` 註冊上游 task 來源，建立對應的 `HttpPageBufferClient`（`DirectExchangeClient.java:170`）。
- `scheduleRequestIfNecessary()`（`:185`、`:192`、`:235`）動態發 HTTP 請求，依 buffer 剩餘容量平衡併發。
- pages 累積在 `DirectExchangeBuffer`；`pollPage()`（`:223`）取出 page、更新記憶體、必要時再排請求。
- `isFinished()`（`:243`）：buffer 消費完且 `completedClients.size() == allClients.size()`（`:73`、`:80`）為 true。
- **遠端 task 失敗傳播**：`HttpPageBufferClient` 失敗回呼時，透過 `taskFailureListener.onTaskFailed(...)`（`:99`、`:128`）向外廣播，中斷下游讀取。`@ThreadSafe`，以 synchronized 與並行集合保護。

上游 `ExchangeOperator` 把拉到的 page 餵進本地 pipeline。重分區由上游 task 的 output buffer 依 `PartitioningHandle` 做 hash 分桶寫出，下游各 task 拉自己那一份 partition。資料以 Trino 自訂二進位格式（serialized page bytes）在 HTTP 上傳輸。

### 7.3 Task 執行：SqlTaskExecution

實際讀 `core/trino-main/src/main/java/io/trino/execution/SqlTaskExecution.java`：

- 持有 `io.trino.execution.executor.TaskExecutor taskExecutor`（`SqlTaskExecution.java:32`、`:91`）。
- **split-lifecycle vs task-lifecycle 二分**（建構子分流，`:138`–`161`）：
  - **partitioned drivers（split-lifecycle）**：有 partitioned source（如 TableScan）的 pipeline → `driverRunnerFactoriesWithSplitLifeCycle`（`:97`、`:147`）。一個 split 一個 driver。
  - **unpartitioned drivers（task-lifecycle）**：其餘（如 remote source / exchange pipeline）→ `driverRunnerFactoriesWithTaskLifeCycle`（`:98`、`:152`）。整個 task 一個 driver。
- `addSplitAssignments()`（`:312`）用 sequence ID 過濾已知 split，partitioned 走 `schedulePartitionedSource()`（`:313`、`:343`），其餘走 `factory.enqueueSplits(...)`（`:318`）；task-lifecycle 在啟動時一次性 `scheduleDriversForTaskLifeCycle()`（`:201`、`:375`）。
- **精確終止追蹤**：`DriverAndTaskTerminationTracker`（`:95`、`:135`）以原子計數記錄存活 driver；driver 銷毀觸發 `getDestroyedFuture()` 監聽器時遞減，歸零且 task 終止中時呼叫 `taskStateMachine.terminationComplete()`（`:195`）宣告完成。

> → 對 arneb 的啟發：
> 1. **fragment 邊界 == REMOTE exchange**，且用 `RemoteSourceNode` 把「遠端來源」物化成一個 plan node——arneb 已有 ExchangeExec / RepartitionExec 與 fragmenter，方向一致。值得補強 **`PartitioningHandle` + `getCommonPartitioning()` 的「協調衝突 partitioning」邏輯**：arneb 記憶顯示 Q05/Q09 因缺 broadcast 與 partition-property 推導而 OOM/慢，這正對應 `FragmentProperties.setDistribution()` 與 PreferredProperties / PropertyDerivations 一族（DataFusion 的 EnforceDistribution 同源）。
> 2. **`TaskExecutor` 是「執行緒池 + 協作式 time-slice」的中央排程器，所有 query 共用**——與 arneb「每 task 一個 tokio task + semaphore permit」相反。Trino 的 split runner 跑一小段就讓出，公平輪轉，避免 head-of-line blocking 與長期 permit 占用。arneb 反覆遇到的 exchange stall / OutputBuffer 死等，很可能源於「持有式」併發控制；引入 time-slice 協作式排程是結構性解法。
> 3. **split-lifecycle vs task-lifecycle driver 的二分** 對 arneb 也有用：scan（source）pipeline 隨 split 動態擴張，join/exchange pipeline 則整個 task 存在。

---

## 8. 記憶體管理、資源控管與 spill-to-disk

### 8.1 記憶體（`io.trino.memory`）

| Class | 職責 |
|---|---|
| `MemoryPool` | 記憶體配額單位，追蹤並管理 query/operation 的記憶體用量 |
| `QueryContext` | 單一 query 的記憶體 context |
| `LocalMemoryManager` | 節點層級記憶體管理 |
| `ClusterMemoryManager` / `ClusterMemoryPool` | 跨叢集協調與叢集級記憶體池 |
| `RemoteNodeMemory` / `MemoryInfo` | 追蹤其他節點的記憶體狀態 |
| `LowMemoryKiller` | 記憶體吃緊時的 query / task 終止策略介面 |
| `MemoryManagerConfig` / `NodeMemoryConfig` | 配置門檻 |

層級回報路徑：`OperatorContext → DriverContext → PipelineContext → TaskContext → QueryContext → MemoryPool`。

**「先 reserve 才 allocate」是全域不變式**（核實 `core/trino-main/src/main/java/io/trino/memory/MemoryPool.java`）：

- `reserve(TaskId, tag, bytes)`（`MemoryPool.java:123`）阻塞式保留；`tryReserve(...)`（`:189` 附近）非阻塞嘗試，`getFreeBytes() - bytes < 0` 時失敗。
- **Over-commitment / 不可取消阻塞**：`getFreeBytes()` 可回傳負值（pool 已過度承諾）；剩餘 ≤ 0 時 `reserve` 回傳 `NonCancellableMemoryFuture`（`:55`、`:137`、`:168`），其 `cancel` 直接拋 `UnsupportedOperationException`——記憶體 future 不可被取消。
- **QueryContext 超額**：`resourceOverCommit` 為真時 `initializeMemoryLimits` 把單一 query 的 `maxUserMemory` 放寬至整個 pool 上限，完全交由 coordinator / `LowMemoryKiller` 決定何時殺掉（`QueryContext.java:138`–`150`）。

**`LowMemoryKiller` 策略**（多種實作並存於 `io/trino/memory/`）：

- `NoneLowMemoryKiller`：不殺。
- `TotalReservationLowMemoryKiller`：殺記憶體預留總量最大的 query（`TotalReservationLowMemoryKiller.java:26`–`38`）。
- `TotalReservationOnBlockedNodesQueryLowMemoryKiller` / `...TaskLowMemoryKiller`：只在被阻塞的節點上挑。
- `LeastWastedEffortTaskLowMemoryKiller`：針對 `RetryPolicy.TASK` 的查詢，優先殺 speculative task，或以 `memoryUsed / wallTime` 挑「性價比最低」者（`LeastWastedEffortTaskLowMemoryKiller.java:38`–`82`、`:101`–`111`）。

### 8.2 Spill-to-disk（`io.trino.spiller`）

| 介面 / Class | 角色 |
|---|---|
| `Spiller` / `SpillerFactory` | 頂層 spill 介面與工廠 |
| `SingleStreamSpiller` / `FileSingleStreamSpiller` | 單流 spill（檔案式）|
| `GenericSpiller` | 通用 spiller |
| `PartitioningSpiller` / `GenericPartitioningSpiller` | 分區 spill（hash join build side 依 partition spill）|
| `SpillSpaceTracker` | 追蹤磁碟空間用量 |
| `LocalSpillManager` / `NodeSpillConfig` / `SpillerStats` | 管理、配置、統計 |

- **觸發算子**：主要是 `HashBuilderOperator`（Join）、`HashAggregationOperator`（見 6.4）、`OrderByOperator`（Sort）。
- **運作**：達門檻時把資料以序列化 Page 寫到本地 `spill-path`（常見壓縮如 ZSTD），之後讀回合併。`PartitioningSpiller` 支援按 partition 分流落地，對 grace hash join 特別重要。
- **memory revocation 協議**：壓力可反向通知 operator 主動 spill（`startMemoryRevoke` / `finishMemoryRevoke` + Driver 的 revocation future），而非被動 OOM。

> → 對 arneb 的啟發：
> 1. **記憶體追蹤要「全域單一閘門、先 reserve 才 allocate」**。arneb 的 OOM 根因是「只追蹤部分 operator」（記憶中 Filter/Project/Repartition channel/scan buffer 的 untracked Arrow 配置溢出 cgroup 才觸發 spill）。Trino 的 `MemoryPool` 是**所有** operator 經 `OperatorContext` 申請的唯一閘門；arneb 要根治需把每個會放大記憶體的 operator（含 channel buffer、scan buffer）都納入同一追蹤閘門，而非事後 RSS 探測。
> 2. **memory revocation 協議**讓「記憶體壓力 → 主動 spill」成為平滑回路，比 arneb 現行「admission 時 fail-fast」更柔和。
> 3. **`LowMemoryKiller`**：當 spill 都救不了時，有明確的「挑一個 query / task 殺」策略（多種策略可插拔），避免整個 worker 倒下；arneb 目前傾向 OOM 整個 worker，值得引入 query 級優雅終止 + 可插拔殺手策略。

---

## 9. 儲存與資料來源抽象（Connector SPI）

`core/trino-spi/src/main/java/io/trino/spi/connector` 是 Trino 與所有資料來源之間的合約：

```
Connector ──┬─ ConnectorMetadata          (DDL/metadata + pushdown)
            ├─ ConnectorSplitManager       (產生 splits)
            ├─ ConnectorPageSourceProvider (產生 ConnectorPageSource → 讀資料成 Page)
            ├─ ConnectorPageSinkProvider   (寫資料)
            ├─ ConnectorNodePartitioningProvider (分區語意)
            └─ ConnectorAccessControl      (權限)

  Handle 類: ConnectorTableHandle / ConnectorSplit / ConnectorInsertTableHandle / ...
  Session : ConnectorSession
```

### 9.1 ConnectorMetadata 與 pushdown

實際讀 `core/trino-spi/src/main/java/io/trino/spi/connector/ConnectorMetadata.java`：

- 基本 metadata：`getTableHandle()`、`getTableMetadata()`、`listTables()`、`getColumnHandles()`、`getTableStatistics()`。
- **Pushdown 方法**（皆回傳 `Optional<...ApplicationResult<ConnectorTableHandle>>`，預設實作 `return Optional.empty()`）：
  - `applyLimit()`（`ConnectorMetadata.java:1408`）— 下推 row limit。
  - `applyFilter()`（`:1431`）— 下推謂詞 / `Constraint`。
  - `applyProjection()`（`:1506`）— 下推欄位選取與運算式。
  - `applyAggregation()`（`:1599`）— 下推 GROUP BY 聚合。
  - `applyJoin()`（`:1641`）— 下推 join。
  - `applyTopN()`（`:1667`）— 下推排序＋limit。

- **無限迴圈契約（已逐字核實）**：每個 pushdown 方法的 javadoc 都明文警告——connector 即使一般支援該下推，**只要本次呼叫沒有效果就必須回傳 `Optional.empty()`，否則會導致 optimizer「to loop indefinitely」**（無限迴圈）。此 note 在 `applyLimit`（`:1393`–`1398`）、`applyFilter`（`:1417`–`1422`）、`applyProjection`（`:1518`–`1520`）、`applyAggregation`（`:1534`–`1536`）、`applyJoin`（`:1660`–`1662`）等多處重複出現。
- **applyFilter 參數校驗**：javadoc 註明「applyFilter is expected not to be invoked with a 'false' constraint」（`:1433`）。
- **applyAggregation 全域聚合表示法**：`groupingSets.isEmpty()` 時直接拋例外（`:1607`）；全域聚合（如 `SELECT count(*)`）**並非以空 List 表示，而是以「包含一個空 List 的 List」`[[]]`**（javadoc 例示於 `:1562`）。

### 9.2 資料讀取路徑

```
ConnectorSplitManager.getSplits()  → 一批 ConnectorSplit（如 HDFS block / Parquet row group 範圍）
        ↓ scheduler 把 split 分派到 worker
ConnectorPageSourceProvider.createPageSource(split, columns)
        ↓
ConnectorPageSource.getNextPage()  → Page（直接產欄式資料）
```

- `Split` 是計算的最小任務單元，**由 connector 自己依資料佈局決定粒度**（row group、檔案、partition）；引擎只負責把 split 分派到 worker。
- `ConnectorRecordSetProvider` 是較舊的逐列 API；新 connector 用 `ConnectorPageSource` 直接吐 Page（向量化、零拷貝友善）。

> → 對 arneb 的啟發：
> 1. **pushdown 回傳 `Optional` / `None` 的契約是強約束**——arneb 的 `connectors` crate 實作 filter/projection/limit pushdown 時務必遵守「無效就回傳未改變 / None」，否則 rule-based optimizer 會抖動或迴圈。arneb 已有 Parquet row-group pruning + ArrowPredicate pushdown，正對應 `applyFilter` + `ConnectorPageSource`。
> 2. **Split 由 connector 切、引擎只分派**——arneb 的「per-file row-range scan splits」正是此模型，職責清楚。
> 3. `ConnectorPageSource` 直接吐 Page（= Arrow batch）而非逐列——arneb 的 `DataSource` trait 回傳 `SendableRecordBatchStream` 已對齊。
> 4. `applyJoin` / `applyAggregation` 這類「整段算子下推」對聯邦查詢威力很大（讓底層 RDBMS 算 join），arneb 若做聯邦場景可規劃為未來 SPI 擴充點。

---

## 10. 並行模型與排程

### 10.1 叢集排程（`io.trino.execution.scheduler`）

| Class | 職責 |
|---|---|
| `SqlQueryScheduler` / `QueryScheduler` | 編排整個 query 的排程 |
| `PipelinedQueryScheduler` | pipelined（串流）執行模型的 query scheduler |
| `PipelinedStageExecution` / `StageExecution` | stage 層級執行 |
| `NodeScheduler` | 節點選擇與 task 指派的協調者 |
| `NodeSelector`（+ `UniformNodeSelector` / `TopologyAwareNodeSelector`）| 節點選擇策略（均勻 / 拓樸感知）|
| `SplitPlacementPolicy` / `DynamicSplitPlacementPolicy` | split → node 的指派策略 |
| 子 package `faulttolerant/` | 容錯排程（FTE）|
| 子 package `policy/` | 排程順序策略（如 phased / all-at-once）|

兩種排程拓撲：

- **Pipelined（互動式預設）**：`PipelinedQueryScheduler` 一次把所有 stage 拉起，stage 間以串流 exchange 連接，邊產邊送。低延遲，但任一 task 失敗整個 query 失敗。
- **Fault-tolerant（FTE）**：`faulttolerant/` + `EventDrivenFaultTolerantQueryScheduler`，stage 間 exchange 物化（spool）到外部儲存，task 可重試。適合長批次。

**節點選擇與 split 配額**（核實 `core/trino-main/src/main/java/io/trino/execution/scheduler/NodeSchedulerConfig.java`）：

- `node-scheduler.max-splits-per-node` 限制每個 worker 上排隊的 split 最大數量，**預設 256**（`NodeSchedulerConfig.java:49` `private int maxSplitsPerNode = 256;`；config key 於 `:139`）。防止慢節點被過度分配。
  - **核實修正**：agy 草稿稱「預設 250」**有誤**，正確為 256。
- `TopologyAwareNodeSelector` 考慮網路拓樸（rack/host）提升資料本地性；`UniformNodeSelector` 均勻散佈。

### 10.2 Worker 端執行緒模型

如第 7.3 節，worker 端由 **`io.trino.execution.executor.TaskExecutor`** 排程所有 task 的 split runner（Driver）。預設的 `TimeSharingTaskExecutor`（`io/trino/execution/executor/timesharing/TimeSharingTaskExecutor.java:85`）持有 `runnerThreads` 個工作執行緒（通常與 CPU 核心數相關，`:94`、`:235`），以 `MultilevelSplitQueue` 做優先序輪轉；每個 split runner 跑一個 `SPLIT_RUN_QUANTA`（1 秒，見 6.3）就讓出。另有 `ThreadPerDriverTaskExecutor`（dedicated）為每 driver 配專屬執行緒的替代實作。

> → 對 arneb 的啟發：
> 1. **「pipelined vs fault-tolerant」是兩套可插拔 scheduler**——arneb 目前 pipelined-only。若未來跑長批次，FTE 的「exchange 物化到外部儲存 + task 重試」是清楚的演進路徑，代價是延遲與一個外部 spool 服務。
> 2. **資料本地性靠 `TopologyAwareNodeSelector`**——arneb 若 worker 與資料（MinIO/HDFS）共置，把 split 排到資料所在 node 能大幅省 exchange 流量。
> 3. 重申第 6/7 章重點：**中央 `TaskExecutor` 協作式 time-slice 是 Trino 多 query 公平性與避免長期占用的根本**，與 arneb 的 per-task permit 模型分歧——這是 arneb 反覆 deadlock 的結構性根因，最值得借鏡的單一設計。

---

## 11. 程式碼地圖（關鍵目錄與模組職責對照）

| Module / Package（已核實路徑）| 對應 arneb crate（概念對照）| 職責 |
|---|---|---|
| `core/trino-grammar` → `io.trino.grammar.sql` | `sql-parser`（文法）| ANTLR4 文法 `SqlBase.g4` |
| `core/trino-parser` → `io.trino.sql.parser` | `sql-parser` | SQL → AST（`SqlParser`、`AstBuilder`）|
| `core/trino-spi` → `io.trino.spi.connector` | `connectors`（trait 層）/ SPI | 資料來源合約（`ConnectorMetadata`、`ConnectorSplitManager`、`ConnectorPageSource`）|
| `core/trino-spi` → `io.trino.spi.Page` / `io.trino.spi.block` | （Arrow RecordBatch / Array）| 欄式執行資料單位 |
| `io.trino.sql.analyzer` | `planner`（analyze 階段）| 語意分析（`StatementAnalyzer`、`Analysis`、`Scope`）|
| `io.trino.sql.planner` | `planner` | 邏輯規劃（`LogicalPlanner`、`QueryPlanner`、`Symbol`）|
| `io.trino.sql.planner.iterative` | `planner`（optimizer）| Rule-based 框架（`IterativeOptimizer`、`Rule`、`Memo`、`Pattern`）|
| `io.trino.sql.planner.optimizations` | `planner` | 各種 optimizer pass |
| `io.trino.cost` | `planner`（cost/stats）| 統計與成本（`StatsCalculator`、`CostCalculator`、`PlanNodeStatsEstimate`）|
| `io.trino.sql.planner.PlanFragmenter` | `planner`（`fragment.rs`）| 切 fragment（`SubPlan`、`PlanFragment`、`RemoteSourceNode`、`PartitioningHandle`）|
| `io.trino.execution`（`SqlQueryExecution`、`SqlTaskExecution`）| `server` / `scheduler` / `rpc` | query/task 編排與生命週期 |
| `io.trino.execution.scheduler` | `scheduler` | 叢集排程（`NodeScheduler`、`SqlQueryScheduler`、`NodeSelector`）|
| `io.trino.execution.executor` | `execution` / `scheduler` | **`TaskExecutor` 中央排程器**（`TimeSharingTaskExecutor` / `ThreadPerDriverTaskExecutor`）|
| `io.trino.operator` | `execution`（operators）| Driver / Operator 框架與所有運算元實作 |
| `io.trino.operator`（`DirectExchangeClient`、`ExchangeOperator`）| `rpc`（Flight）/ `execution`（ExchangeExec）| 跨 task exchange |
| `io.trino.memory` | `execution`（MemoryPool）/ `server` | 記憶體管理三層 + `LowMemoryKiller` |
| `io.trino.spiller` | `execution`（spill）| spill-to-disk |
| `io.trino.split` | `execution` / `scheduler` | split 管理 |
| `io.trino.metadata` | `catalog` | metadata / 函式註冊 / catalog 解析 |
| `io.trino.server`（含 `server.protocol`）| `server` / `protocol` | HTTP server、`/v1/statement`、task resource |
| `io.trino.connector` | `connectors` / `hive` | connector 註冊與管理 |
| `io.trino.transaction` | （無直接對應）| 交易管理 |

---

## 12. 對 arneb（Rust 自建引擎）的具體啟發與可借鏡

依重要性排序：

### 12.1 【最高優先】worker 端改用「協作式 time-slice 排程」取代「permit 持有」

arneb 記憶中反覆出現的 deadlock（streaming refactor deadlock、OutputBuffer 死等、exchange stall）共同根因是：**一個 tokio task 持有 semaphore permit 跑完整個 operator 生命週期，與 stream back-pressure 不相容**。Trino 的 `Driver` + `TaskExecutor` 給出結構性解法：

- Driver 不擁有執行緒、不持有任何 permit（已核實 Driver.java 無 `Semaphore`），每次 `process(maxRuntime, maxIterations)` 只跑一小段（預設量子 `SPLIT_RUN_QUANTA = 1 SECOND`）就回傳。
- 中央 `TaskExecutor`（`TimeSharingTaskExecutor`）以 `MultilevelSplitQueue` 公平輪轉所有 query 的 split runner。
- 阻塞用 `ListenableFuture` 表達（arneb 對應 `Future` / `Waker`），而非持鎖等待。

→ arneb 應評估把「per-task permit」換成「協作式 yield + 中央排程器」，比逐個 query 打補丁更根本。記憶中的「Phase A：刪 `task_manager` semaphore」是正確方向。

### 12.2 【高】記憶體追蹤要「全域單一閘門、先 reserve 才 allocate」

arneb 的 OOM 根因是「只追蹤部分 operator」。Trino 的 `MemoryPool` 是**所有** operator 經 `OperatorContext` 申請的唯一閘門（`MemoryPool.reserve` / `tryReserve`），且配 memory revocation 讓壓力反向觸發 spill。

→ arneb 應把 Filter/Project/Repartition channel/scan buffer 等「未追蹤的 Arrow 配置」全部納入同一 `MemoryPool`，引入 `startMemoryRevoke` / `finishMemoryRevoke` 等價回路取代事後 RSS 探測，並加 `LowMemoryKiller` 等價物做 query 級優雅終止（可參考 `TotalReservation...` 與 `LeastWastedEffort...` 多策略設計）。

### 12.3 【高】partitioning property 推導，解 Q05/Q09 的 join 分佈問題

Trino 的 `PartitioningHandle` + `FragmentProperties.setDistribution()` + `getCommonPartitioning()`（PlanFragmenter）、加上 property derivation 一族，是「決定 broadcast vs partitioned join、何時插 exchange、何時可消除 exchange」的核心。arneb 記憶顯示這正是 Q05/Q09 慢/OOM 的缺口。

→ 補上 partitioning-property 推導（DataFusion EnforceDistribution / Trino PreferredProperties 同源），讓 fragmenter 能「小表 broadcast、大表 hash partition」並消除多餘 reshuffle。

### 12.4 【中】rule-based optimizer 的不變式紀律

每條 rule「無效必回傳原樣／None」、加 optimizer 逾時保護（對應 `OPTIMIZER_TIMEOUT`）、用 `CachingStatsProvider` 等價層快取統計。`StatsCalculator` 與 `CostCalculator` 分兩層值得照抄。pushdown 方法的「無效回 `Optional.empty()` 否則無限迴圈」契約是硬約束。

### 12.5 【中】operator 內依記憶體狀態切換 in-memory / spillable builder

`HashAggregationOperator` 在同一 operator 內選 `InMemoryHashAggregationBuilder` 或 `SpillableHashAggregationBuilder`，且明確處理 partial/final 的 identity 邊界（COUNT=0）。arneb 的 HashAgg 應有對等切換，與已有的 Grace HJ / SemiJoin spill 一致。

### 12.6 【中】Analyzer 階段就把名稱解析定案（顯式 Scope）

Trino 在 analyzer 用 `Scope` 樹把每個 column reference 解析到唯一目標，產出富 metadata 的 `Analysis`。arneb 若在子查詢/CTE 遇到欄位對應錯亂，應把名稱解析前移到 analyze 階段定案。

### 12.7 已對齊、無需改的部分

- **Page/Block 欄式向量化** → arneb 用 Arrow RecordBatch 天然對齊。
- **storage/compute 分離 + connector SPI** → arneb 的 `connectors`/`catalog`/`DataSource` trait 已對齊；pushdown 回傳 `Optional`/`None` 契約要守住。
- **split 由 connector 切、引擎只分派** → arneb per-file row-range split 已對齊。
- **fragment 邊界 = remote exchange** → arneb fragmenter 已對齊。

### 12.8 進階借鏡：HTTP Shuffle 升級為 Arrow Flight

Trino 的 exchange 長期依賴自訂格式的 HTTP 傳輸（serialized page bytes），序列化/反序列化的 CPU 開銷不小。arneb 在節點間 shuffle 直接使用 **Arrow Flight（gRPC + HTTP/2）**，支援原生二進位 RecordBatch 近零拷貝傳輸，可顯著降低 shuffle 的 CPU 佔用——這是 arneb 相對 Trino 的天然優勢，應持續貫徹。

---

## 驗證方法與來源

- **核實基準**：本文件所有 file:line 引用，均對照本地 checkout 的 `trinodb/trino`（分支 `master`，commit **`82c47e2be10`**，以 `git -C /Users/bochengyang/formosa-ventures/repos/trino rev-parse --short HEAD` 取得）。路徑相對 repo root（例如 `core/trino-main/src/main/java/io/trino/operator/Driver.java:283`）。
- **latest 回查（2026-06-05）**：已 fetch 並以 `origin/master` commit **`7aa62d08f3ef`** 抽查。`SqlQueryExecution`、`PlanFragmenter`、`IterativeOptimizer`、`TaskExecutor`、`Driver`、`Operator`、`Page`、`DirectExchangeClient` 等文件核心符號仍存在，架構主結論仍成立；本文行號未重標，不能視為 latest 精準行號。
- **引用慣例**：技術名詞 / 類別名 / 套件路徑 / 方法名一律保留英文；行號為本次以 `grep -n` 直接定位的實際行號。由於 Trino 持續演進，若日後 checkout 版本變動，以實際程式碼為準。
- **合併來源**：本文件由三份輸入交叉合併而成——
  1. **agy 版草稿**（訓練知識，未驗證）：`docs/software-arch/trino-agy.md`，提供 12 章結構骨架、ASCII 圖、對 arneb 的多數啟發段落。
  2. **agent 版草稿**（WebFetch 讀過 GitHub、文末誠實標註未取得行號）：`docs/software-arch/trino-agent.md`，提供經 WebFetch 確認的 class/method 名與較精確的職責對照表、12.x 排序。
  3. **agy/agent 本地跨核清單**（已對照本地檔案附 file:line）：`/tmp/verify_trino_agy.md`，提供 Driver / PlanFragmenter / IterativeOptimizer / ConnectorMetadata / Page / SqlTaskExecution / DirectExchangeClient / HashAggregationOperator / memory 等的精確行號，本次再以原始碼複核。
- **本次修正的主要錯誤**（相對草稿）：
  1. Driver/split runner **不持有任何 Semaphore/permit**（Driver.java 全檔無 Semaphore），且時間片量子為 **1 秒**（`PrioritizedSplitRunner.SPLIT_RUN_QUANTA`），非 agy 草稿的「250 毫秒」。
  2. `TaskExecutor` 位於 **`io.trino.execution.executor`**，非 agy 草稿的 `io.trino.operator`；且有 `TimeSharingTaskExecutor` 與 `ThreadPerDriverTaskExecutor` 兩個實作。
  3. ANTLR 文法 `SqlBase.g4` 位於 **`core/trino-grammar`（`io.trino.grammar.sql`）**，非舊草稿暗示的 `trino-parser` 內（`SqlParser.java` 仍在 `core/trino-parser`）。
  4. `max-splits-per-node` 預設為 **256**，非 agy 草稿的「250」。
  5. pushdown「無效必回 `Optional.empty()` 否則無限迴圈」契約，已用原始碼 javadoc「to loop indefinitely」逐字確認，並補上各方法行號。
  6. `LowMemoryKiller` 為**多策略可插拔**（含 `TotalReservation...`、`LeastWastedEffort...`、`...OnBlockedNodes...`、`None`），非單一實作。
  7. 補強：全域聚合在 `applyAggregation` 以 `[[]]`（含一個空 List 的 List）表示，非空 List。
- **補上的 file:line 引用數**：本版相對 agent 草稿（agent 草稿刻意完全不附行號）新增了約 **70 處** `相對路徑:行號` 引用，涵蓋 Driver、PlanFragmenter、IterativeOptimizer、ConnectorMetadata、Page、HashAggregationOperator、SqlTaskExecution、DirectExchangeClient、MemoryPool、QueryContext、LowMemoryKiller 系列、NodeSchedulerConfig、PrioritizedSplitRunner、TaskExecutor / TimeSharingTaskExecutor。
- **仍無法 100% 確認的點（誠實標註）**：
  1. **CBO 細節**：agy 草稿提及的「DPHyp 演算法」「`io.trino.cost.Cost` 的具體成本公式」「`join-multi-clause-independence-factor`」未在本次（受限於只核實指定關鍵檔案、不遍歷整庫）逐一打開 `io.trino.cost` 下每個 class 與 join-reorder optimizer 核實；本文僅保留已確認的 class 名與「DP + 啟發式」層級描述。建議若要引用 join-reorder 演算法的精確名稱，再開 `io.trino.sql.planner.iterative.rule.ReorderJoins` / `io.trino.cost` 對應檔覆核。
  2. **Page 批次大小上限**（草稿稱「1024 行或 1MB」）：`Page.java` 本身未硬編此上限，實際由各 PageSource / PageProcessor 的設定（如 `MAX_BATCH_SIZE`）決定，本次未逐檔核實精確常數，故本文未斷言具體數字。
  3. **memory revocation 在 Driver 端的完整 future 組合行為**：已確認 `firstFinishedFuture` + memory-pool revoke 二擇一即完成的精神（Driver.java:465/656），但未逐行追完 `updateDriverBlockedFuture` 的所有分支。
  4. `MemoryPool.tryReserve` 的確切方法名/簽章以本次 grep 命中的 `:189` 附近為準；如需精確簽章建議直接開檔覆核。
