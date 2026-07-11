# DuckDB 內部架構與設計決策深析：以 Rust 自建查詢引擎 arneb 為視角的權威對照指南

> 本文以本地 checkout 的 `github.com/duckdb/duckdb`（`main` 分支，commit `7e889c9168`）原始碼為原始核實基準，合併兩份既有草稿（agy 版與 agent 版），並逐項以 `相對路徑:行號` 核實關鍵 claim。2026-06-05 另以 latest `origin/main` commit `0ce48ae355b2` 回查：`Optimizer::RunBuiltInOptimizers()` 37 個 pass 順序與主要類別/常數仍可對上；凡引用行號者，仍指原始核實 commit 之檔案內容。
>
> 讀者設定：你正在用 Rust 打造一個 Trino 的替代品 `arneb`（分散式 SQL 查詢引擎，Arrow-native、push-based、coordinator/worker 架構）。每一章末「對 arneb 的啟發」是本文重點。

---

## 1. 專案定位與設計哲學

DuckDB 的定位是「用於分析的 SQLite」（SQLite for Analytics）：一個嵌入式（in-process）、無伺服器（serverless）的 OLAP 引擎。此定位決定了它所有底層架構決策，並與分散式服務端引擎（如 Trino，或以 Rust 開發中的 `arneb`）形成鮮明對比。

| 維度 | DuckDB | Trino / arneb |
|------|--------|---------------|
| 部署形態 | **嵌入式（in-process）程式庫**，像 SQLite | 獨立的分散式服務（coordinator + workers） |
| 並行模型 | **單機多核心**（morsel-driven parallelism） | 跨節點分散式（network exchange） |
| 資料局部性 | 同一進程記憶體，零序列化 | 跨節點需序列化 + 網路傳輸 |
| 主要瓶頸假設 | CPU / 記憶體頻寬 | 網路 shuffle + CPU |
| 容錯 | 單機，無 task 重試 | 需考慮 worker 失敗 |

### 1.1 嵌入式架構的優勢與權衡

* **零網路序列化開銷**：DuckDB 與應用程式同進程，不需 TCP/Unix Socket 傳輸；可與外部以 Apache Arrow / Pandas 零拷貝（zero-copy）交換資料。
* **單節點極限優化**：不需考慮節點間容錯與網路通訊，設計重心完全放在單機 CPU 多核利用、快取友善度（cache locality）與記憶體頻寬。
* **資源共用約束**：作為 library 形式運行，不能無限搶佔宿主進程資源。這要求 `BufferManager` 與 `TaskScheduler` 能對記憶體與執行緒做細粒度動態控制。

DuckDB 核心設計哲學（從原始碼結構可清楚讀出）：

1. **Vectorized + push-based execution**：所有運算以 `DataChunk`（一批向量，預設 2048 列）為單位流動，攤平函式呼叫開銷。
2. **Morsel-driven parallelism**（Leis et al. 2014 落地）：不為每個運算子開固定執行緒，而是把資料切成小塊（morsel），由 `TaskScheduler` 動態派發給 worker 執行緒，達成負載均衡。
3. **Out-of-core 為一等公民**：`BufferManager`、`JoinHashTable::ProbeSpill`、`RadixPartitionedHashTable` 都內建 spill-to-disk，不假設資料能裝進記憶體。
4. **單一二進位、自帶儲存引擎**：自帶 row-group 列存格式、WAL、checkpoint、ART index、buffer pool。
5. **借用成熟元件而非重造**：SQL parser 直接 fork PostgreSQL 的 `libpg_query`（見 `src/parser/`）。

### 對 arneb 的啟發

DuckDB 是「把分散式問題變成單機多核問題」的極致樣板。`arneb` 雖是分散式（Coordinator-Worker），但在 **Worker 內部節點** 上，為壓榨單機效能，幾乎可照搬 DuckDB 的 morsel-driven + push-based 模型；真正不同的只在「跨 worker 的 exchange 邊界」。換句話說，**DuckDB 就是 arneb 一個 worker 應該長成的樣子**：把問題拆成「跨 worker exchange（arneb 自己的事）」+「worker 內單機執行（照搬 DuckDB）」兩層。

---

## 2. 整體架構與核心組件

從 `src/` 頂層目錄可看到子系統切分：

```
src/
├── parser/        # SQL 字串 → parse tree（基於 PostgreSQL libpg_query）
├── planner/       # parse tree → LogicalOperator 樹（Binder 做語意分析）
├── optimizer/     # 邏輯計畫最佳化（見 §5 完整 pass 清單）
├── execution/     # LogicalOperator → PhysicalOperator + 向量化執行
├── parallel/      # Pipeline / Executor / TaskScheduler（morsel-driven 並行）
├── catalog/       # schema / table / function 註冊與解析
├── storage/       # buffer manager / row groups / WAL / checkpoint / ART
├── transaction/   # MVCC、COMMIT/ROLLBACK
├── function/      # 內建純量/聚合/表函式
├── common/        # DataChunk、Vector、型別系統、工具
├── main/          # ClientContext、DatabaseInstance、Connection、QueryResult
├── logging/
└── include/       # 對應上述所有的標頭檔
```

組件關係圖（資料 / 控制流）：

```
                         ┌─────────────────────────────────────────┐
   SQL string  ──────────►            main/ClientContext            │
                         │  (查詢進入點、交易、prepared statement)   │
                         └───────┬─────────────────────────────────┘
                                 │
            ┌────────────────────▼──────────────────┐
            │ parser/  Parser + Transformer          │
            │  libpg_query → ParsedExpression /      │
            │  SQLStatement / QueryNode              │
            └────────────────────┬──────────────────┘
                                 │ parse tree
            ┌────────────────────▼──────────────────┐
            │ planner/ Binder (+ BindContext)        │◄──── catalog/ CatalogEntry
            │  → BoundStatement { LogicalOperator }  │      (解析 table/column/function)
            └────────────────────┬──────────────────┘
                                 │ LogicalOperator 樹
            ┌────────────────────▼──────────────────┐
            │ optimizer/ Optimizer                   │
            │  RunBuiltInOptimizers：多個 pass       │
            │  (pushdown / join order / stats …)     │
            └────────────────────┬──────────────────┘
                                 │ optimized LogicalOperator
            ┌────────────────────▼──────────────────┐
            │ execution/ PhysicalPlanGenerator       │
            │  → PhysicalOperator 樹                 │
            └────────────────────┬──────────────────┘
                                 │ PhysicalOperator
            ┌────────────────────▼──────────────────┐
            │ parallel/ Executor                     │
            │  切成 MetaPipeline → Pipeline → Task   │
            │  TaskScheduler 派發給 worker threads   │──► storage/ BufferManager
            │  DataChunk(Vector) push 流經 operators │    (Pin/Unpin、spill)
            └────────────────────┬──────────────────┘
                                 │ DataChunk
                         ┌───────▼─────────┐
                         │ QueryResult     │ (Materialized / Stream)
                         └─────────────────┘
```

關鍵類別與其檔案：

| 角色 | 類別 | 檔案 |
|------|------|------|
| 查詢入口 | `ClientContext` | `src/main/client_context.cpp` |
| DB 實例 | `DatabaseInstance` | `src/main/database.cpp` |
| 語意分析 | `Binder` | `src/planner/binder.cpp`（宣告 `src/include/duckdb/planner/binder.hpp:202`） |
| 邏輯運算子 | `LogicalOperator` | `src/planner/logical_operator.cpp` |
| 最佳化 | `Optimizer` | `src/optimizer/optimizer.cpp`（pass 清單見 `RunBuiltInOptimizers`，`optimizer.cpp:169`） |
| 實體運算子 | `PhysicalOperator` | `src/execution/physical_operator.cpp`（介面 `src/include/duckdb/execution/physical_operator.hpp`） |
| 實體計畫產生 | `PhysicalPlanGenerator` | `src/execution/physical_plan_generator.cpp` |
| 執行協調 | `Executor` | `src/parallel/executor.cpp` |
| 管線 | `Pipeline` / `MetaPipeline` | `src/parallel/pipeline.cpp` / `meta_pipeline.cpp` |
| 排程 | `TaskScheduler` | `src/parallel/task_scheduler.cpp` |
| 資料批 | `DataChunk` / `Vector` | `src/common/types/data_chunk.cpp` / `vector.cpp` |
| 記憶體 | `BufferManager` | `src/storage/buffer_manager.cpp`（介面 `src/include/duckdb/storage/buffer_manager.hpp`） |

### 對 arneb 的啟發

DuckDB 的子系統切分（parser / planner / optimizer / execution / parallel / storage 各自獨立）與 arneb 的 crate 切分（`sql-parser` / `planner` / `execution` / `scheduler` / `rpc`）幾乎一一對應。差別在 DuckDB 多了一個獨立的 `parallel/` 層，專責「邏輯計畫 → 可排程 task」的轉換；它把「fragment（`MetaPipeline`）」與「執行單元（`Task`）」分成兩層，是值得借鏡的清晰度——arneb 的 `PlanFragmenter` 角色類似但兩者混在一起。

---

## 3. 查詢生命週期：從 SQL 字串到結果

```
1. ClientContext::Query(sql)
2.   Parser::ParseQuery(sql)
        └─ libpg_query 產生 PostgreSQL parse tree
        └─ Transformer 轉成 DuckDB 的 SQLStatement / QueryNode / ParsedExpression
3.   Planner / Binder::Bind(statement)
        └─ BindContext 解析 table / column / function（查 Catalog）
        └─ 產出 BoundStatement，內含 LogicalOperator 樹 + 結果型別
4.   Optimizer::Optimize(logical_plan)
        └─ 依序跑 RunBuiltInOptimizers 的多個 pass（見 §5）
5.   PhysicalPlanGenerator::CreatePlan(logical_plan)
        └─ LogicalOperator → PhysicalOperator 樹
        └─ ColumnBindingResolver 把 column binding 解析成實體 index
6.   Executor::Initialize(physical_plan)
        └─ 拆成 MetaPipeline → Pipeline → Event 圖
7.   TaskScheduler 派 Task 給 worker threads，push DataChunk 流經 operators
8.   QueryResult（MaterializedQueryResult 或 StreamQueryResult）回傳
```

DuckDB 在 `main/` 把「準備」與「執行」分階段，對應 `PendingQueryResult`（`src/main/pending_query_result.cpp`）：可先 prepare 取得 schema，再逐步 `ExecuteTask` 推進，最後拿 `QueryResult`。這支援 streaming 結果（`StreamQueryResult`）與可中斷執行（`src/parallel/interrupt.cpp`）。

### 對 arneb 的啟發

DuckDB 的 `PendingQueryResult` → 逐步 `ExecuteTask` 模型，與 arneb 用 `SendableRecordBatchStream` 做 async streaming 是同一哲學（不一次 materialize）。arneb 的 pgwire 層需要 streaming 回傳給 client，DuckDB 的 `StreamQueryResult` 證明了「pending → 逐 task 推進 → stream」能與 prepared statement / extended query protocol 自然共存。

---

## 4. SQL Parser 與 Analyzer / 語意分析

### 4.1 Parser（基於 PostgreSQL）

DuckDB **直接 fork PostgreSQL 的 grammar**（`third_party/libpg_query`），幾乎免費取得 PostgreSQL 等級的 SQL 相容性（Window Functions、CTE、巢狀子查詢等）。Parse tree 是 PostgreSQL 的 C 結構，再由一層 **Transformer**（`src/parser/transform/` 之下）轉成 DuckDB 自己的 C++ AST：

- `SQLStatement`（`src/parser/statement/`，如 `SelectStatement`、`InsertStatement`）
- `QueryNode`（`SelectNode`、`SetOperationNode` 等）
- `ParsedExpression`（`ColumnRefExpression`、`FunctionExpression`、`CaseExpression`…）
- `TableRef`（`BaseTableRef`、`JoinRef`、`SubqueryRef`…）

* **核心檔案**：`src/parser/parser.cpp`。
* **缺點與代價**：Postgres AST 的 C 介面繁瑣，DuckDB 必須維護一套龐大的轉換代碼（`src/parser/transform/`），把 `A_Expr`、`ResTarget` 等節點一一轉譯。`src/parser/peg/` 顯示 DuckDB 近年也引入了 PEG-based parser 路徑。

### 4.2 Binder / 語意分析與類型推導

`Binder`（`src/include/duckdb/planner/binder.hpp:202`，實作 `src/planner/binder.cpp`）是語意分析核心，把 parse tree 綁定到 catalog 中真實的 table/column：

- **`BindContext`**（成員見 `binder.hpp:214`）：維護目前 binding scope（哪些 table/column 可見），追蹤 correlated columns 與巢狀 binding。
- 多載 `Bind()` 依 statement 類型分派，如 `Bind(SelectStatement&)`（`binder.hpp:395`）、`Bind(InsertStatement&)`、`Bind(TableRef&)` 等。
- **Metadata 綁定**：遇到表名時透過 `CatalogEntryRetriever`（`binder.hpp:325`、`369`）查 catalog 取得真實的 `TableCatalogEntry`，並對該連線 Transaction 做可見性檢查（MVCC）。
- **Expression 綁定與型別推算**：`src/planner/expression_binder/` 針對不同 clause（WHERE、SELECT、GROUP BY、HAVING、ORDER BY、window）有各自的 binder，因為每個 clause 允許的 expression 種類不同（例如 aggregate 不能出現在 WHERE）。為每個運算元決定 `LogicalType` 並插入隱式轉換（implicit cast）。
- **Correlated Subquery 平坦化（Unnesting）**：相關子查詢的依賴列（correlate variables）在 binding 階段被收集，隨後利用 `LogicalDependentJoin` 在 planner 中消除，轉成標準 `Join`。（對應 optimizer 的 `Deliminator` pass，見 §5。）

**輸出**：`BoundStatement`，內含一棵 `LogicalOperator` 計畫與結果欄位型別。

### 對 arneb 的啟發

1. **clause-specific binder** 的設計（`expression_binder/` 下分 WHERE/SELECT/HAVING…）是處理「aggregate 只能在特定 clause、window 只能在 SELECT/ORDER BY」這類規則的乾淨方式。arneb 若在 planner 用單一 binding 函式塞滿 if/else，可考慮拆成 per-clause binder。
2. `BindContext` 統一管理 correlated column 與 scope——arneb 的 subquery（IN/EXISTS/scalar）支援若要更穩，需要類似的明確 scope stack。
3. **借用成熟 parser**：DuckDB 借 PostgreSQL grammar，arneb 借 `sqlparser-rs`。兩者都是「不要自己寫 grammar」的正確選擇。

---

## 5. 查詢規劃與最佳化

### 5.1 LogicalOperator

`LogicalOperator` 是邏輯計畫節點基底（`src/planner/logical_operator.cpp`），子類在 `src/planner/operator/`（如 `LogicalGet`、`LogicalFilter`、`LogicalProjection`、`LogicalJoin`、`LogicalAggregate`、`LogicalOrder`）。`LogicalOperatorVisitor`（`logical_operator_visitor.cpp`）提供 visitor pattern 讓各 optimizer pass 走訪/改寫樹。

### 5.2 Optimizer pass 完整順序（從 `Optimizer::RunBuiltInOptimizers` 核實）

> **修正說明**：agent 草稿曾回報 `optimizer.cpp` blob HTTP 404，並給出帶有編號跳號（缺 10、17、18、26-28…）的近似清單。本地該檔案 **存在且為 486 行**（`src/optimizer/optimizer.cpp`）。以下為 `RunBuiltInOptimizers()`（`optimizer.cpp:169`）中 **逐行核實的實際 pass 呼叫順序與行號**。注意：DuckDB 是 **rule-based + cost-based 混合**，並非單純 RBO。

| # | Pass（`OptimizerType`） | 作用 | 行號 |
|---|------------------------|------|------|
| 1 | `EXPRESSION_REWRITER` | 常數摺疊、表達式簡化（不改計畫結構） | `optimizer.cpp:188` |
| 2 | `CTE_INLINING` | 嘗試 inline CTE 取代 materialization | `optimizer.cpp:191` |
| 3 | `AGGREGATE_FUNCTION_REWRITER` | `AVG(x)→SUM(x)/COUNT(x)`、`SUM(x+C)→SUM(x)+C*COUNT(x)` | `optimizer.cpp:197` |
| 4 | `FILTER_PULLUP` | filter 先上拉 | `optimizer.cpp:203` |
| 5 | `FILTER_PUSHDOWN` | filter 下推（含 `CheckMarkToSemi`） | `optimizer.cpp:209` |
| 6 | `CTE_FILTER_PUSHER` | 推 filter 進 materialized CTE | `optimizer.cpp:217` |
| 7 | `REGEX_RANGE` | regex 轉 range filter | `optimizer.cpp:222` |
| 8 | `IN_CLAUSE` | IN 子句改寫 | `optimizer.cpp:227` |
| 9 | `DELIMINATOR` | 消除冗餘 DelimGet/DelimJoin（correlated subquery） | `optimizer.cpp:233` |
| 10 | `CTE_INLINING`（再次） | 第二次 CTE inline | `optimizer.cpp:239` |
| 11 | `EMPTY_RESULT_PULLUP` | 上拉空結果 | `optimizer.cpp:245` |
| 12 | `WINDOW_SELF_JOIN` | 部分 window 計算改成 self-join | `optimizer.cpp:251` |
| 13 | `PROJECTION_PULLUP` | 從 join 上拉 projection | `optimizer.cpp:257` |
| 14 | `OUTER_JOIN_SIMPLIFICATION` | FULL→LEFT/RIGHT→INNER（若 NULL 已被過濾） | `optimizer.cpp:263` |
| 15 | `JOIN_ORDER` ★ | **動態規劃 join 排序**（也把 cross product+filter 改寫成 join） | `optimizer.cpp:270` |
| 16 | `PARTIAL_AGGREGATE_PUSHDOWN` | join 下方預聚合 SUM/COUNT（GROUP BY 在維度欄、fact 側佔多數時） | `optimizer.cpp:277` |
| 17 | `JOIN_ELIMINATION` | 消除不影響結果的 join | `optimizer.cpp:282` |
| 18 | `UNNEST_REWRITER` | 把 DelimJoin 中的 UNNEST 移到 projection | `optimizer.cpp:288` |
| 19 | `UNUSED_COLUMNS` ★ | **column pruning**（移除未用欄位） | `optimizer.cpp:294` |
| 20 | `DUPLICATE_GROUPS` | 移除聚合的重複 group | `optimizer.cpp:300` |
| 21 | `COMMON_SUBEXPRESSIONS` | 抽取運算子內的共同子表達式 | `optimizer.cpp:306` |
| 22 | `COLUMN_LIFETIME` ★ | 建 projection map，提早投影掉未用欄位 | `optimizer.cpp:312` |
| 23 | `BUILD_SIDE_PROBE_SIDE` ★ | 決定 hash join 的 build/probe 邊 | `optimizer.cpp:319` |
| 24 | `COMMON_SUBPLAN` | 共同子計畫轉 materialized CTE（含 DML 防呆 `CTEContainsDML`） | `optimizer.cpp:328` |
| 25 | `LIMIT_PUSHDOWN` | LIMIT 推到 PROJECTION 下方 | `optimizer.cpp:335` |
| 26 | `ROW_GROUP_PRUNER` ★ | 用統計剪掉 row group | `optimizer.cpp:340` |
| 27 | `SAMPLING_PUSHDOWN` | sampling 下推 | `optimizer.cpp:346` |
| 28 | `TOP_N` | `ORDER BY + LIMIT` → TopN | `optimizer.cpp:352` |
| 29 | `LATE_MATERIALIZATION` ★ | **延遲物化大欄位** | `optimizer.cpp:358` |
| 30 | `STATISTICS_PROPAGATION` ★ | **統計傳播**（含 DML 防呆） | `optimizer.cpp:370` |
| 31 | `TOP_N_WINDOW_ELIMINATION` | `row_number window + filter` → aggregate | `optimizer.cpp:378` |
| 32 | `COMMON_AGGREGATE` | 移除重複 aggregate | `optimizer.cpp:384` |
| 33 | `COLUMN_LIFETIME`（再次） | 第二次 column lifetime 分析 | `optimizer.cpp:390` |
| 34 | `REORDER_FILTER` | 用表達式啟發式做初步 filter 重排 | `optimizer.cpp:396` |
| 35 | `PARTITIONED_EXECUTION` | 把 pipeline 切成 partition 再 union 回來 | `optimizer.cpp:402` |
| 36 | `JOIN_FILTER_PUSHDOWN` ★ | **runtime filter / sideways information passing** | `optimizer.cpp:408` |
| 37 | `ROW_NUMBER_REWRITER` | `ROW_NUMBER() OVER()` → row_number 虛擬欄 | `optimizer.cpp:414` |

> 與草稿的差異修正：(a) `CTE_INLINING` 與 `COLUMN_LIFETIME` 各 **出現兩次**（草稿只列一次）；(b) 補上草稿遺漏的 `SAMPLING_PUSHDOWN`、`UNNEST_REWRITER`、`TOP_N_WINDOW_ELIMINATION`、`PARTITIONED_EXECUTION`、`COMMON_AGGREGATE`；(c) agy 草稿把最佳化描述成「RBO 後接局部 CBO，Join Order 在 Projection Pushdown 前」的單向流程，實際順序是 `JOIN_ORDER`（#15）→ `UNUSED_COLUMNS`（#19）→ `COLUMN_LIFETIME`（#22），即 join 排序 **先於** 大部分 column pruning。

幾個對查詢引擎特別關鍵的 pass：

- **Filter Pullup → Filter Pushdown 兩階段**（`src/optimizer/pullup/`、`pushdown/`、`filter_pushdown.cpp`、`filter_combiner.cpp`）：先上拉再下推，中途用 `FilterCombiner` 合併/化簡多個 filter（例如 `a>5 AND a>3` 併成 `a>5`），再一次下推到最佳位置。
- **Join Order Optimizer**（`src/optimizer/join_order/`）：
  - `join_order_optimizer.cpp`：主流程
  - `plan_enumerator.cpp`：plan 枚舉（DP）
  - `query_graph.cpp` / `query_graph_manager.cpp`：以 query graph 表示 join 關係
  - `join_relation_set.cpp`：DP 的 relation 子集
  - `cardinality_estimator.cpp` + `cost_model.cpp` + `relation_statistics_helper.cpp`：基數估計與成本模型
  - 這是教科書級的 **bottom-up dynamic programming join enumeration**（DPsize / DPccp 風格）。統計來源 `BaseStatistics`（`src/storage/statistics/`）維護 min/max、null count、字串最大長度、以 HyperLogLog 估算的 distinct count（NDV）。
- **Statistics Propagation**（`statistics_propagator.cpp`）：把 base table 統計沿計畫樹往上傳，供 join order 與 row group pruning 使用。
- **Join Filter Pushdown**（`join_filter_pushdown_optimizer.cpp`）：runtime filter——build 端建好 hash table 後動態產生 filter 推回 probe 端掃描，剪掉不可能 match 的列。

### 對 arneb 的啟發

1. **這份 pass 清單本身就是 arneb optimizer 的 roadmap**。對照 arneb 已落地的 PredicatePushdown、ColumnPruning（`prune_for_columns`）、JoinReorder（Selinger DP）、partial/final aggregate——走的路與 DuckDB 高度一致。arneb 尚未有的高價值 pass：**Late Materialization**（#29，見 §9）、**Build/Probe Side Optimizer**（#23，用統計決定哪邊當 build）、**Join Filter Pushdown / runtime filter**（#36，對應 arneb 規劃中的 A1 dynamic filter，Q09 痛點）。
2. **Filter Pullup→Pushdown 兩階段 + FilterCombiner**：比直接遞迴下推更能化簡謂詞。
3. **每個 pass 都是獨立的 visitor，串成明確順序的 pipeline**——順序可控、可逐一測試。arneb 的 `LogicalOptimizer` 若用同樣的「pass 列表 + 明確順序」結構，會比一坨遞迴好維護，也符合 arneb 偏好的 per-query targeted fix。

---

## 6. 執行引擎模型

這是 DuckDB 效能核心，也是對 arneb 最有借鏡價值的一章：**Vectorized Push-based Execution（Morsel-Driven Parallelism）**。

### 6.1 向量化記憶體佈局：`DataChunk` 與 `Vector`

相較於每列呼叫一次虛擬函式的火山模型（Volcano Model），DuckDB 一次處理一批資料（一個 `DataChunk`）。

> **路徑修正**：agent 草稿曾把 vector size 常數歸到 `common/types/vector_size.hpp`，agy 已核實正確路徑為 **`src/include/duckdb/common/vector_size.hpp`**（非 `common/types/` 之下）。本地核實：
> - `#define DEFAULT_STANDARD_VECTOR_SIZE 2048U`（`src/include/duckdb/common/vector_size.hpp:16`）。
> - `STANDARD_VECTOR_SIZE` 若未預定義則採用 `DEFAULT_STANDARD_VECTOR_SIZE`（`vector_size.hpp:19-21`）。
> - 編譯期檢查 `STANDARD_VECTOR_SIZE` 必為 2 的次方，否則 `#error`（`vector_size.hpp:23-25`）。

```
DataChunk = 一組 Vector + 一個列數 count（預設批量 2048）
 +-------------------------------------------------------+
 | Vector 1: col_int32   (FLAT / 連續記憶體)              |
 +-------------------------------------------------------+
 | Vector 2: col_varchar (DICTIONARY / SelectionVector)  |
 +-------------------------------------------------------+
 | Vector 3: const_col   (CONSTANT / 單一值)             |
 +-------------------------------------------------------+
        ↑ ValidityMask 處理 NULL；SelectionVector 表達過濾後視圖（不複製資料）
```

- `DataChunk`（`src/common/types/data_chunk.cpp`）= 一組 `Vector` + 列數 `count`。等同 arneb 的 Arrow `RecordBatch`。
- `Vector`（`src/common/types/vector.cpp`，介面 `src/include/duckdb/common/types/vector.hpp`）：`LogicalType type` + `buffer_ptr<VectorBuffer> buffer`（主資料）+ auxiliary data + heap reference（給變長字串）。`UnifiedVectorFormat` 把任意 VectorType 攤平成統一存取格式供運算子讀取。`Flatten()` 可把任意壓縮型態轉成 `FLAT_VECTOR`（`vector.hpp:113`）。
- **VectorType**（核實：定義於 `src/include/duckdb/common/enums/vector_type.hpp:15`，**並非** 在 `vector.hpp` 內。草稿把此 enum 列在 `vector.cpp/vector.hpp` 是不精確的）。完整成員（`vector_type.hpp:15-22`）：
  - `FLAT_VECTOR`：標準未壓縮（每列一個值）
  - `FSST_VECTOR`：FSST 壓縮的字串資料
  - `CONSTANT_VECTOR`：整個 vector 只存一個常數值（節省記憶體與計算）
  - `DICTIONARY_VECTOR`：在另一個 vector 之上套 selection vector（字典/篩選視圖）
  - `SEQUENCE_VECTOR`：以 (start, increment) 表示等差序列（例如 row id）
  - `SHREDDED_VECTOR`：shredded variant vector
- Null 處理用 `ValidityMask`（`validity_mask.cpp`）；過濾用 `SelectionVector`（`selection_vector.cpp`）——**不複製資料，只改一層 `uint32_t` 索引**，後續運算子間接定址。

這套 **多 VectorType + selection vector + validity mask** 是向量化精髓：常數欄不重複存、過濾不複製、序列不展開。

### 6.2 Push-based pipeline + PhysicalOperator 三介面

舊式向量化引擎（如 MonetDB/X100）多用 Pull-Based（火山 + 向量化）；DuckDB 採 **Push-Based**。`PhysicalOperator`（`src/include/duckdb/execution/physical_operator.hpp`）同時可扮演三種角色：

| 介面 | 方法（核實行號） | 角色 | 範例 |
|------|------------------|------|------|
| **Operator** | `Execute() → OperatorResultType`（`physical_operator.hpp:101`） | 串流轉換（中段） | Filter、Projection |
| **Source** | `GetData() → SourceResultType`（`physical_operator.hpp:136`） | 產生資料（pipeline 起點） | TableScan、HashJoin 的 probe 結果 |
| **Sink** | `Sink() → SinkResultType`（`physical_operator.hpp:180`）、`Combine() → SinkCombineResultType`（`:184`）、`Finalize() → SinkFinalizeType`（`:191`） | 吸收資料（pipeline 終點） | HashJoin 的 build、Aggregate、Sort |

> **核實補充**：`OperatorResultType` 等狀態 enum **不在** `physical_operator.hpp` 定義，而是 `#include "duckdb/common/enums/operator_result_type.hpp"`（`physical_operator.hpp:16`）。該檔的 `OperatorResultType` 完整成員為 `{ NEED_MORE_INPUT, HAVE_MORE_OUTPUT, FINISHED, BLOCKED }`（`operator_result_type.hpp:27`，草稿漏了 `BLOCKED`——它代表運算子正在做 async I/O、暫不想被呼叫）。

狀態分兩層：

- `GlobalOperatorState` / `GlobalSinkState`：跨執行緒共享（如共享 hash table）
- `OperatorState` / `LocalSinkState`：執行緒本地（避免鎖競爭）

**Sink 三步驟協定** 是 morsel-driven 平行聚合/join 的核心：
1. `Sink()`：每個 worker 執行緒不斷餵入 input（寫進自己的 `LocalSinkState`，無鎖）。
2. `Combine()`：該執行緒 pipeline 結束時，把 local state 合併進 global state。
3. `Finalize()`：**所有** 執行緒結束後，單執行緒做最終整理（如 hash table 重組）。

另有 `CachingPhysicalOperator`（`physical_operator.hpp:276`）：把過小的 chunk 緩衝起來，避免一堆 < 2048 列的 chunk 拖垮 pipeline 效率。

### 6.3 表達式執行

`ExpressionExecutor`（`src/execution/expression_executor.cpp` + `expression_executor_state.cpp`）對整個 `DataChunk` 一次評估一個 expression（向量化），而非逐列。`adaptive_filter.cpp` 還會 **自適應重排 filter 謂詞順序**（把選擇率高/便宜的條件排前），是 runtime feedback 優化。

### 對 arneb 的啟發（最重要）

1. **Source/Operator/Sink 三介面 + Local/Global state 兩層** 是 push-based 平行執行黃金模式。arneb worker 內部執行若採用此模型，「無鎖 local state → Combine 合併 → 單執行緒 Finalize」能直接解決 arneb 反覆遇到的「partial/final aggregate 需分開 accumulator 型別」問題——DuckDB 的答案就是 `LocalSinkState`（partial）+ `Combine`（merge）+ `Finalize`（final）。
2. **明確的 `OperatorResultType` enum（含 `HAVE_MORE_OUTPUT`）** 讓 operator「一個 input 吐多個 output chunk」而不全部 materialize——這正是 arneb「per-batch probe streaming refactor 反覆失敗」想達到的效果。DuckDB 做法：operator 回傳 `HAVE_MORE_OUTPUT`，pipeline executor 再呼叫同一 operator（不前進 source），直到 `NEED_MORE_INPUT`。arneb 的 async stream 若能表達這個狀態，就能避免 collect-into-Vec 造成的 OOM。
3. **多 VectorType（尤其 CONSTANT / DICTIONARY）**：arneb 用 Arrow，Arrow 本身有 `DictionaryArray`、scalar/constant 概念。DuckDB 的 `Vector` 在執行中是 **可變的**，arneb 應全面擁抱 Arrow 的 **不可變性**（filter kernel 生成新 `RecordBatch`、底層共用 buffer，或自訂輕量 selection 包裹層）。確認 join/aggregate 是否善用 dictionary/constant 避免物化——Q09 那種 wide lineitem⋈orders 中間結果若 join key 能保持 dictionary/constant，記憶體可大幅下降。
4. **`CachingPhysicalOperator` 的小 chunk 合併**：arneb 若在 RepartitionExec / exchange 邊界產生很多小 batch，合併成接近 2048 列的批再往下送，能提升下游向量化效率。
5. **`adaptive_filter` runtime 重排謂詞**：arneb 已知「expression evaluator per-call overhead」是多次優化失敗根因；DuckDB 的解法是「向量化整批評估 + 對 `BinaryOp(Col,op,Col)` 用 compute kernel 直打」（arneb 的 F-Perf-RS 已做後者）。runtime 重排謂詞是 arneb 還沒做、低成本高回報的點。

---

## 7. 分散式執行：DuckDB 的「沒有分散式」與 arneb 的對照

**DuckDB 沒有分散式執行層。** `src/parallel/` 全是單機多執行緒。它用 morsel-driven parallelism「在一台機器內」榨乾所有核心，取代了傳統分散式引擎在單節點內的平行化需求。

```
              DuckDB（單機）                    arneb / Trino（分散式）
   ┌──────────────────────────────┐     ┌──────────────────────────────────┐
   │  Executor                    │     │  Coordinator                      │
   │   └ MetaPipeline             │     │   └ PlanFragmenter → Stages       │
   │       └ Pipeline             │     │       └ Fragment（分派給 worker） │
   │           └ Task (morsel)    │     │           └ worker 內：           │
   │                              │     │              Pipeline → Task      │
   │  TaskScheduler 派給          │     │  跨 worker：Flight RPC exchange   │
   │   本機 worker threads        │     │   (RepartitionExec / shuffle)     │
   │  共享記憶體，零序列化        │     │   序列化 + 網路傳輸               │
   └──────────────────────────────┘     └──────────────────────────────────┘
        morsel = 記憶體中的一塊            fragment 邊界 = 網路 shuffle 點
```

| 概念 | DuckDB（單機 Morsel-Driven） | arneb（分散式 Trino 替代品） |
|------|------------------------------|------------------------------|
| **並行單元** | `morsel`（記憶體中一塊資料區塊） | `partition`（worker 上 RepartitionExec 的 partition） / `Split`（儲存上的 Parquet 分片） |
| **「fragment」** | `MetaPipeline`（一組相連 pipeline） | `PlanFragmenter` 切出的 stage |
| **執行單元** | `PipelineTask` | `Fragment` / `StageTask`（運作於不同 worker） |
| **資料交換** | 共享記憶體指標傳遞（零拷貝） | `Exchange` / `Shuffle`（透過 Arrow Flight RPC、`OutputBuffer`） |
| **負載均衡** | `TaskScheduler` 動態派 morsel | 靜態 partition 分派（arneb `partition_count` 曾寫死 2） |
| **容錯** | 無（單機） | 需考慮 worker 失敗 |

### Shuffle/Exchange 在 Rust 分散式引擎中的實作考量

在 `arneb` 中，當 `HashJoin` 的資料分佈在不同 worker 上時需要 Shuffle。借鏡 DuckDB 的 `Pipeline` 劃分：在 Shuffle 邊界處切斷執行樹——前段一個 `Pipeline`，其 `Sink` 是 `FlightExchangeSinkOperator`（序列化並寫入本機緩衝區）；後段另一個 `Pipeline`，其 `Source` 是 `FlightExchangeSourceOperator`（從其他 worker 拉取資料）。因 arneb 是 Arrow-native，傳輸可直接用 Arrow Flight，與 DuckDB DataChunk 零拷貝哲學一脈相承：儘量減少編解碼開銷。

### 對 arneb 的啟發

1. **morsel-driven 的負載均衡 vs arneb 的靜態分割**：DuckDB 把資料切成遠多於核心數的小 morsel 動態派發，自然解決 data skew。arneb 反覆出現 Q09 的 skew/stall、`partition_count` 寫死 2、RepartitionExec first-batch 40s。**借鏡：arneb worker 內部可把每個 partition 再切成多個 morsel，由 worker 本地 task queue 動態派給核心**，而非「一個 partition 綁一個執行緒」。
2. **DuckDB 的 spill（§8）讓單機能處理超大資料**——正是 arneb 在受限記憶體 Docker 反覆 OOM 想要的能力。arneb fragment 在 worker 上跑時，應比照把 hash join / aggregate 設計成可 spill，而非靠擴大 cgroup。
3. **不要把 fragment 邊界切太細**：DuckDB 證明「能在單機共享記憶體解決的就別跨網路」。arneb Q09 痛點正是「wide 中間結果在 5 個 join level 被反覆 re-partition + re-exchange」——DuckDB 在單機完全沒有此成本。arneb 的 broadcast join（小表廣播免 shuffle）方向正確，要繼續推進。

---

## 8. 記憶體管理、資源控管與 spill-to-disk

對 OLAP 引擎而言，沒有嚴格記憶體管理，遇到大資料量 Join/Group By 時很容易 OOM 被系統強制終止。

### 8.1 BufferManager

`BufferManager`（`src/storage/buffer_manager.cpp`、`standard_buffer_manager.cpp`，介面 `src/include/duckdb/storage/buffer_manager.hpp`）是 out-of-core 能力的地基：

- **配置**：`AllocateMemory()` / `AllocateTemporaryMemory()` 回傳 `shared_ptr<BlockHandle>`（`buffer_manager.hpp:46`、`51`）；`Allocate()` 直接配置並 pin（`:54`、`:56`）。`BlockHandle` 代表一個實體區塊。
- **Pin / Unpin**：純虛擬 `Pin()`（兩個多載：僅 `shared_ptr<BlockHandle>`，以及額外帶 `QueryContext`，皆回傳 `BufferHandle`，`buffer_manager.hpp:58-59`）把 block 鎖在記憶體；純虛擬 `Unpin()`（接 `shared_ptr<BlockHandle>`、無回傳，`:64`）釋放使其可被驅逐。`BufferHandle`、`BlockHandle`、`BlockManager` 為其 friend class（`:26-28`）。
- **驅逐**：`BufferPool` + eviction queue，記憶體壓力時驅逐未 pin 的 block（LRU 風格）。
- **記憶體上限**：`SetMemoryLimit()`，驗證能否驅逐出足夠空間。
- **Spill / 暫存檔**：`WriteTemporaryBuffer()` / `ReadTemporaryBuffer()`、`SetTemporaryDirectory()`，把驅逐的 block 寫到磁碟暫存檔。
- **查詢級記憶體預留**：`TemporaryMemoryManager`（`buffer_manager.hpp:20` 前向宣告），讓單一查詢在多個運算子間協調記憶體配額。

### 8.2 Out-of-core hash join

`JoinHashTable`（介面 `src/include/duckdb/execution/join_hashtable.hpp:63`）是 linear-probing hash table，**內建外部 / 分割（partitioned）join**：

- **Radix partitioning**：以可調 radix bits 分割資料，追蹤 active/completed partitions。`SetRepartitionRadixBits()`（`join_hashtable.hpp:571`）依最大 HT 大小動態調整。
- **Probe spill**：巢狀 `ProbeSpill` struct（`join_hashtable.hpp:474`）把暫時無法處理的 probe 端資料物化到磁碟；註解明說「**若剩餘資料一輪就能處理完就不分割，否則 radix 分割**」（`join_hashtable.hpp:471-473`）。
- **多輪處理**：`PrepareExternalFinalize()` + `ProbeAndSpill()`（`:594-595`），HT 超過記憶體時迭代處理。
- **Load factor**：`DEFAULT_LOAD_FACTOR = 2.0`（`join_hashtable.hpp:516`），`EXTERNAL_LOAD_FACTOR = 1.5`（`:518`，外部 join 時控制密度，留更多空間）。

### 8.3 Out-of-core aggregate

`RadixPartitionedHashTable`（`src/execution/radix_partitioned_hashtable.cpp`）是平行 + 可 spill 的 group-by 聚合：把 group 依 hash radix bits 分割成多個 partition，每個 partition 的 HT 可獨立 spill。`PerfectAggregateHashTable`（`perfect_aggregate_hashtable.cpp`）是當 group key 範圍小且密（低基數整數）時的特化——直接用 key 當陣列索引，免 hash。

### 對 arneb 的啟發（直擊核心痛點）

1. arneb 的整個 Spill Phase 0–4 + Grace HJ 就是在重造 DuckDB 的 `JoinHashTable::ProbeSpill` + `RadixPartitionedHashTable`。**逐項對照**：(a) **radix partitioning 是 spill 基礎**——先按 hash radix 分割，每 partition 獨立決定是否 spill，而非整個 HT 一起 spill；(b) **external load factor 調低（2.0→1.5）**——外部 join 多輪時留更多空間；(c) **`ProbeSpill` 的「一輪能處理完就不分割」啟發式** 避免不必要分割。
2. **`TemporaryMemoryManager`（查詢級配額協調）** 正是 arneb `QueryMemoryPool` decorator 想做的事——讓同一查詢的多個運算子協調記憶體而非各自為政。DuckDB 把它做成獨立元件值得借鏡。
3. **`PerfectAggregateHashTable`（低基數特化）**：arneb 的 group-by 遇低基數 key（如 TPC-H `l_returnflag`、`l_linestatus` 只有幾種值，正是 Q01）用直接陣列索引免 hash 是巨大的 win。
4. arneb 「未追蹤的 Arrow 配置（Filter/Project/Repartition channel）溢出 cgroup」的根因在 DuckDB 不存在——因為 **DuckDB 所有大配置都過 BufferManager**，沒有「繞過記憶體追蹤」的路徑。arneb 若想根治 OOM，方向應是「讓所有大 Arrow buffer 都過一個統一的 MemoryPool」（DataFusion 風格 track-every-alloc），與 DuckDB BufferManager 哲學一致——arneb 自己也已得出同結論。Rust 可用 `Drop`/RAII 實現自動 `Unpin`（`BufferFrameGuard` 在 drop 時減 pin 計數），比 DuckDB 手動 `Unpin` 更安全。

---

## 9. 儲存與資料來源抽象

### 9.1 原生儲存格式：row group + column segment

DuckDB 自帶單文件列存格式（`src/storage/`、`src/storage/table/`）：

```
DuckDB Data File
 +------------------------------------------------------------+
 | Metadata Header                                            |
 +------------------------------------------------------------+
 | Row Group 0（預設 DEFAULT_ROW_GROUP_SIZE = 122,880 列）    |
 |   +------------------------------------------------------+ |
 |   | Column 0 Segment - 壓縮：BitPacking / FSST / …       | |
 |   +------------------------------------------------------+ |
 |   | Column 1 Segment - 壓縮：RLE / Dictionary / …        | |
 |   +------------------------------------------------------+ |
 +------------------------------------------------------------+
 | Row Group 1 …                                              |
 +------------------------------------------------------------+
```

- `DataTable`（`data_table.cpp`）是 table 的儲存層，由多個 **row group** 組成。
- **Row Group 預設大小**：核實 `#define DEFAULT_ROW_GROUP_SIZE 122880ULL`（`src/include/duckdb/storage/storage_info.hpp:26`），且編譯期要求其為 `STANDARD_VECTOR_SIZE` 的整數倍（`storage_info.hpp:394`）。利於粗粒度並行與 pruning。
- **Column Segment**：row group 內每列由多個 segment 組成，帶 min/max 統計（供 row group pruning）。
- **壓縮**（`src/storage/compression/`）：per-segment 輕量壓縮——RLE、Dictionary、BitPacking、FSST、ALP 等。
- **統計**（`src/storage/statistics/`）：per-segment / per-column 統計，餵給 §5 的 Statistics Propagation pass。

### 9.2 ART index（Adaptive Radix Tree）

DuckDB 主要次級索引是 **ART**（Leis et al. 2013），用於 unique/primary key constraint 與點查詢加速（`src/execution/index/art/`）：

- **節點型別依子節點數自適應**（核實 `enum class NType`，`src/include/duckdb/execution/index/art/node.hpp:21`）：`NODE_4`（`:24`）→ `NODE_16`（`:25`）→ `NODE_48`（`:26`）→ `NODE_256`（`:27`），另有葉節點 `NODE_7_LEAF`（`:29`）、`NODE_15_LEAF`（`:30`）、`NODE_256_LEAF`（`:31`），省記憶體。
  > 註：本地 checkout 的實作檔已合併為 `base_node.cpp` / `node48.cpp` / `node256.cpp`（無獨立的 `node4.cpp` / `node16.cpp`），但邏輯節點型別仍是 4/16/48/256 四階自適應。
- `prefix.cpp`：path compression（共同前綴壓縮）。
- `leaf.cpp` / `base_leaf.cpp` / `node256_leaf.cpp`：葉節點存 row id。
- ART 比 B+ 樹更適合記憶體型資料庫（快取命中率高、查找快）。

### 9.3 外部格式：Connector / TableFunction 與 pushdown

- **`TableFunction`** 是核心擴充點：任何外部資料來源實作 `bind` / `init` / `scan` 即可接入（CSV、Parquet、聯邦查詢）。
- **Pushdown**：當 `TableFunction` 宣告支援 filter / projection pushdown 時，optimizer 把謂詞傳入其 `bind`/`init`。讀 Parquet 時可用 metadata page statistics 跳過不符的 row group。
- `src/execution/operator/csv_scanner/`：state-machine based 平行 CSV 掃描（可切分大 CSV）。
- Parquet 透過 `parquet` extension 提供，支援 **projection pushdown**（只讀需要的 column）與 **filter pushdown / row group pruning**（用 min/max 統計剪 row group）。
- **Late Materialization**（optimizer pass #29，`LATE_MATERIALIZATION`，`optimizer.cpp:358`）：先只讀 join/filter 需要的 key 欄位，過濾完再回頭讀大的 payload（長字串、blob），大幅減少 I/O 與記憶體。

### 對 arneb 的啟發

1. **row group + column segment + per-segment 統計** 與 arneb 用的 Parquet 同構（row group / column chunk / page）。arneb 已做 row group pruning + predicate pushdown——方向一致。可再學 DuckDB 的 **column segment 級統計傳到 optimizer**（Statistics Propagation）做更積極剪枝。
2. **Late Materialization** 是 arneb 還沒做、且直擊 Q09 痛點的 pass：wide lineitem 不該在 join 前就把 30 欄全讀進來/搬過 exchange，應先只帶 join key + filter 欄，最後才回讀 payload。
3. **ART index** 對 arneb（OLAP 掃描為主）目前優先度低，但若未來要支援點查詢 / unique constraint，ART 比 B-tree 更省記憶體。
4. **平行 CSV scanner**（state-machine 可切分）：arneb 若要支援大 CSV，DuckDB 的 csv_scanner 是參考實作。

---

## 10. 並行模型與排程

`src/parallel/` 是 morsel-driven parallelism 的實作所在。

### 10.1 三層結構：MetaPipeline → Pipeline → Task

- **`Pipeline`**（`src/parallel/pipeline.cpp`，介面 `src/include/duckdb/parallel/pipeline.hpp:72`）：一條 `source → [operators…] → sink` 的線性鏈。存取子：`GetSource()`（`pipeline.hpp:121`）/ `GetSink()`（`:117`）/ `GetOperators()`（`:113`）。
- **`MetaPipeline`**（`meta_pipeline.cpp`，介面 `src/include/duckdb/parallel/meta_pipeline.hpp:22`）：管理一組相關 pipeline（一個 hash join 會切出「build pipeline」與「probe pipeline」，build 必須先完成）；`CreateChildMetaPipeline()`（`meta_pipeline.hpp:96`）、`GetPipelines()`（`:50`）管理依賴。
- **`Executor`**（`executor.cpp`）：從 physical plan 建出 MetaPipeline / Pipeline，用 **Event 圖** 表達相依（`pipeline_event.cpp`、`pipeline_finish_event.cpp`、`pipeline_complete_event.cpp`、`pipeline_initialize_event.cpp`）。Event 完成才觸發下游 Event——dataflow scheduling。在「阻斷算子」（如 hash table build 階段）處切斷，形成各 pipeline。

```
[Scan] -> [Filter] -> [HashJoin (Probe)]  --> (Pipeline B)
                           ^
                           | (依賴 Build 完成)
                      [HashJoin (Build)]  --> (Pipeline A)
                           ^
                           |
                        [Scan]
```

`Pipeline B`（probe）必須等 `Pipeline A`（build）完全 `Finalize` 後才開始。

### 10.2 TaskScheduler

`TaskScheduler`（`src/parallel/task_scheduler.cpp`，介面 `src/include/duckdb/parallel/task_scheduler.hpp`）：

- **任務佇列**：底層用 `unique_ptr<ConcurrentQueue> queue`（核實 agy：`task_scheduler.hpp` 內以 `ConcurrentQueue` 前向宣告，`:19`）；背景 worker threads（`SchedulerThread`，`:25`）。
- **`ProducerToken`**（`:27`）：thread-safe 提交 task 的 token；`CreateProducer()`（`:48`）、`ScheduleTask()`（`:50`）。worker 用 `GetTaskFromProducer()` / `ExecuteForever()` 執行。
- **執行緒數**：`SetThreads(total_threads, external_threads)`——啟動 `total - external` 個背景 worker，external thread（如主執行緒）也參與執行；`RelaunchThreads()` 可動態重配。
- **超時常數**：`TASK_TIMEOUT_USECS = 5000` 微秒（核實 agy：`task_scheduler.hpp:39`）。
- **CPU 親和性提示**：靜態 `GetEstimatedCPUId()` 取目前執行緒運作所在 CPU 編號（核實 agy：`task_scheduler.hpp:90`）。
- 一條 pipeline 透過 `Schedule(Event)` → `ScheduleParallel()` 把工作切成多個平行 task；`GetMaxThreads()` 決定可平行度。`RegisterNewBatchIndex()` / `UpdateBatchIndex()` 協調需保序場景的 batch 順序。

> **「work-stealing / morsel」表述修正**（agent 草稿與 agy 核實重點）：**`task_scheduler.hpp` 標頭檔內並未出現 "work-stealing" 或 "morsel" 字樣**（agy 已逐字確認）。DuckDB 的並行特性是 **中央 `ConcurrentQueue` + 多 consumer worker threads** 的 task queue，而非標頭明寫的「工作竊取」。其 morsel-driven 特性主要體現在 **source 把掃描切成多個 scan task（每個處理一塊 morsel）並動態派發**，而非標頭檔字面。因此本文不宣稱 DuckDB 用 work-stealing——它靠「task 顆粒夠細（morsel 遠多於核心）+ 中央 queue」即達成負載均衡。

### 對 arneb 的啟發

1. **MetaPipeline 用 Event 圖表達「build 必須先於 probe」依賴**：arneb 的分散式 stage 依賴本質相同——Event-driven dataflow scheduling 比「輪詢/sleep 等上游」乾淨。arneb RepartitionExec first-batch 40s、CPU idle 1.5% 的 stall，部分是「下游空等上游」——event-driven 觸發能改善。
2. **`SetThreads(total, external)` + external thread 參與執行**：呼叫者執行緒也跑 task，不浪費。arneb worker runtime 可比照。
3. **task queue + ProducerToken 的單機 work distribution** 對應 arneb 一個 worker 內部該有的東西——arneb 已刪掉 `task_manager` semaphore（Phase A），下一步「worker 內把 partition 切成多個 morsel 由 thread pool 動態取」正是這套模型。
4. **DuckDB 不靠 work-stealing（中央 queue + 多 consumer）也能負載均衡**——關鍵在 task 切細。arneb 不必上 work-stealing，把 partition 切細 + 中央 queue 即可。

---

## 11. 程式碼地圖：關鍵目錄與模組職責對照表

| 目錄 / 檔案 | 核心類別 / 職責 | 對應 arneb crate |
|-------------|------|------------------|
| `src/parser/` | `Parser` + `Transformer`：fork PostgreSQL `libpg_query`，轉 SQLStatement / ParsedExpression | `sql-parser` |
| `src/parser/{statement,query_node,expression,tableref}/` | AST 節點 | `sql-parser` AST |
| `src/planner/binder.cpp` + `binder/` | `Binder`：語意分析、綁定 catalog、相關子查詢消除 | `planner`（binding） |
| `src/planner/expression_binder/` | per-clause expression binder（WHERE/SELECT/HAVING…） | `planner` |
| `src/planner/logical_operator.cpp` + `operator/` | `LogicalOperator` 樹 | `planner` LogicalPlan |
| `src/optimizer/optimizer.cpp` | `RunBuiltInOptimizers`：37 個 pass 的 driver（RBO + CBO） | `planner` LogicalOptimizer |
| `src/optimizer/{pushdown,pullup}/`、`filter_pushdown.cpp`、`filter_combiner.cpp` | filter 下推/上拉 + combiner | `planner` PredicatePushdown |
| `src/optimizer/join_order/` | DP join 排序 + 基數估計 + 成本模型 | `planner` JoinReorder（Selinger DP） |
| `src/optimizer/statistics_propagator.cpp` | 統計傳播 | `planner`（NDV/統計） |
| `src/execution/physical_plan_generator.cpp` | `LogicalOperator → PhysicalOperator` | `execution`（physical planner） |
| `src/execution/physical_operator.cpp` + `operator/` | `PhysicalOperator`（source/operator/sink 三介面） | `execution` operators |
| `src/execution/operator/{join,aggregate,scan,order,projection,filter,set}/` | 各類實體運算子 | `execution`（scan/filter/join/aggregate/sort…） |
| `src/execution/expression_executor.cpp` | 向量化表達式評估 + adaptive filter | `execution` ScalarFunction / expr eval |
| `src/execution/join_hashtable.cpp` | 可 spill 的 partitioned hash join | `execution` HashJoinExec / Grace HJ |
| `src/execution/radix_partitioned_hashtable.cpp` | 可 spill 的平行聚合 | `execution` StreamingHashAggregate |
| `src/execution/index/art/` | ART 次級索引（NODE_4/16/48/256） | （arneb 尚無對應） |
| `src/common/types/{data_chunk,vector,validity_mask,selection_vector}.cpp` | 向量化資料結構（2048/批） | Arrow `RecordBatch` / `Array` |
| `src/common/vector_size.hpp` | `STANDARD_VECTOR_SIZE` / `DEFAULT_STANDARD_VECTOR_SIZE=2048U` | （Arrow batch size 慣例） |
| `src/common/enums/vector_type.hpp` | `VectorType`（FLAT/CONSTANT/DICTIONARY/SEQUENCE/FSST/SHREDDED） | Arrow array variants |
| `src/parallel/{pipeline,meta_pipeline}.cpp` | Pipeline / MetaPipeline | `planner` PlanFragmenter（部分） |
| `src/parallel/executor.cpp` | `Executor`：建 pipeline、Event 圖 | `server` / `scheduler` 協調 |
| `src/parallel/task_scheduler.cpp` | `TaskScheduler`：task queue + worker threads | `scheduler` NodeScheduler（單機版對應 worker 內部） |
| `src/storage/buffer_manager.cpp` | `BufferManager`：pin/unpin/驅逐/spill | arneb MemoryPool / QueryMemoryPool |
| `src/storage/table/`、`data_table.cpp` | row group / column segment 列存格式 | （arneb 用 Parquet，無自有格式） |
| `src/storage/{write_ahead_log,checkpoint_manager}.cpp` | WAL + checkpoint | （arneb 無持久化儲存） |
| `src/transaction/` | MVCC | （arneb 無） |
| `src/main/client_context.cpp` | `ClientContext`：查詢入口、交易 | `protocol` handler + `server` coordinator |
| `src/main/{pending_query_result,stream_query_result}.cpp` | pending / streaming 結果 | arneb `SendableRecordBatchStream` + pgwire |

---

## 12. 對 arneb（Rust 自建引擎）的具體啟發與可借鏡之處（總結）

把全文收斂成「可行動」清單，並標註與 arneb 既有工作的關係：

### A. 直接對應 arneb 當前痛點（Q09 / OOM）

1. **Late Materialization pass（DuckDB optimizer #29，`optimizer.cpp:358`）** —— arneb 還沒做，直擊 Q09「wide lineitem 中間結果搬過多個 exchange」。先只帶 join key + filter 欄過網路，最後回讀 payload。**最高優先、最高回報。**
2. **統一 MemoryPool 涵蓋所有大 Arrow 配置** —— DuckDB BufferManager 沒有「繞過追蹤」的路徑，不會被 Filter/Project/Repartition channel 的未追蹤配置撐爆 cgroup。arneb 應走 DataFusion 風格 track-every-alloc（arneb 已得同結論，本文佐證方向正確）。
3. **`PerfectAggregateHashTable`（低基數聚合特化）** —— Q01 的 `l_returnflag × l_linestatus` 是經典低基數 group，直接陣列索引免 hash，是 arneb Q01 潛在 win。
4. **`JoinHashTable::ProbeSpill` 的成熟設計** —— arneb Grace HJ / Spill Phase 在重造它。對照三細節：radix-partition-then-spill（非整體 spill）、external load factor 調低（2.0→1.5，`join_hashtable.hpp:516-518`）、「一輪能處理完就不分割」啟發式（`join_hashtable.hpp:471-473`）。

### B. 執行模型升級（worker 內部）

5. **Source/Operator/Sink 三介面 + Local/Global state 兩層** —— 直接解 arneb「partial/final aggregate 需分開 accumulator」的反覆困擾：`LocalSinkState`(partial) → `Combine`(merge) → `Finalize`(final)。
6. **`OperatorResultType::HAVE_MORE_OUTPUT` 狀態機（`operator_result_type.hpp:27`）** —— 讓 operator 一個 input 吐多個 output chunk 而不 materialize，正是 arneb「per-batch probe streaming」想要、但反覆失敗的能力。DuckDB 用「pipeline 重呼叫同 operator 直到 NEED_MORE_INPUT」實現。
7. **morsel-driven：worker 內把 partition 切成遠多於核心的小 morsel + 中央 task queue 動態派發** —— 緩解 arneb worker 內 data skew（Q09 stall）。不需 work-stealing，task 切細即可。
8. **Event-driven dataflow scheduling（MetaPipeline + Event 圖）** —— 取代「下游 sleep 等上游」，改善 arneb RepartitionExec first-batch 40s / CPU idle 的 stall。

### C. Optimizer 補強

9. **Filter Pullup→Pushdown 兩階段 + FilterCombiner**（`optimizer.cpp:203`、`:209`）—— 比單純遞迴下推更能化簡謂詞。
10. **Build/Probe Side Optimizer（#23）+ Join Filter Pushdown（#36，runtime filter）** —— 用統計決定 hash join build 邊；build 完動態產生 filter 推回 probe scan（arneb A1 dynamic filter 即此，Q09 痛點）。
11. **把 optimizer 組織成「明確順序的 pass 列表」** —— DuckDB 37 passes 各自獨立可測，符合 arneb 偏好的 per-query targeted fix。

### D. 架構層面的「不要做」

12. **不要把 fragment 邊界切太細** —— DuckDB 證明能在單機共享記憶體解決的就別跨網路。arneb broadcast join（小表廣播免 shuffle）方向正確；wide 中間結果反覆 re-partition 是 Q09 元兇。
13. **arneb 一個 worker 應該長成 DuckDB 的樣子** —— 最大 mental model 收穫：把 arneb 分散式問題拆成「跨 worker exchange 邊界（arneb 自己的事）」+「worker 內單機執行（照搬 DuckDB morsel-driven + push-based + spill）」兩層，各自借鏡最成熟的設計。

### Rust 實作要點（向量化與 spill 的權衡）

- **擁抱 Arrow 不可變性**：DuckDB `Vector`/`DataChunk` 在執行中可變；arneb 用 Arrow（不可變）。filter 應用 Arrow `filter` kernel（生成新 `RecordBatch`、底層共用 buffer），或自訂輕量 selection 包裹層（Rust 版 `SelectionVector`），跨節點 RPC 前才實體壓縮拷貝。
- **`Drop`/RAII 自動 Unpin**：DuckDB 手動 `Unpin`；Rust 可設計 `BufferFrameGuard`，drop 時自動減 pin 計數，安全不易出錯：
  ```rust
  struct BufferFrameGuard { block_id: BlockId, buffer_pool: Arc<BufferPool> }
  impl Drop for BufferFrameGuard {
      fn drop(&mut self) { self.buffer_pool.unpin(self.block_id); }
  }
  ```
- **Spill 控制**：HashJoin 溢寫時用 `tempfile` 管理臨時檔；`MemoryTracker` 回報超限時，依 hash 值分流寫入不同臨時檔（對照 DuckDB radix partitioning）。

---

## 驗證方法與來源

- **對照的本地原始碼**：`/Users/bochengyang/formosa-ventures/repos/duckdb`，`main` 分支，commit **`7e889c9168`**（`git rev-parse --short HEAD`）。
- **latest 回查（2026-06-05）**：已 fetch 並以 `origin/main` commit **`0ce48ae355b2`** 抽查。`Optimizer::RunBuiltInOptimizers()` 的 37-pass 順序仍吻合；`STANDARD_VECTOR_SIZE=2048`、`Binder`、`Executor`、`Pipeline`、`TaskScheduler`、`DataChunk`、`Vector` 等文件核心符號仍存在。本文行號未重標，不能視為 latest 精準行號。
- **引用方式**：凡帶 `相對路徑:行號` 者（如 `src/optimizer/optimizer.cpp:358`、`src/include/duckdb/common/vector_size.hpp:16`），皆指本地該 commit 之檔案；路徑相對於 duckdb repo 根目錄。
- **合併來源**：
  1. agy 版草稿 `docs/software-arch/duckdb-agy.md`（保留 12 章結構、ASCII 圖、Rust 範例、儲存格式圖）。
  2. agent 版草稿 `docs/software-arch/duckdb-agent.md`（保留逐節「對 arneb 啟發」、子系統圖、JoinHashTable/RadixPartitionedHashTable 細節、附錄）。
  3. agy 核實清單 `/tmp/verify_duckdb_agy.md`（vector_size.hpp 路徑修正、2048U 常數、task_scheduler 無 morsel/work-stealing 字樣、Pin/Unpin 簽名）。
- **本次新核實並補上 file:line 的關鍵事實**（超過 20 處）：
  - `DEFAULT_STANDARD_VECTOR_SIZE 2048U`（`common/vector_size.hpp:16`）+ 2 的次方編譯檢查（`:23-25`）；路徑修正（非 `common/types/`）。
  - **`Optimizer::RunBuiltInOptimizers` 完整 37-pass 順序與逐行行號**（`optimizer.cpp:169-418`）——agent 回報的 404 為誤，本地檔案存在（486 行）；補齊草稿遺漏的 5 個 pass、修正 CTE_INLINING / COLUMN_LIFETIME 各出現兩次。
  - `VectorType` enum 實際位置 `common/enums/vector_type.hpp:15`（非 `vector.hpp`），完整 6 成員（含 FSST_VECTOR、SHREDDED_VECTOR）。
  - `OperatorResultType` 來自 `common/enums/operator_result_type.hpp`（`physical_operator.hpp:16` include），完整 4 成員含 `BLOCKED`（`operator_result_type.hpp:27`）。
  - PhysicalOperator 三介面方法行號：`Execute`(`:101`)、`GetData`(`:136`)、`Sink`(`:180`)、`Combine`(`:184`)、`Finalize`(`:191`)；`CachingPhysicalOperator`(`:276`)。
  - `JoinHashTable`(`:63`)、`ProbeSpill`(`:474`、註解 `:471-473`)、`DEFAULT_LOAD_FACTOR=2.0`(`:516`)、`EXTERNAL_LOAD_FACTOR=1.5`(`:518`)、`SetRepartitionRadixBits`(`:571`)。
  - `BufferManager` Pin/Unpin/Allocate 簽名行號（`buffer_manager.hpp:46,51,54,56,58-59,64`）、friend class（`:26-28`）、`TemporaryMemoryManager`(`:20`)。
  - ART `enum class NType` 四階節點 NODE_4/16/48/256（`art/node.hpp:21-31`）+ 葉節點變體。
  - `DEFAULT_ROW_GROUP_SIZE 122880ULL`（`storage_info.hpp:26`）。
  - Pipeline `GetSource/GetSink/GetOperators`（`pipeline.hpp:121/117/113`）、`MetaPipeline`(`meta_pipeline.hpp:22`)、`CreateChildMetaPipeline`(`:96`)。
  - TaskScheduler `TASK_TIMEOUT_USECS=5000`(`:39`)、`GetEstimatedCPUId`(`:90`)、`ConcurrentQueue`(`:19`)、`ProducerToken`(`:27`)。
  - Binder `class Binder`(`binder.hpp:202`)、`Bind(SelectStatement&)`(`:395`)、`CatalogEntryRetriever`(`:325`)。
- **仍不確定 / 未逐檔展開之處（誠實標註）**：
  - `src/parser/transform/` 的 Transformer 逐檔節點對映未逐一打開（§4 依目錄存在 + 架構知識描述其角色）。
  - `src/storage/table/` 的 row group / column segment 內部 struct 佈局未逐欄核實行號（僅確認 `DEFAULT_ROW_GROUP_SIZE` 與目錄結構）。
  - §6 中 `GlobalSinkState` / `LocalSinkState` 兩層 state 的精確類別宣告行號未逐一核實（依 physical_operator.hpp 介面方法簽名推得其存在）。
  - DuckDB 各 optimizer pass 內部演算法細節（如 join_order 是 DPsize 或 DPccp 變體）依 DuckDB 公開論文/文件描述，未逐行追 `plan_enumerator.cpp` 實作。
