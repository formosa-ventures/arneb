# ClickHouse 分散式查詢引擎架構解析（經本地原始碼核實）與 arneb 借鏡指南

> 本文件由兩份既有草稿（`clickhouse-agy.md`、`clickhouse-agent.md`）合併而成，並逐一對照本地 checkout 的 ClickHouse `master`（commit `ab8cbaa2`）原始碼核實。2026-06-05 另以 latest `origin/master` commit `1b10470b19e5` 回查：Analyzer / QueryTree / Planner / IProcessor 主軸仍正確，但 QueryPlan optimization 清單已有新增項，本文已補註。所有載重事實標註 `相對路徑:行號`（相對於 ClickHouse 倉庫根目錄），行號仍以原始核實 commit 為準。讀者設定為正在用 Rust + Apache Arrow 自建 Trino 替代品（分散式 SQL 查詢引擎 **arneb**）的工程師；每章末附「對 arneb 的啟發」。
>
> 兩份草稿的關鍵錯誤已修正，詳見文末「## 驗證方法與來源」。其中最重要的兩點：動態改 pipeline 的狀態確為 **`UpdatePipeline`**（非 `ExpandPipeline`）；`ColumnReplicated` **確實存在**於現行 master（非抓取工具的幻覺命名）。

---

## 1. 專案定位與設計哲學

ClickHouse 是一套 **column-oriented（列式）OLAP 資料庫管理系統**，自帶儲存引擎（MergeTree 家族），而非像 Trino / arneb 那樣是純運算層（compute-only）的 federated query engine。這是它與 arneb 最根本的架構差異：ClickHouse 把「儲存」與「運算」緊密耦合，並圍繞 MergeTree 的物理佈局做大量最佳化（primary index、skip index、PREWHERE、read-in-order）。

### 1.1 核心設計哲學

* **硬體效率極大化（Hardware-Efficiency）**：設計出發點是「跑滿 CPU、記憶體頻寬與磁碟 I/O」。大量使用向量化執行（Vectorized Execution）與 CPU SIMD，而非如 Spark / Presto 偏好的動態程式碼生成（Codegen）。手寫且高度優化的 C++ 向量化核心，在實際場景能更穩定地榨乾 CPU 暫存器與快取。
* **儲存與計算緊密耦合（Tight Coupling）**：與計算儲存分離的聯邦引擎不同，ClickHouse 擁有原生儲存引擎 `MergeTree`。查詢引擎深度感知儲存層物理佈局（Data Parts、Granules、Sparse Index），在規劃階段就做極限 I/O 裁剪（Pruning）。
* **實用主義與局部優化**：不追求學術上的「優雅通用架構」，而是針對特定場景實作高度專門化演算法。例如針對不同基數（Cardinality）的 `GROUP BY` 用不同 Hash Table 實作（`AggregatedDataVariants` 中有 `key32`/`key64`/`key_string` 等數十種變體，並有對應的 `*_two_level` 版本，`src/Interpreters/AggregatedDataVariants.h:79-90`）。
* **分階段分散式聚合**：分散式查詢被切成 mergeable 的中間狀態，shard 先做 partial，coordinator 再 merge（見第 7 節）——這點與 arneb 的兩階段聚合直接對應。

### 1.2 技術權衡（Trade-offs）

* **犧牲併發度換取單次查詢極限速度**：預設把單一查詢拆到本機所有 CPU 核心。單次查詢極快，但整體並行查詢數（Concurrent Queries）受限。
* **弱化事務與隨機更新**：為極致讀取效能採用類 LSM-Tree 的 `MergeTree`，寫入 append-only 且分批；隨機 `UPDATE`/`DELETE` 透過非同步 `MUTATION` 實作，代價極高，不適合 OLTP。

> **對 arneb 的啟發**：ClickHouse 是「儲存耦合」的對照組。arneb 作為 federated engine 無法依賴自有 primary index，但「把 plan 階段的剪枝能力推到 connector」這個思路，正對應 arneb 已有的 Parquet row-group pruning / predicate pushdown。

---

## 2. 整體架構與核心組件

ClickHouse 近年最大的架構演進是引入 **Analyzer（QueryTree）** 取代舊的 syntax-analysis 路徑。現代查詢走 `Parsers → Analyzer → Planner → Processors`；舊路徑 `InterpreterSelectQuery`（拉取式 `IBlockInputStream` 時代殘留）仍在但逐步淘汰。

```
                    ┌──────────────────────────────────────────────┐
   SQL string  ───► │  src/Parsers   ParserQuery（遞迴下降）         │
                    │     ↓  IAST (ASTSelectQuery, ASTFunction…)     │
                    └──────────────────────────────────────────────┘
                                       │
                    ┌──────────────────▼───────────────────────────┐
                    │  src/Analyzer  QueryTreeBuilder               │
                    │     AST → QueryTree (IQueryTreeNode)           │
                    │     QueryAnalyzer: identifier resolution,      │
                    │     constant folding, alias/CTE substitution   │
                    │     QueryTreePassManager (IQueryTreePass)      │
                    └──────────────────┬───────────────────────────┘
                                       │  已 resolve 的 QueryTree（不再含 IdentifierNode）
                    ┌──────────────────▼───────────────────────────┐
                    │  src/Planner   Planner.cpp                    │
                    │     QueryTree → QueryPlan (IQueryPlanStep)     │
                    │     PlannerActionsVisitor → ActionsDAG         │
                    └──────────────────┬───────────────────────────┘
                                       │  QueryPlan
                    ┌──────────────────▼───────────────────────────┐
                    │  src/Processors/QueryPlan/Optimizations       │
                    │     filterPushDown, optimizePrewhere,         │
                    │     optimizeReadInOrder, joinOrder…           │
                    └──────────────────┬───────────────────────────┘
                                       │  QueryPipeline（IProcessor 圖）
                    ┌──────────────────▼───────────────────────────┐
                    │  src/Processors/Executors/PipelineExecutor    │
                    │     ExecutingGraph + ThreadPool 排程           │
                    │     資料以 Block / Chunk 在 Port 間流動         │
                    └──────────────────┬───────────────────────────┘
                                       ▼
                                    Result
   橫切：src/Common/MemoryTracker（per-query/user/global 階層式記帳）
         src/Interpreters/Context（查詢全域狀態、Settings）
```

### 2.1 核心組件職責對照（皆經本地原始碼核實）

| 組件 | 檔案（含關鍵行號） | 職責 |
|------|------|------|
| Parser | `src/Parsers/ParserQuery.h`、`IAST.h`、`ASTSelectQuery.h` | SQL → AST（`IAST` 節點樹） |
| Analyzer | `src/Analyzer/IQueryTreeNode.h:87`、`QueryTreeBuilder.h`、`Resolve/QueryAnalyzer.h:35` | AST → 已 resolve 的 QueryTree IR |
| Planner | `src/Planner/Planner.cpp`、`PlannerJoinTree.h`、`PlannerActionsVisitor.h` | QueryTree → QueryPlan + ActionsDAG |
| Query plan | `src/Processors/QueryPlan/IQueryPlanStep.h`、`AggregatingStep`、`JoinStep` | 邏輯/物理 plan step |
| 執行引擎 | `src/Processors/IProcessor.h:121`、`Executors/PipelineExecutor.h` | pull-based pipeline、多執行緒排程 |
| 列式記憶體 | `src/Columns/IColumn.h:98`、`src/Core/Block.h:30` | 向量化資料 |
| 聚合 | `src/Interpreters/Aggregator.h` | hash 聚合 + two-level + 外部聚合 spill |
| 儲存 | `src/Storages/MergeTree/MergeTreeData.h`、`StorageDistributed.h:45` | 物理儲存 + 分散式 |
| 記憶體 | `src/Common/MemoryTracker.h` | 階層式記憶體記帳與限制 |

---

## 3. 查詢生命週期：從 SQL 字串到結果

以一條 `SELECT … GROUP BY … ORDER BY …` 為例：

| 階段 | 步驟 | 核心類別 / 檔案 | 職責與產出 |
| :--- | :--- | :--- | :--- |
| **Parsing** | 1. 語法解析 | `src/Parsers/ParserQuery.h`（遞迴下降） | 解析 SQL 文字，產出 `ASTSelectQuery` 等 `IAST` 節點樹。 |
| **Analysis** | 2. 語意分析 | `src/Interpreters/InterpreterSelectQueryAnalyzer.h:15`、`src/Analyzer/Resolve/QueryAnalyzer.h:35` | `QueryTreeBuilder` 把 AST 轉成 `QueryTree`（所有 identifier 先是未解析的 `IdentifierNode`）；`QueryAnalyzer` 做 identifier resolution、型別推導、constant folding、alias/CTE 代換。**resolve 完的 QueryTree 不應再有 `IdentifierNode`**。 |
| **Planning** | 3. 生成計畫 | `InterpreterSelectQueryAnalyzer`（內含 `Planner planner`，`:92`）、`src/Planner/Planner.cpp` | 把 QueryTree 轉成 `QueryPlan`（由 `IQueryPlanStep` 組成）；`PlannerActionsVisitor` 把表達式編成 `ActionsDAG`，並生成跨分散式查詢一致的唯一名稱。 |
| **Optimization** | 4. 計畫優化 | `src/Processors/QueryPlan/Optimizations/optimizeTree.cpp` | 對 QueryPlan 套一系列 pass（filterPushDown、optimizePrewhere、optimizeReadInOrder、joinOrder…，見第 5 節）。 |
| **Compilation** | 5. 組裝 Pipeline | `InterpreterSelectQueryAnalyzer::buildQueryPipeline()` | 把最佳化後的 QueryPlan 轉成 `QueryPipelineBuilder`，再實例化為由 `IProcessor` 構成的執行圖、連好 Port。`execute()` 回傳 `BlockIO`。 |
| **Scheduling**| 6. 排程執行 | `src/Processors/Executors/PipelineExecutor.h` | 在 thread pool 上排程整張 processor 圖，依 `prepare()` 回傳的狀態決定下一步。 |
| **Execution** | 7. 向量化計算 | `src/Columns/`、`src/Processors/` | 執行緒對 `Ready` 的 processor 呼叫 `work()`，資料以 `Block`/`Chunk` 在 `Port` 間流動，最終透過協定回傳結果。 |

> **對 arneb 的啟發**：ClickHouse 把「parse / analyze（語意+resolve）/ plan / optimize / execute」切成五個清楚的階段，且 **analyze 產物（QueryTree）是可重用、可變更的 IR**（`InterpreterSelectQueryAnalyzer` 對外暴露 `query_tree`，`src/Interpreters/InterpreterSelectQueryAnalyzer.h:91`）。arneb 的 `Planner → LogicalPlan → Optimizer` 已有類似分層；值得借鏡的是把「語意解析」獨立成不可逆的 resolve 階段（不再有未解析 identifier），讓後續所有 pass 都能假設 schema 已綁定。

---

## 4. SQL Parser 與 Analyzer / 語意分析

### 4.1 Parser（`src/Parsers/`）

採 **遞迴下降（recursive descent）**，基底是 `IParser`（`src/Parsers/IParser.h`），AST 基底是 `IAST`（`src/Parsers/IAST.h`）。每種語句有對應的 parser 與 AST node（100+ 個 `AST*.h`），如 `ASTSelectQuery`、`ASTCreateQuery`、`ASTFunction`、`ASTIdentifier`、`ASTLiteral`。`ParserQuery` 是高階協調器，分派到各語句 parser。另含多方言子目錄（`MySQL/`、`Kusto/`、`PRQL/`、`Prometheus/`）與 `fuzzers/`。

> 舊版架構中 AST 不只是語法表示，還承載大量分析狀態，導致結構臃腫、複雜查詢（多表 JOIN、子查詢）直接改 AST 極易出錯——這正是引入 QueryTree 的動機。

### 4.2 新版 Analyzer 與 QueryTree（`src/Analyzer/`）

這是 ClickHouse 近年最重要的重構：引入現代編譯器常見的 IR 層 `QueryTree`，取代舊的、糾纏在 `InterpreterSelectQuery` 裡的 syntax-analysis 邏輯。

* **節點基底**：`IQueryTreeNode`（`src/Analyzer/IQueryTreeNode.h:87`，繼承 `TypePromotion<IQueryTreeNode>`）。指標別名 `QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>`（`:66`）。
* **節點型別**：以列舉 `QueryTreeNodeType`（`src/Analyzer/IQueryTreeNode.h:29-49`）表示，包含 `IDENTIFIER`、`COLUMN`、`TABLE`、`QUERY`、`UNION`、`FUNCTION`、`CONSTANT`、`LAMBDA`、`WINDOW`、`SORT`、`JOIN`、`ARRAY_JOIN` 等。對應的具體類別有 `QueryNode`、`UnionNode`、`TableNode`、`TableFunctionNode`、`JoinNode`、`ColumnNode`、`FunctionNode`、`ConstantNode`、`LambdaNode` 等。
* **分析流程**：
  1. `QueryTreeBuilder`（`src/Analyzer/QueryTreeBuilder.h`）走訪 `IAST` 建立初步 `QueryTree`，所有 identifier 先是未解析的 `IdentifierNode`。
  2. `QueryAnalyzer`（`src/Analyzer/Resolve/QueryAnalyzer.h:35`）做 identifier resolution（把 `IdentifierNode` 換成 `ColumnNode` / `TableNode` / 別名表達式）、型別推斷、constant folding、alias / CTE 代換。
  3. `QueryTreePassManager`（`src/Analyzer/QueryTreePassManager.h`）+ `IQueryTreePass`（`src/Analyzer/IQueryTreePass.h`）管理 tree 層級的最佳化 pass。
* **遍歷器兩種**：`InDepthQueryTreeVisitor`（無 context，README 明言「只有在你確定安全時才用」）與 `InDepthQueryTreeVisitorWithContext`（追蹤 subquery context，能正確處理未解析成分）。

> ⚠️ 修正：兩份草稿把這個解析器稱為 `QueryTreeBuilder` 或籠統的 `QueryAnalysisPass`。實際做 identifier resolution / 型別推斷 / constant folding 的類別名為 **`QueryAnalyzer`**（`src/Analyzer/Resolve/QueryAnalyzer.h:35`）；`QueryTreeBuilder` 只負責「AST → 初步 QueryTree」的建樹。

> **對 arneb 的啟發**：ClickHouse 用「**先 resolve 成不含 identifier 的 IR，再跑 pass**」的兩段式，把「名稱解析」與「邏輯改寫」徹底分離。Rust 的 `enum` + pattern matching 非常適合表示此類強型別 IR：
> ```rust
> // 概念設計示範
> pub enum QueryTreeNode {
>     Query(Box<QueryNode>),
>     Table(TableSource),
>     Column(ColumnIdentifier),  // resolve 後不應再有 Identifier 變體
>     Function(FunctionCall),
> }
> ```
> arneb 若曾在 optimizer pass 裡仍需處理「這欄到底指哪張表」的歧義，這個分離模式（resolve 一次性完成、pass 階段 schema 必定已綁）能消除大量隱性 bug。另外 `WithContext` 與非 `WithContext` 兩種 visitor 的明確區分，提醒「跨 subquery 邊界的改寫必須帶 scope」。
>
> arneb 用 `sqlparser-rs`，相當於把 parser 這層外包；ClickHouse 自寫 parser 換來「方言可插拔」與 fuzzing 友善，但維護成本高——對 arneb 而言維持外部 parser 是正確的 KISS 取捨。

---

## 5. 查詢規劃與最佳化

### 5.1 Planner（`src/Planner/`）

`Planner.cpp` 是「啟用 analyzer 時把 query tree 轉 executable plan 的主協調器」。關鍵檔案（皆經本地核實）：

| 檔案 | 職責 |
|------|------|
| `Planner.cpp/h` | 主協調器 |
| `PlannerJoinTree.cpp/h` | 處理 join tree |
| `PlannerExpressionAnalysis.cpp/h` | `buildExpressionAnalysisResult`，算出每個 step 後的 stream header |
| `PlannerActionsVisitor.cpp/h` | 從 query tree 表達式建 `ActionsDAG`，並生成跨分散式一致的唯一名稱 |
| `PlannerAggregation` / `PlannerSorting` / `PlannerWindowFunctions` / `PlannerJoins`(+`PlannerJoinsLogical`) | 各語意區塊的規劃 |
| `CollectSets` / `CollectTableExpressionData` / `CollectColumnIdentifiers` / `CollectMaterializedCTE` | 收集 plan 所需 metadata |
| `PlannerCorrelatedSubqueries` | 關聯子查詢 |

注意 **表達式以 `ActionsDAG`（有向無環圖）表示**，而非 tree——讓共同子表達式可共用、便於 pushdown。

### 5.2 QueryPlan 與最佳化 pass（`src/Processors/QueryPlan/`）

* **`QueryPlan`**：一棵由 `QueryPlanStep`（`src/Processors/QueryPlan/IQueryPlanStep.h`）組成的樹，分 `ISourceStep` / `ITransformingStep` 兩大類。常見 step：`ReadFromMergeTree`（及 `LazilyReadFromMergeTree`）、`FilterStep`、`ExpressionStep`、`AggregatingStep`、`JoinStep`、`LimitStep`、`DistinctStep`、`SortingStep`。
* **優化器性質**：ClickHouse 沒有 Calcite 式完整的 Volcano/Cascades 成本模型框架，主要是**規則為主（RBO）+ 局部啟發式**。

最佳化 pass 主要集中在 `Optimizations/`（多數是一檔一 pass）。以下是原始核實 commit 中最具代表性的 pass；2026-06-05 以 latest `origin/master` 回查時，該目錄已新增/擴充更多 pass，因此此清單不再宣稱是 latest 完整清單：

```
通用改寫:   filterPushDown  splitFilter  mergeExpressions  liftUpArrayJoin
            liftUpUnion  limitPushDown  removeRedundantSorting
            removeRedundantDistinct  removeUnusedColumns
排序/Top-K: applyOrder  optimizeReadInOrder  optimizeTopK  topKThroughJoin
MergeTree:  optimizePrewhere  optimizeLazyMaterialization
            optimizeLazyFinal  optimizeDirectReadFromTextIndex
Join:       joinOrder  optimizeJoin  optimizeJoinByShards  convertJoinToIn
            convertOuterJoinToInnerJoin  convertAnyJoinToSemiOrAntiJoin
            mergeFilterIntoJoinCondition  topKThroughJoin  joinRuntimeFilter
            partialJoinFilterPushDown
Projection: optimizeUseAggregateProjection  optimizeUseNormalProjection
其他 latest 顯著項: useVectorSearch  useDataParallelAggregation
            useMemoryBufferForCommonSubplanResult
```

幾個對自建引擎特別有啟發的 pass：
- **`optimizePrewhere.cpp`**：MergeTree 特有的「先讀 WHERE 用到的欄、過濾掉大部分 row、再讀其餘欄」，本質是 column-level 的 late materialization。
- **`optimizeReadInOrder.cpp`**：利用 part 已按 primary key 排序的事實，把 `ORDER BY`/`GROUP BY` 變成無需排序的 streaming。
- **`optimizeLazyMaterialization.cpp` / `optimizeLazyFinal.cpp`**：latest 中 lazy materialization / lazy FINAL 已成為 QueryPlan 二階段優化的一部分，會在 projection 類優化後再嘗試延遲讀取或延遲 FINAL 成本。
- **`useVectorSearch.cpp` / `optimizeDirectReadFromTextIndex.cpp`**：latest 增強了向量檢索與 text index 直接讀取相關路徑，顯示 ClickHouse 的 QueryPlan optimizer 正持續往 storage-aware / index-aware 特化方向演進。
- **`convertOuterJoinToInnerJoin.cpp` / `convertAnyJoinToSemiOrAntiJoin.cpp`**：join 型別降階——arneb 的 `SemiJoinToInnerJoin`、`PredicatePushdown` 正是同類 pass。
- **`joinOrder.cpp` / `optimizeJoinByShards.cpp`**：join 重排與「沿 shard 邊界做 join」。在 `JoinStep` 中 ClickHouse 也會依兩側預估資料量（Data Parts 統計）決定 Hash Join 或 Merge Join。

> **對 arneb 的啟發**：ClickHouse 把最佳化 pass **拆成數十個單一職責的 `.cpp`**（一個檔案一個 pass），符合 arneb CLAUDE.md 的 SOLID 與「小步提交」原則。arneb 已有 JoinReorder（Selinger DP）、PredicatePushdown、ColumnPruning；ClickHouse 的 `optimizeReadInOrder`（利用資料已排序省掉 sort）與 `topKThroughJoin`（把 limit/top-k 推過 join）是 arneb 可考慮新增的方向，尤其 read-in-order 對 Parquet 已排序檔案同樣適用。

---

## 6. 執行引擎模型：向量化 + pull-based pipeline

ClickHouse 已從舊版 Pull 模型（`IBlockInputStream`）全面遷移到基於資料流圖的 `IProcessor` 管道引擎。

### 6.1 列式記憶體：`IColumn`（`src/Columns/IColumn.h:98`）

`IColumn` 是列式記憶體核心抽象，繼承 `COW<IColumn>`（**copy-on-write**，`:98`），透過 `MutablePtr mutate(Ptr)` 與 `shallowMutate()` 在「不可變共享」與「可寫」間切換，靠 reference count 避免不必要複製。它是連續記憶體陣列，無 JVM 式物件指標開銷：`ColumnVector<T>` 本質是 `std::vector<T>`；`ColumnString` 由「緊湊拼接的字串位元組」+「偏移量 Offsets」兩個 vector 組成。

核心向量化虛擬方法（直接對應運算子需求，行號皆經本地核實）：

| 方法（位置） | 對應運算 |
|------|----------|
| `insertFrom` / `insertRangeFrom` | 串接 / 物化 |
| `filter(const Filter &, ssize_t hint)`（`:402`，原地版 `:406`） | WHERE / HAVING |
| `permute(const Permutation &, size_t limit)`（`:419`） | 排序 |
| `index(...)`（`:423`） | 依索引篩子集 |
| `compareColumn(...)`（`:472-474`） | 整列對單值的向量化比較 |
| `replicate(const Offsets &)`（`:542`） | ARRAY JOIN |
| `scatter(num_columns, const Selector &)`（`:549`，回傳 `VectorWithMemoryTracking<MutablePtr>`） | 依 selector 散到多 column（分割 / repartition） |
| `gather(ColumnGathererStream &)`（`:555`） | 垂直合併（merge）時把多來源重組回單一 column |
| `serializeValueIntoArena` / `deserializeAndInsertFromArena` | hash table key 序列化 |

具體型別：`ColumnVector`（數值）、`ColumnString`、`ColumnArray`、`ColumnNullable`、`ColumnConst`、`ColumnLowCardinality`、`ColumnSparse`，以及 **`ColumnReplicated`**（前向宣告於 `src/Columns/IColumn.h:33`；`convertToFullColumnIfReplicated()` 於 `:132-134` 把它惰性物化為 full column——是一種延遲展開的 lazy-materialization column）。`isFixedAndContiguous()`、`getRawData()` 等揭露記憶體佈局供向量化最佳化。

> ⚠️ 修正：agent 草稿懷疑 `ColumnReplicated` 可能是抓取工具的近似命名（幻覺）。**本地核實確認它真實存在**（`src/Columns/IColumn.h:33`、`:132-134`），與 `ColumnConst` / `ColumnLowCardinality` / `ColumnSparse` 同屬「需要時才 `convertToFull*` 展開」的惰性 column 家族。

### 6.2 資料流動單位：`Block`（`src/Core/Block.h:30`）

`Block` 是「一批 row 的 column 集合」，為資料處理的最小單元。內含 `Container data`（`ColumnWithTypeAndName` 的 vector，`:36`）與 `IndexByName index_by_name`（名稱→位置的 hash map，`:34`、`:37`）。關鍵方法：`getByPosition`（`:62`）、`getByName`（`:80`）、`rows()`（`:118`）、`columns()`（`:120`）、`insert`/`erase`、`dumpStructure()`。typical block 約 65,536 行（或 8,192）。在 processor 間流動時通常以 `Chunk`（`src/Processors/Chunk.h`）承載 column 資料（去掉 schema，schema 由 port header 攜帶）。

```
Block 結構示意圖:
┌────────────────────────────────────────────────────────┐
│ Column 1 (ColumnVector<Int32>): [ 1,  2,  3,  4,  5 ]  │  <-- 連續記憶體
├────────────────────────────────────────────────────────┤
│ Column 2 (ColumnString):       [ "a","b","c","d","e" ] │  <-- Offset + Byte Array
└────────────────────────────────────────────────────────┘
```

### 6.3 Pull-based pipeline：`IProcessor`（`src/Processors/IProcessor.h:121`）

ClickHouse **不是** 傳統 Volcano 逐 row `next()`。它是一張由 `IProcessor` 組成的圖，每個 processor 有 0..N 個 `InputPort` 與 0..N 個 `OutputPort`，資料以 block 透過 port 傳遞。

核心是 **`prepare()` / `work()` 兩段式狀態機**，`enum class Status`（`src/Processors/IProcessor.h:136-162`）：

```cpp
enum class Status : uint8_t {
    NeedData,        // :140  需要輸入；須先跑上游產生資料再呼叫 prepare()
    PortFull,        // :144  輸出 port 滿或不被 isNeeded()；須先把資料移到下游 input port（天然 back-pressure）
    Finished,        // :147  全部完成
    // Unneeded,     // :150  （原始碼中已註解掉，保留為文件記號）
    Ready,           // :153  可同步呼叫 work()
    Async,           // :157  回傳可 poll 的 fd，就緒後再呼叫 work()
    UpdatePipeline,  // :161  想動態增刪 processor；須以 updatePipeline() 取得變更
};
```

- **`prepare()`**（`:184`）：只做「便宜的計算」（O(1) 資料量、無等待），存取 port、拉輸入、推輸出，**不阻塞**；非執行緒安全，須由單一執行緒呼叫（即使是相連的不同 processor 也不可並行 prepare）。
- **`work()`**（`:196`）：只在 `prepare()` 回 `Ready` 後呼叫，**不可存取任何 port**，用 prepare 階段備好的資料做實際 CPU 運算；**不同 processor 的 work() 可並行**（即使相連）。
- **`schedule()`**（`:212`）：`prepare()` 回 `Async` 時呼叫，回傳 epollable fd；就緒後走 `onAsyncJobReady()` → `work()`。
- **`updatePipeline()`**（`:253`）：`prepare()` 回 `UpdatePipeline` 時呼叫，回傳 `PipelineUpdate { Processors to_add; Processors to_remove; }`（`:248-252`），用於執行期動態改圖（如依資料量決定平行度）。此方法不可搬移 port 資料、不可運算，呼叫後須再次 `prepare()`。

> ⚠️ 修正（重點之一）：agy 草稿完全未提動態改 pipeline 的狀態；agent 草稿寫成 `ExpandPipeline（原文 UpdatePipeline/ExpandPipeline）` 並列、未定版。**本地核實確認現行 master 的 enum 值是 `UpdatePipeline`（`:161`），對應方法 `updatePipeline()`（`:253`），回傳 `PipelineUpdate` 結構（`:248-252`）。沒有 `ExpandPipeline` 這個名字。**（`ExpandPipeline` 是 ClickHouse 歷史舊名，現行碼已改名。）此外 agent 草稿漏列了已被註解掉的 `Unneeded`（`:150`）。

reshaping / 基底 processor：`ISource`（只有輸出）、`ISink`（只有輸入）、`ISimpleTransform`、`IAccumulatingTransform`、`IInflatingTransform`；reshaping 用 `ResizeProcessor`、`ForkProcessor`、`ConcatProcessor`；運算子如 `LimitTransform`、`OffsetTransform`。

> **對 arneb 的啟發（重點）**：arneb 是 Rust async streaming（`SendableRecordBatchStream`），等價於 push/poll 混合的 Volcano-on-futures。ClickHouse 的 `IProcessor` 模型有兩個值得借鏡的點：
> 1. **`PortFull` 即 back-pressure**——arneb 近期在 `exec-exchange-backpressure` 上掙扎的 OutputBuffer 滿 / streaming deadlock 問題，本質上 ClickHouse 用「port 滿就回 `PortFull`、排程器不再 schedule 此 processor」優雅解決，且 `work()` **永不持有阻塞資源**（對照 arneb 的 `task_manager` semaphore 在 stream 生命週期內持有 permit 導致的 deadlock）。
> 2. **`prepare()`（cheap、不阻塞）與 `work()`（CPU-bound、不碰 port）的徹底分離**——讓排程與運算解耦。arneb 若想做 cooperative scheduling，這比 semaphore-gating 乾淨。設計時可避免裸 `async fn next()` 接口，改設計成類似 `Future::poll` 的無鎖狀態機，明確區分「CPU 計算工作」與「非同步 I/O 工作」。

---

## 7. 分散式執行

與 Trino 擁有固定 Coordinator / Worker 不同，ClickHouse 是**無中心（de-centralized）架構**：任何實例都能作為查詢發起者（Initiator），扮演 coordinator 角色。

### 7.1 `StorageDistributed`（`src/Storages/StorageDistributed.h:45`）

一張 Distributed table 是「橫跨多台 server」的虛擬表，本身不存資料、只引用每台 server 上的實體 database/table。關鍵成員（皆經本地核實）：

- `sharding_key_expr`（`ExpressionActionsPtr`）+ `sharding_key_column_name`（`getShardingKeyExpr()` `:144`、`getShardingKeyColumnName()` `:145`）：決定每 row 去哪個 shard。
- `static IColumn::Selector createSelector(ClusterPtr, const ColumnWithTypeAndName &)`（`:169`）：計算 shard 分派。
- `ClusterPtr getCluster()`（`:132`）：目標 cluster 設定；`getOptimizedCluster`（`:173`）、`skipUnusedShards`（`:179`）做分散式版 partition pruning。
- **`getQueryProcessingStage(...)`**（宣告 `:89`，實作 `src/Storages/StorageDistributed.cpp:462`）：決定查詢在 shard 上處理到哪個階段。

**查詢拆分流程**：Initiator 收到對 Distributed 表的查詢後，重寫為對各 shard 實體表的 local 查詢，經 ClickHouse Native Protocol（TCP）發送；各 shard 本地獨立執行並把（中間）結果回傳 Initiator 合併。

### 7.2 處理階段 enum 與兩階段聚合（two-phase GROUP BY）

`getQueryProcessingStage()` 的決策邏輯（`src/Storages/StorageDistributed.cpp:462-555`）：

- `distributed_group_by_no_merge == DISTRIBUTED_GROUP_BY_NO_MERGE_AFTER_AGGREGATION` → 依是否允許 limit 下推回傳 `WithMergeableStateAfterAggregationAndLimit` 或 `WithMergeableStateAfterAggregation`（`:503-508`）。
- `distributed_group_by_no_merge == 1` → `Complete`，各 shard 自主完整運算（`:519`）。
- `to_stage == WithMergeableState` → `WithMergeableState`（`:524-525`）。
- shard 數為 1 → `std::max(to_stage, Complete)`（`:529-537`）；shard 數為 0 → `FetchColumns`（`:538-544`）。
- 其他 → `getOptimizedQueryProcessingStage(Analyzer)`（`:547-555`）。

兩階段聚合是分散式 OLAP 核心，也是 arneb partial/final aggregation 的直接對照：

```
   shard 1            shard 2            shard 3
  ┌────────┐        ┌────────┐        ┌────────┐
  │ scan   │        │ scan   │        │ scan   │
  │ filter │        │ filter │        │ filter │
  │ PARTIAL│        │ PARTIAL│        │ PARTIAL│   ← WithMergeableState
  │ GROUP  │        │ GROUP  │        │ GROUP  │     （保留 aggregate 函式「狀態」，不算最終值）
  └───┬────┘        └───┬────┘        └───┬────┘
      └─────────────────┼─────────────────┘
                        ▼  (TCP, Native Protocol)
                ┌────────────────┐
                │  Initiator     │
                │  MERGE states  │   ← Aggregator::convertToChunks(final=false → true)
                │  FINAL GROUP   │
                │  ORDER/LIMIT   │
                └────────────────┘
```

`Aggregator::convertToChunks(AggregatedDataVariants &, bool final)`（`src/Interpreters/Aggregator.h:266`）的 `final` 旗標控制這點：`final=false` 保留 aggregate function 的中間狀態（state，如 `uniq` 的 HyperLogLog 結構）供分散式 merge；`final=true` 才算出具體結果。`WithMergeableStateAfterAggregation` 是「shard 連最終聚合都做完、只差跨 shard 合併」的進階階段。

*注意*：若中間狀態量過大，Initiator 會面臨單點記憶體瓶頸；ClickHouse 提供 `distributed_group_by_no_merge` 與把部分 merge 下推到特定 shard 來緩解。

### 7.3 寫入 sharding 與 exchange

- 寫入：`write()` 回傳 sink，用 sharding key 把 insert 分散到各 shard；`DistributedAsyncInsertDirectoryQueue` 在本地目錄暫存每 shard 待送資料，做可靠最終投遞。
- `skipUnusedShards`：條件允許時剪掉不需查的 shard。

> **對 arneb 的啟發**：`getQueryProcessingStage()` 三階段 enum（`WithMergeableState` / `WithMergeableStateAfterAggregation` / `Complete`）是「partial/final 切分」的成熟抽象。arneb 的 partial/final agg 曾因「需要分開的 accumulator 型別」受阻——ClickHouse 的解法是 **aggregate function 的 state 本身是 first-class、可序列化的型別**（`serialize`/`merge`/`finalize` 是 aggregate function 介面的一等方法），而非把 partial 與 final 當兩個運算子。arneb 若把 accumulator 的「合併中間態」與「輸出最終值」設計成同一型別的兩個方法（如 Trino 的 `Step` enum + 4-method Accumulator），就能避免雙型別問題。分散式 `exchange`/`shuffle`（Arrow Flight RPC）時，可在 Arrow `Schema` 用 `Binary` 型別封裝中間狀態（HLL / Bloom Filter），直到 coordinator `finalize` 才轉成 Int64。

---

## 8. 記憶體管理、資源控管與 spill-to-disk

OLAP 查詢極易 OOM。ClickHouse 實作了精細的階層化記憶體追蹤與外排機制。

### 8.1 `MemoryTracker`（`src/Common/MemoryTracker.h`）

**階層式記帳**：透過 `std::atomic<MemoryTracker *> parent`（`:90`）構成單向鏈結（thread → query → user → global），分配資訊向上傳播。核心欄位（皆在熱點 cache-line 結構中，皆經本地核實）：

- `std::atomic<Int64> amount`（目前用量，`:78`）、`peak`（歷史峰值，`:79`）、`rss`（RSS 追蹤，`:80`）。
- 三種 limit：`soft_limit`（`:84`）、`hard_limit`（`:85`）、profiler 相關。`hard_limit` 超過即丟 `MEMORY_LIMIT_EXCEEDED`。
- 修改方法：`setSoftLimit`（`:185`）、`setHardLimit`（`:186`）、`setOrRaiseHardLimit`（`:200`）。
- `allocImpl(Int64 size, bool enforce_memory_limit, ...)`（`:146`）/ `free()`，經 `CurrentMemoryTracker` 呼叫，回傳 `AllocationTrace`。
- `getResolvedSampleConfig()`（`:225-236`）沿 parent chain 解析抽樣設定；整合 jemalloc profiling。

關鍵在於 ClickHouse 重載全域 `operator new`/`delete`，**在 allocator 層攔截每筆 allocation**——連 Arrow-style 中間 buffer 都被計入，`hard_limit` 是全域硬牆。

### 8.2 外部聚合 / 外部排序（spill-to-disk）

**外部聚合** `Aggregator`（`src/Interpreters/Aggregator.h`）：
- 用量超過 `max_bytes_before_external_group_by`（`Params`，`:118`）時 spill 到磁碟：`tmp_data_scope`（`TemporaryDataOnDiskScopePtr`，`:121`）管理暫存檔，`writeToTemporaryFile(...)`（`:289`，內部 `writeToTemporaryFileImpl` `:451`）負責序列化。
- **two-level hash table**：用量超過 `group_by_two_level_threshold`（`:113`）/ `group_by_two_level_threshold_bytes`（`:114`）時，把資料切成多個 bucket（`AggregatedDataVariants` 中的 `*_two_level` 變體，`src/Interpreters/AggregatedDataVariants.h:79-90`），既利於平行也利於 spill（一次 spill 一個 bucket，記憶體曲線平滑）。

**外部排序**：`SortingStep`（`src/Processors/QueryPlan/SortingStep.cpp:150`）讀取 `max_bytes_before_external_sort` 設定，傳給 `MergeSortingTransform`；後者持有 `TemporaryDataOnDiskScopePtr tmp_data` 與 `max_bytes_in_block_before_external_sort` / `max_bytes_in_query_before_external_sort`（`src/Processors/Transforms/MergeSortingTransform.h:32-52`），記憶體超限即切換外部 merge sort，把已排序 chunk 寫暫存檔最後歸併。

> ⚠️ 補充（agent 草稿標為「未逐字驗證」）：外部排序的實作位置已核實——設定在 `SortingStep`、實際 spill 在 `MergeSortingTransform`（持有 `tmp_data`），不是 Aggregator。

> **對 arneb 的啟發（重點）**：arneb 的整個 spill 史（Phase 0→3b）痛點是「**untracked Arrow allocations 在 tracked-operator 觸發 spill 前就撐爆 cgroup**」。ClickHouse 不會有這問題，是因為它在 **allocator 層**（不是 operator 層）記帳——`hard_limit` 是全域硬牆，任何分配（含中間 buffer）都會被擋並丟 `MEMORY_LIMIT_EXCEEDED`，而非依賴個別 operator 自願 `try_grow`。arneb 記憶體筆記裡「DataFusion-style 追蹤每筆 alloc 比 custom GlobalAlloc 更 Rust-native」的結論與此方向一致：**記帳要下沉到分配點，而非散在運算子**。此外 two-level「按 bucket spill」也比 arneb 的 Grace HJ 整批 spill 更細緻。Rust 可透過 `#[global_allocator]` + thread-local QueryID + 原子更新 `HashMap<QueryID, AtomicU64>` 達成同類效果。

---

## 9. 儲存與資料來源抽象

ClickHouse 儲存層接口為 `IStorage`（`src/Storages/IStorage.h`），`MergeTree`、`StorageS3`、`StorageDistributed` 等皆實作之。`read()` 回傳資料流並接收 `SelectQueryInfo`（含 AST 與過濾條件）；`getQueryProcessingStage()` 宣告能下推到哪個階段。

### 9.1 MergeTree（`src/Storages/MergeTree/`）

MergeTree 是 immutable append-only：插入時資料按 primary key 排序寫成新 **part**，背景 `MergeTreeDataMergerMutator` 依啟發式 merge。`MergeTreeData`（`MergeTreeData.h`）管理表級結構；單一 part 由 `IMergeTreeDataPart`（`IMergeTreeDataPart.h:79`）表示，其生命週期受對應 `MergeTreeData` 限制。

```
table
 ├─ partition（依 partition key；不同 partition 的 part 不互相 merge）
 │   └─ part  (state: PreActive → Active → Outdated → Deleting)
 │        ├─ 各 column 的資料檔（.bin）
 │        ├─ primary index    ← using Index = Columns（IMergeTreeDataPart.h:91-92）
 │        │                      每隔 index_granularity row 取樣一個 PK 值
 │        ├─ .mrk marks       ← granule 的 seek 位置（可跳過 n*k row）
 │        ├─ MinMaxIndex      ← 分區鍵的 min/max 超矩形（IMergeTreeDataPart.h:358-387）
 │        └─ skip indexes     ← IMergeTreeIndex 次級索引
```

- **granule**：marks 檔定義的最小可跳過單位（`index_granularity`，型態 `MergeTreeIndexGranularityPtr`，`IMergeTreeDataPart.h:352`，預設 8192 row）。
- **primary index 常駐記憶體**：`Index = Columns`、`IndexPtr`（`IMergeTreeDataPart.h:91-92`），`getIndex` / `loadIndexToCache`（`:418-419`）。**稀疏索引**——只對每 granule 第一行取樣，極省索引記憶體。
- **skip index（跳數索引）**：`IMergeTreeIndex`（`src/Storages/MergeTree/MergeTreeIndices.h:242`）+ `IMergeTreeIndexCondition`（`:153`）計算查詢與 granule 的關聯性，可在不讀具體列資料下跳過不符的 granule（MinMax / Set / Bloom Filter）。
- **merge 模式**：`MergingParams` 支援 Ordinary / Collapsing / Replacing / Summing / Aggregating / Graphite。

查詢時的剪枝鏈：**partition pruning → primary index 範圍掃描 → granule skip（marks）→ skip index → PREWHERE column-level 過濾**。這是 ClickHouse 掃描快的根本。

> ⚠️ 修正：agy 草稿把 part 索引的 primary index 與 marks 寫成固定檔名 `primary.idx` / `column.mrk` / `column.bin`，並把背景合併寫成 `MergeTreeReaderIndex.cpp`。本地核實顯示載重型別是 `IMergeTreeDataPart` 內的 `Index = Columns`（`:91-92`）、`index_granularity`（`:352`）、`MinMaxIndex`（`:358-387`），skip index 在 `MergeTreeIndices.h`；檔名敘述保留為「概念示意」即可，類別/欄位以本核實為準。

### 9.2 `IStorage` 與 table functions

- `IStorage::read()` 自行解析下推條件，把過濾推到硬碟掃描階段；對外部源（MySQL/PostgreSQL/S3）會把 `WHERE` 重寫發給外部資料庫。
- `src/TableFunctions/`：`ITableFunction` + `TableFunctionFactory`，具體有 `TableFunctionRemote`（跨 server）、`TableFunctionMerge`、`TableFunctionFile`、`TableFunctionURL`、`TableFunctionFilesystem`，以及 `ITableFunctionCluster` / `*Cluster`（cluster 平行版）與 `Hive/` 子目錄。讓「臨時把外部資料當表查」成為一等公民。

> **對 arneb 的啟發**：arneb 沒有自有 MergeTree，但其剪枝層次完全對應 arneb connector pushdown：partition pruning ≈ Hive partition pruning、primary index/granule skip ≈ Parquet row-group 統計剪枝、PREWHERE ≈ late materialization（先讀 filter 欄、過濾出 row mask，再用 Arrow `take` 讀其餘投影欄；對 S3 connector 可省數倍頻寬）。ClickHouse 把這些做成 plan 階段的 `optimizePrewhere`/`optimizeReadInOrder` pass，arneb 可比照把「利用 Parquet 已排序 / row-group 統計」提升為**顯式 optimizer pass**，而非只在 connector 內隱式處理。`ITableFunction` + factory 也與 arneb 的 `ConnectorFactory`/`ConnectorRegistry` 同構。

---

## 10. 並行模型與排程

### 10.1 `PipelineExecutor`（`src/Processors/Executors/PipelineExecutor.h`）

- 接受一組 processor（port 全連好的完整圖），建成 `ExecutingGraphPtr graph`（`:84`），排程任務存 `tasks`（`:86`）。
- `ExecutingGraph`（`src/Processors/Executors/ExecutingGraph.h`）：圖節點為 `Node` 結構（`:70-119`），執行狀態以 `ExecStatus`（`Idle`/`Preparing`/`Executing`/`Finished`/`Async`，`:57-64`）表示；`Edge`（`:26-50`）代表 output→input port 連線，靠 `Port::UpdateInfo`（`:49`）做 port 狀態的版本追蹤與傳播。
- 用 `std::unique_ptr<ThreadPool> pool` 跨執行緒執行；`SlotAllocationPtr cpu_slots` 做 CPU slot 配額（concurrency control），動態 spawn 執行緒。
- task queue 是「帶 memory tracking 的 deque」，避免 queue 無上限成長造成 OOM。
- 兩種驅動：`execute(num_threads, concurrency_control)`（多執行緒跑整張圖）、`executeStep(std::atomic_bool * yield_flag)`（單執行緒增量執行，回 true 表示還要繼續——支援 cooperative / 互動式排程）。
- 執行狀態 `std::atomic<ExecutionStatus>`（NotStarted / Executing / Finished / Exception / CancelledByUser），`cancel()` / `cancelReading()` 支援外部中斷。

**排程本質**：排程器反覆對「`prepare()` 回 `Ready`」的 processor 呼叫 `work()`、對回 `NeedData`/`PortFull` 的暫停，圖的拓撲與 port 狀態驅動執行順序。`UpdatePipeline` 狀態允許執行期動態改圖。同一段 pipeline 可被複製成 N 條平行流（`ResizeProcessor` 做 fan-out/in），由共享 thread pool 抽 task 執行——即 **morsel-driven 風格**。為避免執行緒因非同步 I/O 被掛起，`Async` processor 會被移出 thread pool、註冊到 epoll/kqueue，I/O 就緒後重標 `Ready` 再塞回隊列。

> **對 arneb 的啟發**：arneb 用 tokio task + per-task semaphore，曾因 semaphore 在 stream 生命週期內持有導致 back-pressure deadlock。ClickHouse 從不讓 `work()` 持有阻塞資源（`PortFull` 就讓出），而 `executeStep` + `yield_flag` 提供 cooperative yield——正是 arneb「Phase C cooperative `yield_now()`」想要的模型。arneb 的「刪掉 `task_manager` semaphore」（Phase A）方向與 ClickHouse「不用 semaphore gating、靠 port 狀態自然 back-pressure」一致。

---

## 11. 程式碼地圖

| 目錄 / 檔案（含關鍵行號） | 職責 | 對應 arneb |
|------------|------|-----------|
| `src/Parsers/` (`ParserQuery.h`, `IAST.h`, `ASTSelectQuery.h`) | SQL → AST（遞迴下降） | `crates/sql-parser`（sqlparser-rs） |
| `src/Analyzer/` (`IQueryTreeNode.h:87`, `QueryTreeBuilder.h`, `Resolve/QueryAnalyzer.h:35`, `Passes/`) | AST → 已 resolve 的 QueryTree IR | `crates/planner`（AST→LogicalPlan）+ catalog resolution |
| `src/Planner/` (`Planner.cpp`, `PlannerJoinTree.h`, `PlannerActionsVisitor.h`) | QueryTree → QueryPlan + ActionsDAG | `crates/planner` QueryPlanner |
| `src/Processors/QueryPlan/` (`IQueryPlanStep.h`, `AggregatingStep`, `JoinStep`, `Optimizations/`) | plan step + 數十個最佳化 pass | `crates/planner` LogicalOptimizer / pass |
| `src/Processors/` (`IProcessor.h:121`, `ISource`, `ISink`, transforms) | pull-based pipeline 運算子 | `crates/execution` 物理運算子 |
| `src/Processors/Executors/PipelineExecutor.h`, `ExecutingGraph.h` | thread pool 排程整張 processor 圖 | tokio runtime + `crates/execution` 串流 |
| `src/Columns/` (`IColumn.h:98`, COW；`ColumnReplicated` 等惰性 column) | 列式記憶體抽象 | Apache Arrow `Array` / `ColumnarValue` |
| `src/Core/Block.h:30`, `src/Processors/Chunk.h` | 資料流動單位（一批列） | Arrow `RecordBatch` |
| `src/Interpreters/Aggregator.h` (`:266` convertToChunks, `:289` writeToTemporaryFile) | hash 聚合 + two-level + 外部聚合 spill | `crates/execution` aggregate + Grace HJ spill |
| `src/Processors/QueryPlan/SortingStep.cpp:150` + `Transforms/MergeSortingTransform.h:32-52` | 外部排序 spill | `crates/execution` SortExec / 外排 |
| `src/Interpreters/HashJoin/` | hash join | `crates/execution` HashJoinExec / SemiJoinExec |
| `src/Interpreters/InterpreterSelectQueryAnalyzer.h:15` | 串起 analyze→plan→pipeline | `crates/server` coordinator + `crates/protocol` handler |
| `src/Interpreters/Context.h` | 查詢全域狀態 + Settings | `crates/execution` ExecutionContext |
| `src/Storages/IStorage.h` + `MergeTree/IMergeTreeDataPart.h:79` | 儲存引擎 + 物理佈局 | `crates/connectors` DataSource / `crates/hive` |
| `src/Storages/StorageDistributed.h:45` (`getQueryProcessingStage` `.cpp:462`) | 分散式表、sharding、兩階段 | `crates/rpc` Flight + `crates/planner` PlanFragmenter |
| `src/TableFunctions/` (`ITableFunction`, `TableFunctionRemote`) | table function（外部源即時查） | `crates/connectors` ConnectorFactory |
| `src/Common/MemoryTracker.h` (`:90` parent, `:85` hard_limit) | 階層式記憶體記帳 + limit | `crates/execution` MemoryPool / QueryMemoryPool |

---

## 12. 對 arneb（Rust 自建引擎）的具體啟發與可借鏡之處

依「可立即行動」程度排序。

### 高槓桿（直接打到 arneb 已知痛點）

1. **Back-pressure 用「port 滿就讓出」而非 semaphore-gating。** `IProcessor` 的 `PortFull`（`IProcessor.h:144`）讓滿載運算子被排程器跳過，`work()` 從不持有阻塞資源。arneb 的 `exec-exchange-backpressure` 與 streaming deadlock（`task_manager` semaphore 在整個 stream 生命週期持有 permit）正是反例。借鏡：讓 exchange 滿載變成「排程器層級的可觀測訊號」，而非「持有 permit 阻塞」。對應 arneb 已規劃的 Phase A（刪 semaphore）。

2. **記憶體記帳下沉到分配點，而非個別運算子。** `MemoryTracker`（`src/Common/MemoryTracker.h`）在 allocator 層攔截每筆 alloc，`hard_limit`（`:85`）是全域硬牆——「untracked Arrow 中間 buffer 撐爆 cgroup」這個 arneb Q09 根因在 ClickHouse 不存在。借鏡：朝 DataFusion-style「每筆 alloc 經 MemoryReservation」收斂，而非靠運算子自願 `try_grow`。

3. **Aggregate function 的中間狀態是 first-class 可序列化型別。** 兩階段聚合靠 `Aggregator::convertToChunks(final=false)`（`Aggregator.h:266`）輸出 state、coordinator 再 merge state。arneb 曾因「partial/final 需分開 accumulator 型別」受阻——解法是把 accumulator 設計成「同一型別 + `serialize`/`merge`/`finalize` 方法」（Trino `Step` enum 模式），而非兩個運算子。

4. **two-level hash table「按 bucket spill」。** 比 arneb Grace HJ 整批 spill 更細緻：超過 `group_by_two_level_threshold`（`Aggregator.h:113`）就切 bucket，spill 時一次只 spill 一個 bucket（`AggregatedDataVariants.h:79-90`），記憶體曲線平滑。

### 中槓桿（最佳化 pass 可移植）

5. **`optimizeReadInOrder`**：利用資料已按某鍵排序（arneb 的 Parquet 檔可能已排序）省掉 `ORDER BY`/`GROUP BY` 的 sort，改 streaming。

6. **`optimizePrewhere`（late materialization）**：先讀 filter 欄、剪枝後再讀其餘欄——arneb 的 column pruning 已做「只讀需要的欄」，PREWHERE 是再進一步「先讀過濾欄、生成 row mask、用 Arrow `take` 讀其餘」。

7. **`topKThroughJoin` / `limitPushDown`**：把 limit/top-k 推過 join——arneb 曾嘗試 Q21 early-exit，這類 pass 是更系統化的做法。

8. **把每個 optimizer pass 拆成單一職責的檔案。** ClickHouse `Optimizations/` 一檔一 pass，符合 arneb CLAUDE.md 的 SOLID + 小步提交，利於測試與 review。

### 架構觀念（長期）

9. **語意 resolve 與邏輯改寫徹底分離。** QueryTree resolve 完「不再有 `IdentifierNode`」，後續所有 pass 都能假設 schema 已綁——消除「這欄到底指哪張表」的隱性歧義。

10. **`getQueryProcessingStage()` 三階段 enum**（`WithMergeableState`/`WithMergeableStateAfterAggregation`/`Complete`，`StorageDistributed.cpp:462-555`）是分散式下推程度的乾淨抽象；arneb 的 PlanFragmenter 可比照用一個 enum 明確標記「這個 fragment 在 worker 做到哪」。

### 不建議照搬

- **自寫 parser**：ClickHouse 換來方言可插拔，但維護成本高；arneb 用 sqlparser-rs 是正確的 KISS 取捨。
- **儲存耦合（MergeTree）**：arneb 是 federated engine，不該自建儲存引擎；但 MergeTree 的「plan 階段剪枝」思路應透過 connector pushdown 借鏡。
- **C++ COW（`COW<IColumn>`）**：Rust 用 Arrow 的 `Arc<dyn Array>` + 不可變 buffer 已是更安全的等價物，不需重造。`ColumnReplicated` 之類惰性 column 在 Arrow 也已有 `RunArray`(REE)/`DictionaryArray` 等對應，不需照抄 C++ 實作。

---

## 驗證方法與來源

* **核實對象**：本地 checkout 的 ClickHouse 倉庫 `master` 分支，commit **`ab8cbaa2`**（`/Users/bochengyang/formosa-ventures/repos/clickhouse`）。
* **latest 回查（2026-06-05）**：已 fetch 並以 `origin/master` commit **`1b10470b19e5`** 抽查。`QueryAnalyzer`、`IQueryTreeNode`、`Planner`、`IProcessor::Status::UpdatePipeline`、`ColumnReplicated` 等核心符號仍存在；QueryPlan optimization 目錄已有新項或擴充項（如 `useVectorSearch`、`optimizeLazyMaterialization2`、`optimizeDirectReadFromTextIndex`、`useDataParallelAggregation`、`removeUnusedColumns`、`optimizeLazyFinal`、`useMemoryBufferForCommonSubplanResult`），本文第 5.2 節已改為代表性清單。本文 file:line 未重標，不能視為 latest 精準行號。
* **引用方式**：全文載重事實標註 `相對路徑:行號`（相對於 ClickHouse 倉庫根目錄），以本地 `Read`/`grep` 直接驗證的符號為準。
* **合併來源**：
  - agy 版（`docs/software-arch/clickhouse-agy.md`）：保留其 12 章結構骨幹、ASCII 圖、MergeTree 讀取定位流程示意、arneb 借鏡的具體 Rust 程式碼片段。
  - agent 版（`docs/software-arch/clickhouse-agent.md`）：保留其更精確的目錄/類別命名（QueryTree/Planner/Optimizations pass 清單、IProcessor 狀態機、兩階段聚合與 `getQueryProcessingStage` 三階段）、附錄誠實標註的不確定點。
  - agy 核實清單（`/tmp/verify_clickhouse_agy.md`）：提供大量 file:line，已全數比對採用。

* **本次核實修正的關鍵錯誤**：
  1. **`UpdatePipeline`（非 `ExpandPipeline`）**：動態改 pipeline 的 `Status` 值是 `UpdatePipeline`（`src/Processors/IProcessor.h:161`），對應方法 `updatePipeline()`（`:253`）回傳 `PipelineUpdate { to_add; to_remove; }`（`:248-252`）。agent 草稿並列標註的 `ExpandPipeline` 是歷史舊名，現行 master 已不存在；agy 草稿則完全未提此狀態。同時補正 enum 含已被註解的 `Unneeded`（`:150`）。
  2. **`ColumnReplicated` 真實存在（非幻覺）**：前向宣告於 `src/Columns/IColumn.h:33`，`convertToFullColumnIfReplicated()` 於 `:132-134`。agent 草稿誤標為「可能是抓取工具近似命名」，本核實推翻此疑慮——它是 `ColumnConst`/`ColumnLowCardinality`/`ColumnSparse` 同族的惰性 column。
  3. **語意分析器類別名為 `QueryAnalyzer`**（`src/Analyzer/Resolve/QueryAnalyzer.h:35`），非草稿籠統稱的 `QueryTreeBuilder`（後者只做 AST→初步建樹）或 `QueryAnalysisPass`。
  4. **外部排序實作位置**：agent 草稿標為「未驗證、推論」；本核實確認在 `SortingStep`（讀 `max_bytes_before_external_sort`，`SortingStep.cpp:150`）+ `MergeSortingTransform`（持 `tmp_data`，`MergeSortingTransform.h:32-52`），非 Aggregator。
  5. **MergeTree 索引型別**：agy 草稿的固定檔名（`primary.idx`/`column.mrk`/`column.bin`）與 `MergeTreeReaderIndex.cpp` 改以核實的型別/欄位為準：`IMergeTreeDataPart` 的 `Index = Columns`（`:91-92`）、`index_granularity`（`:352`）、`MinMaxIndex`（`:358-387`）、skip index `IMergeTreeIndex`（`MergeTreeIndices.h:242`）。檔名敘述保留為概念示意。

* **補了 file:line 的關鍵事實（約 50+ 處）**：IProcessor 狀態機與各 Status 值、prepare/work/schedule/updatePipeline、ExecutingGraph Node/Edge/ExecStatus、Block 內部結構、IColumn 各向量化方法、ColumnReplicated、QueryTreeNodeType enum、QueryAnalyzer、InterpreterSelectQueryAnalyzer 內嵌 Planner、Aggregator 的 two-level 門檻 / 外部聚合 / convertToChunks、AggregatedDataVariants two-level 變體、MemoryTracker amount/peak/rss/soft/hard/parent/allocImpl、SortingStep + MergeSortingTransform 外排、IMergeTreeDataPart primary index/granule/MinMax、MergeTreeIndices skip index、StorageDistributed sharding 成員 + getQueryProcessingStage 全分支、Planner / Optimizations pass 檔案清單。

* **仍不確定 / 未逐字深入的點（誠實標註）**：
  - `Aggregator::convertToChunks` 的 `final` 旗標在分散式 merge 路徑的完整呼叫鏈（從 `StorageDistributed` 到 coordinator merge transform）未逐函式追蹤，僅確認 `final` 參數存在（`Aggregator.h:266`）與其語意。
  - MergeTree 的實體檔名（`primary.idx`/`*.mrk`/`*.bin`）為通用文件知識的概念示意，未在 `IMergeTreeDataPartWriter*` 逐一核對序列化路徑。
  - `StorageDistributed::createSelector` 與 sharding 權重/jump-consistent-hash 的細節未展開，僅確認簽名（`StorageDistributed.h:169`）。
  - Hive table function（`src/TableFunctions/Hive/`）的具體類別未逐一核實，僅確認子目錄存在。
  - `ColumnReplicated` 的完整實作檔（`src/Columns/ColumnReplicated.*`）未開啟細讀，僅由 `IColumn.h` 的宣告與 `convertToFullColumnIfReplicated` 確認其存在與用途。
