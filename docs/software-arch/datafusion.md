# Apache DataFusion 分散式 SQL 查詢引擎架構解析與 arneb 設計指南

> 本文件為針對 Apache DataFusion 核心架構與設計決策的技術解析，並特別為以 Rust + Apache Arrow 自建分散式 SQL 查詢引擎 [arneb](file:///Users/bochengyang/formosa-ventures/repos/arneb)（目標為 Trino 替代品）的工程團隊提供設計參考與架構權衡分析。
>
> **核實方法**：本文每一個關鍵事實都對照本地 checkout 的 DataFusion 原始碼（`main` 分支，commit `e71bd56`）逐項驗證，並標註 `相對路徑:行號`（相對於 `/Users/bochengyang/formosa-ventures/repos/datafusion`）。2026-06-05 另以 latest `origin/main` commit `e1d8d463b51e` 回查：logical optimizer 25 條與 physical optimizer 21 條仍吻合，主要 trait / crate 路徑仍存在；本文行號仍以原始核實 commit 為準。技術名詞、模組名、trait 名與檔案路徑一律保留英文原文。本文聚焦於 trait 邊界、`SendableRecordBatchStream` 串流背壓與 optimizer 擴充點 —— 因為 DataFusion 與 arneb 同屬 Rust + Arrow，這些面向最具直接借鏡價值。詳細的核實清單與不確定點見文末「驗證方法與來源」。

---

## 1. 專案定位與設計哲學

Apache DataFusion 是一個用 Rust 撰寫、基於 Apache Arrow 記憶體格式的「可嵌入式（Embeddable）」高效率查詢引擎。它與 arneb 的相似度遠高於 Trino：兩者皆建立於 **Rust + Apache Arrow columnar + async streaming** 之上。但兩者定位有關鍵差異。

| 面向 | DataFusion 核心 (`datafusion/*`) | arneb |
|------|----------------------------------|-------|
| 部署模型 | **單行程 (single-process) 函式庫** | 分散式 coordinator/worker |
| 分散式能力 | 不在核心；由獨立倉庫 `apache/datafusion-ballista` 提供 | 內建（Flight RPC + 自製 fragmenter） |
| 主要消費方式 | 被其他系統當函式庫嵌入（如 InfluxDB IOx、Ballista、Sail、GreptimeDB） | 獨立 server（pgwire 端點） |
| 介面標準 | PostgreSQL 相容性非核心目標 | pgwire v3 為一級目標 |

### 1.1 庫（Library）而非資料庫（Database）

DataFusion 的核心定位是**查詢引擎庫**，而非開箱即用的獨立資料庫。它不綑綁特定的儲存引擎、網路傳輸協定（如 PG Wire Protocol）或 metadata 存儲。這種設計哲學與 DuckDB 類似，但 DataFusion 透過 Rust 提供更強的模組化與可擴充性，使開發者能基於它建構各式專用系統。

### 1.2 從原始碼歸納的四條核心哲學

1. **Trait-first 可擴充性**：幾乎每一個延伸點都是 trait —— `TableProvider`、`ExecutionPlan`、`PhysicalExpr`、`OptimizerRule`、`AnalyzerRule`、`PhysicalOptimizerRule`、`MemoryPool`。使用者可以「只換掉一塊」而不必 fork 整個引擎。
2. **Arrow-native、pull-based streaming**：所有運算子的輸出都是 `SendableRecordBatchStream`（一個 `Stream<Item = Result<RecordBatch>>`），消費端「拉」資料，背壓自然由 async runtime 處理。
3. **不可變計畫樹 + 重寫（rewrite）而非就地修改**：`LogicalPlan` 與 `Arc<dyn ExecutionPlan>` 都是不可變樹；optimizer 透過 `Transformed<T>` 回傳「新樹 + 是否改變」的旗標。
4. **編譯階段嚴格分層**：SQL → AST →（bind）→ `LogicalPlan` →（analyze，可改語意）→（optimize，不改語意）→ `PhysicalPlan` →（physical optimize）→ execute。每一層只做一件事。

### 1.3 設計權衡（Trade-offs）

* **優點**：記憶體開銷低、多執行緒並行效率高、型態系統直接映射 Arrow、模組邊界清晰、擴充點極為豐富。
* **缺點**：高度抽象化使得特定硬體優化或與儲存層深度綁定時，中間抽象層（如 `TableProvider`）可能帶來轉譯開銷。由於缺乏全局資料控制權（無法直接控制底層磁碟頁快取），它高度依賴作業系統或檔案格式（如 Parquet）的快取機制。

對 arneb 而言，採用類似哲學能讓開發專注於分散式排程與執行層，而將單點向量化計算與記憶體管理交給成熟的 Arrow 生態系。

### 對 arneb 的啟發

DataFusion 核心刻意「不內建分散式」是一個值得反思的架構抉擇：把單行程引擎做到極致 trait 化，分散式作為**外掛層**重用同一套 `ExecutionPlan`。arneb 把分散式內建進核心，短期端到端整合方便，長期則增加每個運算子的複雜度（每個運算子都要顧分散式語意）。這個 trade-off 應在新增運算子時持續評估。

---

## 2. 整體架構與核心組件

DataFusion 將工作切成數十個 crate（`datafusion/` 目錄約 40 個子目錄）。核心資料流經過的 crate 與職責如下。

### 2.1 組件關係圖

```
                         ┌──────────────────────────────────────────────┐
   SQL 字串              │              datafusion/core                  │
      │                  │   SessionContext / SessionState (協調者)       │
      ▼                  └──────────────────────────────────────────────┘
┌───────────┐  AST   ┌────────────┐  LogicalPlan  ┌───────────────┐
│  sql       │──────▶ │  expr      │ ────────────▶ │  optimizer    │
│ SqlToRel   │        │ LogicalPlan│               │ AnalyzerRule  │
└───────────┘        │  Expr       │               │ OptimizerRule │
   (sqlparser-rs)    └────────────┘               └───────┬───────┘
                                                          │ 已最佳化 LogicalPlan
                                                          ▼
                                            ┌──────────────────────────┐
                                            │ core::physical_planner     │
                                            │ DefaultPhysicalPlanner     │
                                            └───────────┬──────────────┘
                                                        │ Arc<dyn ExecutionPlan>
                                                        ▼
                            ┌──────────────────────────────────────────────┐
                            │ physical-optimizer                            │
                            │ PhysicalOptimizerRule (EnsureRequirements...)  │
                            └───────────────────┬──────────────────────────┘
                                                │ 已最佳化 PhysicalPlan
                                                ▼
            ┌──────────────────────────────────────────────────────────────┐
            │ physical-plan: ExecutionPlan::execute(partition, TaskContext)  │
            │   ──▶ SendableRecordBatchStream（pull-based，tokio 驅動）        │
            └──────────────────────────────────────────────────────────────┘
                                                ▲
                  physical-expr (PhysicalExpr) ─┘    datasource / catalog (TableProvider)
                                                     execution (MemoryPool, TaskContext, RuntimeEnv)
```

### 2.2 關鍵 crate 與職責對照

| crate | 職責 | arneb 對應 |
|-------|------|-----------|
| `datafusion/sql` | `SqlToRel`：AST → `LogicalPlan`（binding，不做 coercion/optimize） | `crates/sql-parser` + `crates/planner` |
| `datafusion/expr` + `expr-common` | `LogicalPlan` enum、`Expr` enum、logical 型別、`ContextProvider`、`ColumnarValue` | `crates/planner`、`crates/common` |
| `datafusion/optimizer` | `AnalyzerRule`（改語意）+ `OptimizerRule`（不改語意） | `crates/planner` 的 `LogicalOptimizer` |
| `datafusion/physical-plan` | `ExecutionPlan` trait + 所有實體運算子（`*Exec`） | `crates/execution` |
| `datafusion/physical-expr` + `physical-expr-common` | `PhysicalExpr` trait（`physical-expr` re-export common 的定義） | `crates/execution` 的 ScalarFunction/表達式 |
| `datafusion/physical-optimizer` | `PhysicalOptimizerRule`（含 `EnsureRequirements`） | arneb physical 改寫 pass（PredicatePushdown / column-prune） |
| `datafusion/catalog` + `catalog-listing` | `TableProvider`、`ListingTable` | `crates/catalog`、`crates/connectors` |
| `datafusion/datasource-*` | parquet/csv/json/avro 資料源 | `crates/connectors`、`crates/hive` |
| `datafusion/execution` | `MemoryPool`、`TaskContext`、`RuntimeEnv`、`DiskManager` | arneb 的 `MemoryPool` / `ExecutionContext` |
| `datafusion/functions*` | scalar/aggregate/window/table UDF | `crates/execution` 的 19 個內建函式 |
| `datafusion/core` | 引擎上下文、內建物理算子實作、預設 Planner | `crates/server`、`crates/execution` |
| `datafusion/proto` | LogicalPlan/PhysicalPlan ↔ protobuf（序列化） | arneb 自製 Flight RPC plan 序列化 |

### 2.3 核心組件職責

1. **`SessionContext` / `SessionState`**：`SessionContext` 是使用者互動入口，管理查詢生命週期、配置與臨時狀態；`SessionState` 是引擎內部的「大腦」，持有所有 `AnalyzerRule`、`OptimizerRule`、`PhysicalOptimizerRule`、UDF 註冊表、`CatalogProvider` 引用與 `RuntimeEnv`（含 `MemoryPool`）。
2. **`SqlToRel`（datafusion-sql）**：將 sqlparser 解析出的 AST 轉換成 `LogicalPlan`，僅做 binding。
3. **`Optimizer`（datafusion-optimizer）**：以 RBO 為主對 `LogicalPlan` 做等價變換。
4. **`PhysicalExpr` 與 `DefaultPhysicalPlanner`**：前者定義物理表達式求值；後者生成物理算子並驅動執行期。

### 對 arneb 的啟發

`SessionState` 把 catalog、optimizer、執行環境收斂成單一注入點，等同於 arneb 的 `ExecutionContext`。建議對照確認 arneb 的 `ExecutionContext` 是否同樣集中持有 optimizer pass 清單與 `MemoryPool`，避免散落多處導致配置不一致。

---

## 3. 查詢生命週期：從 SQL 字串到結果的完整流程

以 `SELECT a, SUM(b) FROM t WHERE c > 1 GROUP BY a` 為例：

```
SQL String
   │ [1: sqlparser-rs]
   ▼
AST (Statement)
   │ [2: SqlToRel — sql/src/planner.rs；查 ContextProvider 解析表名/欄名]
   ▼
Raw LogicalPlan
   │ [3: Analyzer (AnalyzerRule) — optimizer/src/analyzer；TypeCoercion 插入 CAST，可改語意]
   ▼
Analyzed LogicalPlan
   │ [4: Optimizer (OptimizerRule) — optimizer/src/optimizer.rs；25 條規則，不改語意]
   ▼
Optimized LogicalPlan
   │ [5: DefaultPhysicalPlanner — core/src/physical_planner.rs]
   ▼
Raw ExecutionPlan
   │ [6: PhysicalOptimizerRule — physical-optimizer/src/optimizer.rs；21 條規則]
   ▼
Optimized ExecutionPlan
   │ [7: execute(partition, TaskContext)]
   ▼
SendableRecordBatchStream (消費端 pull；背壓由 tokio 處理)
```

| 階段 | 輸入 | 輸出 | 核心元件/Trait | 職責 |
| :--- | :--- | :--- | :--- | :--- |
| **1. 解析** | `&str` | `Statement` | `sqlparser::Parser` | SQL 字串 → AST |
| **2. 轉譯 (binding)** | `Statement` | `LogicalPlan`(Raw) | `SqlToRel` (`datafusion/sql/src/planner.rs:454`) | AST → 邏輯關係代數；查 `ContextProvider` 解析表名/欄名 |
| **3. 語意分析** | `LogicalPlan`(Raw) | `LogicalPlan`(Analyzed) | `AnalyzerRule` / `TypeCoercion` (`datafusion/optimizer/src/analyzer/type_coercion.rs:68`) | 型態推導、隱式轉換、欄域名解析；**可改語意**，失敗即報錯 |
| **4. 邏輯優化** | `LogicalPlan` | `LogicalPlan`(Optimized) | `Optimizer` / `OptimizerRule` (`datafusion/optimizer/src/optimizer.rs:83`) | 等價變換：列剪裁、謂詞下推、子查詢展開 |
| **5. 物理規劃** | `LogicalPlan` | `ExecutionPlan`(Raw) | `DefaultPhysicalPlanner` (`datafusion/core/src/physical_planner.rs:261`) | 邏輯算子 → 物理算子 |
| **6. 物理優化** | `ExecutionPlan` | `ExecutionPlan`(Optimized) | `PhysicalOptimizerRule` (`datafusion/physical-optimizer/src/optimizer.rs`) | 分區/排序強制、Join 演算法選擇 |
| **7. 執行** | `ExecutionPlan` | `SendableRecordBatchStream` | `ExecutionPlan::execute` (`datafusion/physical-plan/src/execution_plan.rs:475`) | 啟動非同步 pull 串流鏈 |

### 對 arneb 的啟發

DataFusion 把「binding（解析名稱與型別）」與「coercion / optimize」徹底分到不同階段。arneb 若把這三件事混在同一個 AST→LogicalPlan 轉換裡，將難以單獨測試與推理。對照 arneb 的 `crates/planner` 確認 binding 路徑是否能被 pg_catalog metadata 查詢共用。

---

## 4. SQL Parser 與 Analyzer / 語意分析

### 4.1 SqlToRel（binding）

來源：`datafusion/sql/src/planner.rs:454`（`pub struct SqlToRel<'a, S: ContextProvider>`）。`SqlToRel` 只做兩件事：

1. **Name and type resolution（"binding"）**：透過 `ContextProvider` 查表名/欄名。
2. **AST translation**：把 sqlparser 的 `Statement`/`Query` 翻成 `LogicalPlan`。

它**刻意不做** type coercion 與 optimization。`ContextProvider`（`datafusion/expr/src/planner.rs:44`）是 binding 的注入點：提供 `TableSource`、UDF/UDAF/UDWF 定義、config、type planner、系統變數型別等。這是 DataFusion 把「parser 與 catalog 解耦」的核心 trait。

### 4.2 Analyzer 與 type coercion

在 DataFusion 早期版本中，語意校驗與邏輯優化混在 Optimizer 階段。現代架構將其拆為 `Analyzer` 與 `Optimizer` 兩個獨立階段：

* **`AnalyzerRule`**（`datafusion/optimizer/src/analyzer/type_coercion.rs:94` 為 `impl AnalyzerRule for TypeCoercion`）：
  * **核心契約**：**允許改變 `LogicalPlan` 的語意**（如插入 `CAST` 對齊型別），也允許拋出 `Result::Err`。
  * **任務**：驗證查詢語意。`TypeCoercion`（`type_coercion.rs:68`）是最重要的 `AnalyzerRule`，其遞迴走訪計畫、由 inputs 建 schema，再用 `TypeCoercionRewriter` 在以下情境插入 `CAST`：binary operation 兩側型別對齊、join 等值條件左右 schema 對齊、`CASE` 的 WHEN/THEN 型別統一、subquery 型別匹配、window frame bounds。數值型別「向上」coerce（Int8→Int64），不相容組合直接報 planning error 而非靜默。
* **`OptimizerRule`**：
  * **核心契約**：**必須產生等價結果**（不改語意），假設輸入的 `LogicalPlan` 在語意上已 100% 正確，唯一目標是優化效能；**不應**拋出使用者層面的語意錯誤。

### 對 arneb 的啟發

把「會改語意的重寫」（型別強制、`*` 展開）與「等價重寫」（pushdown、消除）拆成兩個 trait / 兩個 pass，是非常值得抄的紀律。arneb 已將 [AnalysisPass](file:///Users/bochengyang/formosa-ventures/repos/arneb/crates/planner/src/analyzer/mod.rs#L432) 與 [LogicalRule](file:///Users/bochengyang/formosa-ventures/repos/arneb/crates/planner/src/optimizer.rs#L13) 分離，方向正確。實作 [correlated_exists_to_leftjoin.rs](file:///Users/bochengyang/formosa-ventures/repos/arneb/crates/planner/src/analyzer/correlated_exists_to_leftjoin.rs) 等複雜關聯子查詢展開時，務必確保語意錯誤在 AnalysisPass 就被阻斷；進入 LogicalRule 的計畫必須是「保證正確但可能不夠快」的狀態。若把 PredicatePushdown 與型別處理混在同一 pass，將難以推理正確性。

---

## 5. 查詢規劃與最佳化

### 5.1 LogicalPlan

來源：`datafusion/expr/src/logical_plan/plan.rs:207`（`pub enum LogicalPlan`）。主要變體：

```
Projection, Filter, Aggregate, Window, Sort, Join, Repartition,   // 關聯運算
TableScan, EmptyRelation, Values,                                  // 資料源
Union, Distinct,                                                   // 集合運算
Limit, Subquery, SubqueryAlias, Unnest, RecursiveQuery,            // 進階
Explain, Analyze, Statement, Dml, Ddl, Copy, DescribeTable,        // 管理/DDL
Extension                                                          // 使用者自訂節點
```

值得注意兩點：(1) `Repartition` 在 **logical** 層就存在 —— DataFusion 允許 logical 層表達分區意圖；(2) `Extension` 變體讓使用者塞入自訂 logical 運算子而不需 fork enum，是 trait-first 哲學在 enum 上的妥協方案。

### 5.2 OptimizerRule 與預設規則集

來源：`datafusion/optimizer/src/optimizer.rs:83`。trait 與容器：

```rust
pub trait OptimizerRule: Debug {
    fn name(&self) -> &str;                                    // optimizer.rs:85
    fn apply_order(&self) -> Option<ApplyOrder> { None }       // optimizer.rs:91
    fn rewrite(&self, plan: LogicalPlan, config: &dyn OptimizerConfig)
        -> Result<Transformed<LogicalPlan>, DataFusionError>;  // optimizer.rs:135
}

pub struct Optimizer { /* rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> */ }  // optimizer.rs:255
```

兩個設計亮點：

* **`Transformed<T>`**：`rewrite` 回傳 `Transformed::yes(plan)`（已改）或 `Transformed::no(plan)`（未改）。Optimizer 迴圈以此判斷是否到達 fixpoint，避免無限迭代。
* **`apply_order`（`ApplyOrder`）**：規則自己宣告 top-down 或 bottom-up 套用，框架負責走訪，規則本身不必手寫遞迴。

預設 **25 條** `OptimizerRule`（依序，`datafusion/optimizer/src/optimizer.rs:290`）：

```
 1 RewriteSetComparison          10 ExtractEquijoinPredicate    19 PushDownFilter
 2 OptimizeUnions                11 EliminateDuplicatedExpr      20 SingleDistinctToGroupBy
 3 UnionsToFilter                12 EliminateFilter              21 EliminateGroupByConstant
 4 SimplifyExpressions           13 EliminateCrossJoin           22 CommonSubexprEliminate
 5 ReplaceDistinctWithAggregate  14 EliminateLimit               23 ExtractLeafExpressions
 6 EliminateJoin                 15 PropagateEmptyRelation       24 PushDownLeafProjections
 7 DecorrelatePredicateSubquery  16 FilterNullJoinKeys           25 OptimizeProjections
 8 ScalarSubqueryToJoin          17 EliminateOuterJoin
 9 DecorrelateLateralJoin        18 PushDownLimit
```

> 關鍵邏輯規則：`PushDownFilter`（謂詞下推到 TableScan）、`OptimizeProjections` / `PushDownLeafProjections`（列剪裁）、`SimplifyExpressions`（布林代數簡化，如 `WHERE 1=1`）、`DecorrelatePredicateSubquery` / `ScalarSubqueryToJoin`（子查詢去關聯）。arneb 已實作的 PredicatePushdown / column-pruning 分別對應 `PushDownFilter` / `OptimizeProjections`。

### 5.3 PhysicalPlanner（Logical → Physical）

來源：`datafusion/core/src/physical_planner.rs:123`（trait）、`:261`（`DefaultPhysicalPlanner`）。

```rust
pub trait PhysicalPlanner: Send + Sync {
    async fn create_physical_plan(&self, logical_plan: &LogicalPlan, session: &SessionState)
        -> Result<Arc<dyn ExecutionPlan>>;                     // physical_planner.rs:125
    fn create_physical_expr(&self, expr: &Expr, schema: &DFSchema, session: &SessionState)
        -> Result<Arc<dyn PhysicalExpr>>;                      // physical_planner.rs:137
}
```

`DefaultPhysicalPlanner` 的做法值得學：先把 logical 樹 DFS 攤平成 vector，再 **bottom-up 並行**建構（即「規劃本身就平行化」，`create_initial_plan` 於 `:434`）。逐節點映射要點：

* `Filter` → `FilterExec`、`Projection` → `ProjectionExec`
* `Aggregate` → **兩階段** `AggregateExec`：先 `AggregateMode::Partial`（`physical_planner.rs:1144`）再 `AggregateMode::FinalPartitioned`（`:1165`）
* `Join` → `HashJoinExec`（含 `CollectLeft`/`Auto` partition mode）/ `SortMergeJoinExec` / `NestedLoopJoinExec` / `CrossJoinExec`
* `Sort` → `SortExec`、`Window` → `WindowAggExec` / `BoundedWindowAggExec`

### 5.4 物理優化與 CBO

物理優化器基於統計（`Statistics`：`num_rows`、`total_byte_size`、各欄 `ColumnStatistics` 含 `null_count`/min/max/NDV）做決策。`JoinSelection` 規則在右表估算記憶體低於閾值時改用 broadcast 風格（不對右側重新分區，直接收集到 build 端記憶體）。

> **latest 回查補註（2026-06-05）**：`origin/main` commit `e1d8d463b51e` 中，物理優化器仍維持 21 條規則，但擴充點更明確：`PhysicalOptimizerContext` 可把 `ConfigOptions` 與可選的 `StatisticsRegistry` 傳給規則；`JoinSelection` 已可透過 `StatisticsRegistry` 做跨 operator 的統計估算；`FilterPushdown` 在 physical optimizer 中分為 pre-optimization 與 post-optimization 兩階段，後者用於處理可能引用 source `ExecutionPlan` 的 dynamic filters。這些補強不改變本文「trait-first、rule-based physical optimizer」的主結論。

### 對 arneb 的啟發

DataFusion 在 **physical planner 就把 aggregate 切成 Partial / Final 兩個 `AggregateExec`**，這正是 arneb 已落地的 partial/final agg。對照確認 arneb 的切法（用不同 accumulator 型別）與 DataFusion 一致是正確方向。此外 `Transformed<T>` + `apply_order` 模式可消除 arneb 中手寫遞迴的改寫 pass —— 讓規則只宣告 top-down/bottom-up，框架負責走訪與 fixpoint 收斂。

---

## 6. 執行引擎模型

DataFusion 採用 **Pull-based 向量化串流執行模型（Vectorized Stream Execution Model）**。

### 6.1 ExecutionPlan trait

來源：`datafusion/physical-plan/src/execution_plan.rs:94`。這是整個引擎最核心的 trait：

```rust
pub trait ExecutionPlan: Any + Debug + DisplayAs + Send + Sync {
    fn name(&self) -> &str;                                   // execution_plan.rs:102
    fn schema(&self) -> SchemaRef { ... }                     // :143 (有預設實作，取自 properties)
    fn properties(&self) -> &Arc<PlanProperties>;             // :152（ordering/partitioning/equivalence 快取）
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;       // :226
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>)
        -> Result<Arc<dyn ExecutionPlan>>;                    // :230

    // 執行：對某個 partition 回傳一條 async 串流（無 push、無 callback，全是 pull）
    fn execute(&self, partition: usize, context: Arc<TaskContext>)
        -> Result<SendableRecordBatchStream>;                 // :475

    // 規劃約束（給 physical optimizer 用，皆有預設實作）
    fn required_input_distribution(&self) -> Vec<Distribution> { ... }       // :166
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> { ... } // :179
    fn maintains_input_order(&self) -> Vec<bool> { ... }                     // :199

    // 其他帶預設實作的可選方法
    fn benefits_from_input_partitioning(&self) -> Vec<bool> { ... }          // :213
    fn repartitioned(&self, target: usize, config: &ConfigOptions)
        -> Result<Option<Arc<dyn ExecutionPlan>>> { ... }                    // :277（自我增加平行度）
    fn metrics(&self) -> Option<MetricsSet> { ... }                          // :492
    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> { ... } // :500
    fn check_invariants(&self, check: InvariantLevel) -> Result<()> { ... }  // :160
    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> { ... } // :255
}
```

> 注意：`execute` **不是** `async fn`，而是同步回傳一個 `SendableRecordBatchStream`（內部的 `poll_next` 才是非同步的）。許多坊間文獻誤寫成 `async fn execute` —— 本地原始碼證實它是同步簽名。

設計重點，與 arneb 高度相關：

1. **`execute(partition, context)` 是一切的核心**：傳入「要哪一個 partition」與 `TaskContext`（含 `MemoryPool`、config），回傳一條 `SendableRecordBatchStream`。**全是 pull，沒有 push，沒有 callback**。
2. **`properties()` 回傳快取的 `PlanProperties`**：把昂貴的 partitioning / output ordering / equivalence / emission type / boundedness 一次算好快取，physical optimizer 反覆查詢不重算。
3. **規劃約束與執行分離**：`required_input_distribution` / `required_input_ordering` 是給 optimizer 看的「我需要 children 怎麼分區/排序」；optimizer 據此插入 `RepartitionExec` / `SortExec`。運算子本身不負責滿足這些約束。
4. **`repartitioned()` 讓運算子自我增加平行度**：optimizer 問「你能變成 N 個 partition 嗎」，運算子回傳新計畫或 `None`。

#### PlanProperties 結構

來源：`datafusion/physical-plan/src/execution_plan.rs:1068`：

```rust
pub struct PlanProperties {
    pub eq_properties: EquivalenceProperties,    // 欄位等價/排序語意
    pub partitioning: Partitioning,              // 輸出分區
    pub emission_type: EmissionType,             // 增量 vs final
    pub boundedness: Boundedness,                // 有限 vs 無限串流
    pub evaluation_type: EvaluationType,
    pub scheduling_type: SchedulingType,
    output_ordering: Option<LexOrdering>,        // 輸出排序（私有欄位）
}
```

#### SendableRecordBatchStream

```rust
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

pub trait RecordBatchStream: Stream<Item = Result<RecordBatch>> {
    fn schema(&self) -> SchemaRef;
}
```

每次 `poll_next` 不像傳統 Volcano 模型只回傳單一 tuple，而是回傳一個 Arrow `RecordBatch`（通常 1024–8192 行）。

```
+----------------------------------------------------------+
|                       Volcano 模型                        |
|  next() -> Tuple   (每次僅一行，虛擬函數調用開銷極大)        |
+----------------------------------------------------------+
                           vs
+----------------------------------------------------------+
|                     DataFusion 模型                       |
|  poll_next() -> RecordBatch  (一次一批，批內 Arrow 向量化)  |
+----------------------------------------------------------+
```

### 6.2 PhysicalExpr trait 與 ColumnarValue

來源：`datafusion/physical-expr-common/src/physical_expr.rs:75`（`physical-expr` crate 只是 re-export）：

```rust
pub trait PhysicalExpr: Any + Send + Sync + Display + Debug + DynEq + DynHash {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> { ... }   // :79
    fn nullable(&self, input_schema: &Schema) -> Result<bool> { ... }        // :83
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue>;        // :87
    fn evaluate_selection(&self, batch: &RecordBatch, selection: &BooleanArray)
        -> Result<ColumnarValue> { ... }                                     // :102（帶 validity filter）
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>>;                       // :164
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn PhysicalExpr>>)
        -> Result<Arc<dyn PhysicalExpr>>;                                    // :170
}
```

`ColumnarValue`（`datafusion/expr-common/src/columnar_value.rs:96`）有兩個變體：

```rust
pub enum ColumnarValue {
    Array(ArrayRef),      // :98  整欄 Arrow 陣列
    Scalar(ScalarValue),  // :100 單一常數純量
}
```

這是 arneb 該抄的關鍵設計：**表達式對整個 `RecordBatch` 求值並回傳「整欄陣列或單一純量」**，避免 row-by-row。當表達式對所有 row 結果相同（如常數），回傳 `Scalar` 省下物化整欄的成本，呼叫端再視需要以 `into_array(num_rows)`（`columnar_value.rs:136`）惰性展開。

### 6.3 Partitioning enum

來源：`datafusion/physical-expr/src/partitioning.rs:117`：

```rust
pub enum Partitioning {
    RoundRobinBatch(usize),                       // :119 均勻分配 batch
    Hash(Vec<Arc<dyn PhysicalExpr>>, usize),      // :122 對指定表達式取 hash 後取模
    Range(RangePartitioning),                     // :124 範圍分區（已實作，非未實作）
    UnknownPartitioning(usize),                   // :126
}
```

> **修正**：早期文獻常稱 `Range` 變體「未實作」；本地 `main` 已是 `Range(RangePartitioning)` 的真實變體。`Hash` 是 `HashJoinExec` 與 partitioned aggregate 能在多執行緒/多節點並行的關鍵。

### 6.4 Push vs Pull 與並行模型權衡

| 模型特性 | DataFusion (Pull-based Stream) | DuckDB / Morsel-driven (Push-based) |
| :--- | :--- | :--- |
| **資料流向** | 上層算子主動 `poll_next` 向下層拉取 | 底層 Source 主動把 morsel 推入 pipeline |
| **非同步集成** | 與 Rust `async/await` + Tokio 天然契合，易處理非同步 I/O | 通常自建 OS 執行緒池，難整合非同步遠端儲存 |
| **排程開銷** | 依賴 Tokio task 排程；batch 過小則 context switch 頻繁 | 極低，每核綁定一 worker，資料局部性佳 |
| **控制複雜度** | 容易（`futures::StreamExt`） | 較複雜，需精準控制 pipeline break 節點 |

### 對 arneb 的啟發

1. **pull-based `SendableRecordBatchStream` 是背壓的根**：DataFusion 全程 pull，背壓 = 消費端不 poll。arneb streaming refactor 的 deadlock（MEMORY 2026-05-23）根因是「`task_manager` semaphore permit 持有整個 task 生命週期」與 stream 背壓相衝 —— DataFusion **沒有 per-task semaphore**，平行度靠 `RepartitionExec` 的 channel 容量天然限流。arneb 的 Phase A（刪除 semaphore）方向與 DataFusion 一致。
2. **`ColumnarValue::Scalar` 惰性物化**正是緩解 MEMORY 反覆出現的「expression evaluator per-call overhead」（拖累 Q21/Q04）的標準手法 —— 確認 arneb 的表達式求值在 `BinaryOp(Col,op,Col)` 以外也走 scalar 短路。
3. **線程飢餓預防**：`hash_join.rs` 的 build/probe、`aggregate.rs` 的 hash group-by 都是 CPU 密集；若 `poll_next` 內單次佔用 CPU 過久（>10ms 不 yield），Tokio work-stealing 池會飢餓導致 I/O 吞吐暴跌。對策：在長迴圈插入 `tokio::task::consume_budget().await` 或保持 batch 適中（DataFusion 預設 8192）讓單批計算維持微秒級。

---

## 7. 分散式執行（核心單行程 + Ballista）

**關鍵事實**：`datafusion/*` 核心是單行程的。真正的分散式排程在獨立倉庫 `apache/datafusion-ballista`，重用同一套 `ExecutionPlan`。但其物理計畫設計（`Partitioning` 與 `Distribution`）為分散式奠定基礎。

### 7.1 Ballista 拓樸

```
              ┌─────────────────────┐
   client ───▶│ ballista-scheduler  │  收 job → 建 execution graph（physical plan 切成 stages）
              └──────────┬──────────┘  以 protobuf 下發 task；UnresolvedShuffleExec→ShuffleReaderExec
                         │ poll tasks (gRPC)
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
  ┌───────────┐   ┌───────────┐   ┌───────────┐
  │ executor1 │   │ executor2 │   │ executor3 │  各自跑 physical plan 的若干 partition
  └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
        │ ShuffleWriterExec：依 shuffle partitioning 重分區，寫 Arrow IPC 落地
        └────────── Arrow Flight 串流 shuffle 檔 ──────────┘
                   ShuffleReaderExec：跨 executor 連線、串回 IPC
```

### 7.2 Stage 切分與 shuffle

* **Stage 在 partitioning 改變處切開**（"pipeline breakers"）。partitioning 的改變定義 query stage 邊界。
* **`ShuffleWriterExec`**：把 stage 輸出依 shuffle partitioning 重分區，每個 output partition 以 **Arrow IPC 格式串流落地**。
* **`ShuffleReaderExec`**：scheduler 在所有 shuffle task 完成後，把計畫中的 `UnresolvedShuffleExec` 替換成 `ShuffleReaderExec`；後者透過 **Arrow Flight** 連到其他 executor 串回 IPC 檔。
* **task = physical plan（protobuf）+ 一組 partition**：executor 可平行跑同一計畫的多個 partition。
* 近期改進：sort-based shuffle 成為預設；shuffle writer 的 disk I/O 移出 tokio worker pool 以免阻塞排程。

### 7.3 與 arneb 的對照

| 概念 | Ballista | arneb |
|------|----------|-------|
| 排程者 | `ballista-scheduler`（獨立行程） | coordinator（內建 fragmenter + 排程） |
| 工作者 | `ballista-executor`（poll task） | worker（接 task，跑 fragment） |
| stage 切分 | scheduler 在 shuffle 邊界切 | `crates/planner` 的 `PlanFragmenter` |
| 跨節點交換 | `ShuffleWriterExec`/`ShuffleReaderExec` + Arrow Flight | arneb 的 `ExchangeExec` + `ExchangeClient` + Arrow Flight |
| shuffle 物化 | **寫 Arrow IPC 落地**再讀 | **streaming Flight**（不落地） |
| 計畫序列化 | protobuf（`datafusion/proto` 風格） | arneb 自製 fragment 序列化 |

### 對 arneb 的啟發

1. Ballista「stage 在 pipeline breaker（partitioning 改變）處切開」與 arneb fragmenter 把每個 Join/Aggregate 拆成獨立 worker fragment 的方向一致。
2. Ballista shuffle **落地 Arrow IPC**；arneb 刻意選 streaming Flight 避免磁碟。兩者是經典 trade-off：落地較穩（容錯、背壓簡單），streaming 較快但背壓難（正是 MEMORY 中 streaming refactor deadlock 的根因）。可考慮把「落地」當成背壓飽和時的 fallback（spillable exchange）—— 這正是 arneb `exec-exchange-backpressure` 在做的。

---

## 8. 記憶體管理、資源控管與 spill-to-disk

來源：`datafusion/execution/src/memory_pool/mod.rs` 與 `datafusion/physical-plan/src/sorts/sort.rs`。這節與 arneb 的 spill 工程（MEMORY Phase 0-4）最直接相關。

### 8.1 MemoryPool / MemoryReservation / MemoryConsumer 三層抽象

```rust
pub trait MemoryPool: Any + Send + Sync + Debug + Display {     // mod.rs:186
    fn register(&self, _consumer: &MemoryConsumer) {}            // :193
    fn unregister(&self, _consumer: &MemoryConsumer) {}          // :198
    fn grow(&self, reservation: &MemoryReservation, additional: usize);  // :203（不會失敗）
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize);    // :206
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize)
        -> Result<()>;                                          // :211（會失敗 → ResourcesExhausted）
    fn reserved(&self) -> usize;                                // :214
    fn memory_limit(&self) -> MemoryLimit { ... }               // :223
}
```

* **`MemoryConsumer`**（`mod.rs:271`）：具名分配者，關鍵是 `can_spill() -> bool`（`:338`）—— 告訴 pool「我撐不住時能落地」。
* **`MemoryReservation`**（`mod.rs:383`）：實際持有的位元組數，提供 `grow`/`try_grow`/`shrink`/`split`（`:497`）/`free`。`split` 能把一塊預留切出獨立子預留（如 SortExec 把 merge 用記憶體先切出來）。

四種具體 pool（`datafusion/execution/src/memory_pool/pool.rs`）：

| Pool | 行為 | 行號 |
|------|------|------|
| `UnboundedMemoryPool` | 無上限（預設） | `pool.rs:33` |
| `GreedyMemoryPool` | 固定上限，先到先得，耗盡即 `ResourcesExhausted` | `pool.rs:83` |
| `FairSpillPool` | 固定上限，在會 spill 的運算子間「公平」分配 | `pool.rs:181` |
| `TrackConsumersPool` | 包裹另一個 pool，追蹤各 consumer，OOM 時報出最大用量者（更好的錯誤訊息） | `pool.rs:418` |

### 8.2 SortExec spill（ExternalSorter）

`ExternalSorter`（`datafusion/physical-plan/src/sorts/sort.rs:209`）的 spill 流程（與 arneb Grace 路徑可逐項對照）：

1. 用 `MemoryConsumer` 註冊 reservation；**另開一塊 `merge_reservation`**（`sort.rs:260`）並預留 `sort_spill_reservation_bytes`（`:263`），保證「合併階段一定有記憶體可用」。
2. `reserve_memory_for_batch_and_maybe_spill`（`sort.rs:732`）：`try_grow` 失敗（`:740`）且有 buffered batch 時，觸發 `sort_and_spill_in_mem_batches().await?`（`:746`）—— 先排序，再寫成 `SortedSpillFile`（Arrow IPC）。batch 記憶體估算採 `record_batch_size + sliced_size`（`:786`）。
3. 輸入耗盡後，`StreamingMergeBuilder` 把所有 spill 檔與殘存 in-memory batch 做 streaming merge，用先前 `split` 出的 merge 預留，避免跨 partition 互相餓死。

通用 spill 流程（亦適用 `HashJoinExec`、`GroupedHashAggregateStream`）：(1) 記憶體閾值觸發 → (2) 以 Arrow IPC 序列化寫本地臨時目錄 → (3) 釋放記憶體、重置初始 reservation → (4) 結束時從磁碟歸併讀回，串流化輸出。

### 對 arneb 的啟發

1. **spill 前先 `split` 出 merge 預留**（`sort_spill_reservation_bytes` + `merge_reservation`）是 arneb 該補的 —— MEMORY 中 Q05/Q09 的 OOM 常因「spill 後合併又爆」；DataFusion 結構性地避免它。
2. **`TrackConsumersPool` 的「OOM 報最大用量者」**對 arneb debug Q09「untracked Arrow allocations」極有價值 —— 但前提是**所有大配置都走 `MemoryReservation`**。MEMORY 已指出 arneb 的 Filter/Project/Repartition channel/FlightDecoder 配置未被追蹤，這正是 DataFusion 模型要求「每筆 alloc 都 register/try_grow」的原因。Z.4 把 RepartitionExec channel 納入 pool 就是往這方向走。
3. arneb 的 [memory_pool.rs](file:///Users/bochengyang/formosa-ventures/repos/arneb/crates/execution/src/memory_pool.rs) 與 [spill.rs](file:///Users/bochengyang/formosa-ventures/repos/arneb/crates/execution/src/spill.rs) 已採類似設計；務必在 [hash_join.rs](file:///Users/bochengyang/formosa-ventures/repos/arneb/crates/execution/src/hash_join.rs) 的 hot path 嚴格調用 `try_grow` 契約。`can_spill()` 旗標讓 pool 知道「該找誰落地」—— 採 `FairSpillPool` 等價物時，運算子需誠實宣告可否 spill。

---

## 9. 儲存與資料來源抽象

### 9.1 TableProvider 與 Catalog 模型

三層 Catalog 階層：`CatalogProvider`（資料庫）→ `SchemaProvider`（命名空間）→ `TableProvider`（實體表）。

`TableProvider`（`datafusion/catalog/src/table.rs:52`）的關鍵在於它**不直接返回數據**，而是返回一個物理掃描算子：

```rust
#[async_trait]
pub trait TableProvider: Any + Debug + Sync + Send {
    fn schema(&self) -> SchemaRef;                  // table.rs:54
    fn table_type(&self) -> TableType;              // :67
    async fn scan(&self, state: &dyn Session,
        projection: Option<&Vec<usize>>,            // projection pushdown
        filters: &[Expr],                            // filter pushdown
        limit: Option<usize>,                        // limit pushdown
    ) -> Result<Arc<dyn ExecutionPlan>>;            // :185（通常回傳 DataSourceExec）
    fn supports_filters_pushdown(&self, filters: &[&Expr])
        -> Result<Vec<TableProviderFilterPushDown>>; // :303
    fn statistics(&self) -> Option<Statistics> { ... }
    async fn insert_into(&self, ...) -> Result<Arc<dyn ExecutionPlan>>;
}
```

`scan()` 一次承接三種 pushdown（projection / filter / limit），回傳 `ExecutionPlan`。

### 9.2 TableProviderFilterPushDown 三態

來源：`datafusion/expr/src/table_source.rs:37`：

```rust
pub enum TableProviderFilterPushDown {
    Unsupported,   // :39 完全推不下去 → 上層保留 FilterExec
    Inexact,       // :45 能近似套用（如 row group 層級剪枝），上層仍需精確 filter 複驗
    Exact,         // :50 完整套用，上層 Filter 可安全移除
}
```

`Inexact` 是非常重要的設計 —— 它精確表達 parquet row-group min/max 剪枝「只能剪掉確定不符的、邊界仍需上層 FilterExec 複驗」的語意。

### 9.3 ListingTable 與 ObjectStore

`ListingTable`（`datafusion/catalog-listing/src/table.rs`）把一堆檔案（CSV/Parquet/JSON）當單一表的內建 `TableProvider`：

* 雙 schema：`file_schema`（檔案實際欄）+ `table_schema`（加上 Hive 風格分區欄如 `date=2024-06-01`）。
* `scan` 把 projection 餵給 `FileScanConfigBuilder`；**分離 partition filter（用於目錄剪枝）與一般 filter**；最後產出 `FileScanConfig` → 各格式的 `create_physical_plan` → `DataSourceExec`。
* `FileStatisticsCache` 快取 file-level 統計，避免重複 metadata fetch。

DataFusion 透過 `object_store` 套件統一本地檔案、AWS S3、GCS、Azure Blob 的讀寫介面，使同一 `ParquetFormat` 無縫運行於本地或雲端。

### 對 arneb 的啟發

`TableProviderFilterPushDown::{Exact, Inexact, Unsupported}` 三態非常值得抄 —— arneb 的 parquet/Hive 剪枝若只有「推得下/推不下」二態，會在 row-group 剪枝後面臨「該不該移除上層 filter」的正確性風險。直接引入 `Inexact` 語意，讓 planner 知道哪些 filter 必須保留複驗。`FileStatisticsCache` 呼應 arneb 已做的 NDV/統計快取。此外建議仿照 `TableProvider::scan` 讓 [TableProvider](file:///Users/bochengyang/formosa-ventures/repos/arneb/crates/catalog/src/lib.rs#L84) 返回一個實作 `ExecutionPlan` 的 `ScanExec`（而非 arneb 目前 [DataSource::scan](file:///Users/bochengyang/formosa-ventures/repos/arneb/crates/execution/src/datasource.rs#L49) 直接回傳 `SendableRecordBatchStream`）—— 這讓 optimizer/fragmenter 能在物理層介入重寫掃描細節（如把一個 `ScanExec` 切分為多個 fragment 發往不同 executor）。

---

## 10. 並行模型與排程

DataFusion 的並行**不靠中央排程器**，而是「每個 partition 一條 tokio 串流」+ `RepartitionExec` 做分區交換。

### 10.1 基於 Tokio Tasks 的 Pipeline 並行

**一個分區（partition）就是一個獨立的非同步 task 工作單元**。執行 `B.execute(0)` 時遞迴呼叫 `A.execute(0)`，整條 pull chain 被包成一個 Tokio task：

```
Thread 1 (Tokio Worker)  <--- Task [A.part0 -> B.part0]
Thread 2 (Tokio Worker)  <--- Task [A.part1 -> B.part1]
Thread 3 (Tokio Worker)  <--- Task [A.part2 -> B.part2]
Thread 4 (Tokio Worker)  <--- Task [A.part3 -> B.part3]
```

每個 task 內部是一條 pull chain：`TaskStream.poll() -> FilterStream.poll() -> ScanStream.poll() -> I/O Read`。這讓排程器實作極簡 —— 直接讓 Tokio 做協作式排程。

### 10.2 RepartitionExec

來源：`datafusion/physical-plan/src/repartition/mod.rs`（`pub struct RepartitionExec` 於 `:1038`）。「把 N 個 input partition 映射成 M 個 output partition」。

* `BatchPartitioner`：`new_hash_partitioner`（用 `StrengthReducedU64` 除數加速取模）與 `new_round_robin_partitioner`。`partition_iter()` 產出 `(partition_index, RecordBatch)`，把 CPU-bound 的分區與 I/O 分離。
* **channel-based shuffle**：每個 input partition 一個 async task，pull 上游 → `BatchPartitioner` 分配 → 透過 `DistributionSender` 送往 output partition 的 channel；下游用 `PerPartitionStream` 接收。背壓由 channel 容量自然提供。
* **連程序內 repartition 都支援 spill**：`RepartitionBatch::{Memory, Spilled}`（`mod.rs:140`）標示資料在記憶體或已落地，記憶體吃緊時透過 `SpillPoolWriter`（`mod.rs:41,163`）落地，且保證 FIFO 順序。
* `preserve_order` 欄位為 `true` 時，作為 order-preserving repartition（等同 `SortPreservingRepartitionExec`）運作。

### 10.3 並行如何被「插入」

平行度不是運算子寫死的，而是 physical optimizer（`EnsureRequirements`，見下節）依 `target_partitions` **自動插入 `RepartitionExec`**。運算子只透過 `required_input_distribution()` 宣告需求（如 `HashJoinExec` 要 `Distribution::HashPartitioned`），optimizer 負責湊齊。

### 對 arneb 的啟發

DataFusion 的 `RepartitionExec` 以 **tokio channel + 每 partition 一 task** 做程序內 shuffle，背壓由 channel 容量天然提供 —— 幾乎就是 arneb worker 內 [RepartitionExec](file:///Users/bochengyang/formosa-ventures/repos/arneb/crates/execution/src/repartition.rs) 的設計。值得注意 DataFusion **連程序內 channel 都做 spill 並納入 MemoryPool 追蹤**；arneb 的 Z.4（RepartitionExec channel 納入 pool）方向完全正確 —— DataFusion 印證了「程序內 channel 也會吃光記憶體、也要 spill + 追蹤」。

---

## 11. 物理優化器與 EnsureRequirements（重大發現）

### 11.1 PhysicalOptimizerRule 預設規則集

來源：`datafusion/physical-optimizer/src/optimizer.rs`。介面：

```rust
fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions)
    -> Result<Arc<dyn ExecutionPlan>>;
fn name(&self) -> &str;
fn schema_check(&self) -> bool;   // EnsureRequirements 回 true（ensure_requirements/mod.rs:254）
```

預設 **21 條** `PhysicalOptimizerRule`（依序，建構於 `optimizer.rs:144` 起的 `PhysicalOptimizer::new()`）：

```
 1  OutputRequirements(add)         8  OptimizeAggregateOrder     15 LimitPushdown
 2  AggregateStatistics             9  WindowTopN                 16 TopKRepartition
 3  JoinSelection                   10 ProjectionPushdown         17 ProjectionPushdown
 4  LimitedDistinctAggregation      11 OutputRequirements(remove) 18 PushdownSort
 5  FilterPushdown                  12 TopKAggregation            19 EnsureCooperative
 6  EnsureRequirements              13 LimitPushPastWindows       20 FilterPushdown(post)
 7  CombinePartialFinalAggregate    14 HashJoinBuffering          21 SanityCheckPlan
```

### 11.2 EnsureRequirements 合併取代了 EnforceDistribution + EnforceSorting

**重大發現（已逐字佐證）**：早期 DataFusion 文獻常提到的 `EnforceDistribution` 與 `EnforceSorting` 兩條獨立規則，在本地 `main` 已**合併成單一 `EnsureRequirements`**。

* 檔案位於 `datafusion/physical-optimizer/src/ensure_requirements/mod.rs`（**注意是目錄不是單檔**；舊路徑 `enforce_distribution.rs` 已不存在）。
* `pub struct EnsureRequirements {}` 定義於 `ensure_requirements/mod.rs:166`；`impl PhysicalOptimizerRule for EnsureRequirements` 於 `:175`。
* 模組頂端的文件註解（`ensure_requirements/mod.rs:18-31`）**逐字寫明**：

  > "[`EnsureRequirements`] optimizer rule that enforces distribution and sorting requirements together so that the two never invalidate each other. This rule **replaces the separate `EnforceDistribution` + `EnforceSorting` rules** with a unified approach inspired by Apache Spark's `EnsureRequirements` and Presto/Trino's `AddExchanges`."
  >
  > 動機（非冪等問題）："The previous two-rule design suffers from **non-idempotent composition**: `EnforceSorting`'s `pushdown_sorts` can break distribution invariants established by `EnforceDistribution`, because `SortExec.preserve_partitioning` couples sorting and distribution decisions."（引 issue #21973）

`EnsureRequirements::optimize` 的運作（取自 `mod.rs:176` 起的多次樹走訪）：

```
Phase 1：top-down join-key 重排
Phase 2（核心，定義此規則的性質）：單一 combined bottom-up pass，對每個節點同時解決
    distribution 與 sorting：
      Step 1：插入 RepartitionExec / CoalescePartitionsExec / SortPreservingMergeExec 滿足 distribution
      Step 2：插入帶 preserve_partitioning 旗標的 SortExec 滿足 ordering
Phase 3：sort 平行化、order-preserving 變體、sort pushdown、partial sort（部分可再合併）
```

合併的理由是 **idempotency**：舊兩段式 pipeline 中，`EnforceSorting` 的 `pushdown_sorts` 會破壞 distribution invariant，使第二次套用把平行 sort 退化成序列 sort。合併後「跑兩次得到同一個計畫」。

> 校正前述兩份草稿：agy 版未提及此合併、仍以舊的 `EnforceDistribution`/`EnforceSorting` 心智模型描述；agent 版正確指出合併但因採 WebFetch 摘要而未附行號。本文以本地原始碼確認並補齊 file:line，並更正 agent 草稿中「`EnsureRequirements {}` 直接建構」的細節 —— 實際在 `PhysicalOptimizer::new()` 是以 `EnsureRequirements::new()` 加入規則清單。

### 對 arneb 的啟發（重要）

arneb 自製 fragmenter + RepartitionExec 自動包裹的邏輯，本質上就是 DataFusion `EnsureRequirements` 在做的事 —— 比對每個運算子的 `required_input_distribution` 與 child 實際 `Partitioning`，不符就插入交換運算子。DataFusion「合併 distribution 與 sorting 強制」與「保證 idempotency」的教訓直接適用：**分散式 exchange 插入規則必須冪等**，否則多 pass 改寫會互相破壞 partition 不變式（與 MEMORY 中 A.4 / both_leaf gate 的踩雷同源）。建議把 distribution 與 ordering 強制放在**單一 bottom-up pass** 內處理，讓 sorting 決策能看到完整的 distribution context。

---

## 12. 程式碼地圖（關鍵目錄與模組職責對照表）

| 子系統 | crate / 路徑 | 核心 trait/struct | arneb 對應 crate |
|--------|--------------|-------------------|------------------|
| SQL binding | `datafusion/sql/src/planner.rs:454` | `SqlToRel`、`ContextProvider`(`expr/src/planner.rs:44`) | `crates/sql-parser` + `crates/planner` |
| Logical plan | `datafusion/expr/src/logical_plan/plan.rs:207` | `LogicalPlan`、`Expr` | `crates/planner` |
| Analyzer（改語意） | `datafusion/optimizer/src/analyzer/type_coercion.rs:68` | `AnalyzerRule`、`TypeCoercion` | `crates/planner` AnalysisPass |
| Optimizer（不改語意） | `datafusion/optimizer/src/optimizer.rs:83,290` | `OptimizerRule`、`Optimizer`、`Transformed`、25 條規則 | `crates/planner` `LogicalOptimizer` |
| Physical planner | `datafusion/core/src/physical_planner.rs:261` | `PhysicalPlanner`、`DefaultPhysicalPlanner` | `crates/execution` `ExecutionContext` |
| Physical optimizer | `datafusion/physical-optimizer/src/optimizer.rs`、`ensure_requirements/mod.rs:166` | `PhysicalOptimizerRule`、`EnsureRequirements`、21 條規則 | arneb physical rewrite passes |
| Execution plan | `datafusion/physical-plan/src/execution_plan.rs:94,1068` | `ExecutionPlan`、`PlanProperties`、`SendableRecordBatchStream` | `crates/execution` 運算子 |
| Repartition | `datafusion/physical-plan/src/repartition/mod.rs:1038` | `RepartitionExec`、`BatchPartitioner`、`RepartitionBatch` | `crates/execution` `RepartitionExec` |
| Partitioning | `datafusion/physical-expr/src/partitioning.rs:117` | `Partitioning`（RoundRobin/Hash/Range/Unknown） | `crates/execution` |
| Sort + spill | `datafusion/physical-plan/src/sorts/sort.rs:209,732` | `SortExec`、`ExternalSorter`、`StreamingMergeBuilder` | `crates/execution` SortExec |
| Physical expr | `datafusion/physical-expr-common/src/physical_expr.rs:75` | `PhysicalExpr`、`ColumnarValue`(`expr-common/src/columnar_value.rs:96`) | `crates/execution` 表達式 |
| Memory / runtime | `datafusion/execution/src/memory_pool/mod.rs:186`、`pool.rs` | `MemoryPool`、`MemoryReservation`、`MemoryConsumer`、`TaskContext` | `crates/execution` `MemoryPool` |
| Catalog / table | `datafusion/catalog/src/table.rs:52`、`expr/src/table_source.rs:37` | `TableProvider`、`TableProviderFilterPushDown` | `crates/catalog` |
| File table | `datafusion/catalog-listing/src/table.rs` | `ListingTable`、`FileScanConfig`、`DataSourceExec` | `crates/connectors`、`crates/hive` |
| 分散式（外掛） | `apache/datafusion-ballista` | `ballista-scheduler`/`-executor`、`ShuffleWriterExec`、`ShuffleReaderExec`、`UnresolvedShuffleExec` | `crates/server`、`crates/rpc` |

---

## 驗證方法與來源

* **核實對象**：本地 checkout `/Users/bochengyang/formosa-ventures/repos/datafusion`，`main` 分支，commit **`e71bd56`**（"fix: Improve consistency of per-column stats on `FilterExec` output (#22718)"，2026-06-03）。
* **latest 回查（2026-06-05）**：已 fetch 並以 `origin/main` commit **`e1d8d463b51e`** 抽查。`SqlToRel`、`LogicalPlan`、`AnalyzerRule`、`OptimizerRule`、`PhysicalPlanner`、`ExecutionPlan`、`SendableRecordBatchStream`、`TableProvider`、`PhysicalExpr` 等核心符號仍存在；logical optimizer 25 條與 physical optimizer 21 條規則仍吻合。latest 另凸顯 `PhysicalOptimizerContext`、`StatisticsRegistry`、`FilterPushdown` pre/post phase 等物理優化擴充點。本文 file:line 未重標，不能視為 latest 精準行號。
* **引用方式**：行號為相對路徑（相對於上述倉庫根）`相對路徑:行號`。所有 trait 簽名、enum 變體、struct 欄位、規則清單均以 `Grep` + `Read` 直接打開原始碼逐項確認，未引用未開啟過的檔案。
* **合併來源**：
  * agy 版（`datafusion-agy.md`）—— 提供完整 12 章節敘事、ASCII 圖、Push/Pull 對照表、arneb 借鏡段落；其行號清單（`/tmp/verify_datafusion_agy.md`）多數正確，但部分偏移（如 `ExecutionPlan` trait 實為 `:94` 非 `:94-258` 範圍標示、`PlanProperties` 實為 `:1068`）。
  * agent 版（`datafusion-agent.md`）—— 提供 crate 對照、25 條 logical 規則、21 條 physical 規則、`EnsureRequirements` 合併的重大發現；但因採 WebFetch 摘要刻意未附行號。本文整合兩者並補齊全部 file:line。
* **本次修正的錯誤**：
  1. **EnsureRequirements（最重要）**：確認 agent 版的發現屬實 —— `EnforceDistribution` + `EnforceSorting` 已合併為單一 `EnsureRequirements`。佐證檔案為 `datafusion/physical-optimizer/src/ensure_requirements/mod.rs`（目錄非單檔），模組註解 `:18-31` 逐字寫明 "replaces the separate `EnforceDistribution` + `EnforceSorting` rules"、動機是 non-idempotent composition（issue #21973），靈感來自 Spark `EnsureRequirements` 與 Presto/Trino `AddExchanges`。struct 於 `:166`、impl 於 `:175`。agy 版的舊心智模型（仍分兩條規則）已更正。
  2. **`Partitioning::Range`**：agent 草稿稱「未實作」；本地 `partitioning.rs:124` 已是真實的 `Range(RangePartitioning)` 變體 —— 已更正。
  3. **`ExecutionPlan::execute` 非 async**：agy 草稿把 `execute` 寫成 `async fn`；本地 `execution_plan.rs:475` 是同步簽名，回傳 `Result<SendableRecordBatchStream>`（非同步性在 stream 的 `poll_next`）—— 已更正。
  4. **物理優化器規則建構**：agent 草稿暗示 `EnsureRequirements {}` 直接入列；實際 `PhysicalOptimizer::new()`（`optimizer.rs:190`）以 `EnsureRequirements::new()` 加入 —— 已標註。
* **補入的 file:line**：本文為以下關鍵事實新補行號（agent 版完全無行號）：`ExecutionPlan` trait 全部 14 個方法各自行號、`PlanProperties` 7 欄位、`PhysicalExpr` 6 方法、`ColumnarValue` 2 變體 + `into_array`、`LogicalPlan` enum、`OptimizerRule` 3 方法 + 25 條規則建構位置、`PhysicalOptimizer` 21 條規則建構位置、`EnsureRequirements` struct/impl/name/schema_check、`TableProvider` 各方法、`TableProviderFilterPushDown` 3 變體、`MemoryPool` 8 方法、4 種 pool、`MemoryReservation`/`MemoryConsumer`/`can_spill`/`split`、`ExternalSorter` + spill 函數、`RepartitionExec` + `RepartitionBatch` + `SpillPool`、`Partitioning` 4 變體、`SqlToRel` + `ContextProvider`、physical planner trait + Partial/Final agg 行號。合計約 60+ 個 file:line 直接核實。
* **仍不確定/未深入的點**：
  1. **Ballista 分散式細節（第 7 章）** 來自 agent 版的 Ballista 官方文件與 WebSearch，**未對照本地 Ballista 原始碼**（本地僅 checkout `apache/datafusion` 核心，無 `datafusion-ballista`）。`ShuffleWriterExec`/`ShuffleReaderExec`/`UnresolvedShuffleExec` 的行號因此從缺。
  2. **`JoinSelection` 的 broadcast 閾值具體判定邏輯**（第 5.4 節）未逐行打開 `join_selection.rs`，僅依規則清單與通用認知描述。
  3. **`DefaultPhysicalPlanner` 的 `planning_concurrency` 平行建構細節**僅確認 `create_initial_plan` 存在（`:434`）與 Partial/Final agg 切法（`:1144`/`:1165`），未逐行追蹤平行 task 數的計算。
  4. 第 8.2 節通用 spill 流程中「`HashJoinExec` / `GroupedHashAggregateStream` 亦實作 spill」依架構通則描述，僅 `ExternalSorter` 路徑逐行核實。

> 全文 12 章 + 驗證附錄；DataFusion 與 arneb 同為 Rust + Arrow，trait 邊界、`SendableRecordBatchStream` 背壓與 optimizer 擴充點為本文重點。每章末「對 arneb 的啟發」對應 MEMORY 中的實際工程脈絡（spill Phase 0-4、Q05/Q09 OOM、streaming refactor deadlock、Z.4、exec-exchange-backpressure）。
