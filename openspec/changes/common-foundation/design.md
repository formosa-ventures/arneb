## Context

trino-alt 目前是空專案，只有 CLAUDE.md 定義了架構藍圖。`common` crate 是第一個要建立的 crate，所有後續 8+ 個 crate 都會依賴它。這意味著此處的 API 設計必須穩定且具前瞻性——改動成本會隨著下游 crate 增加而放大。

專案約定：使用 `thiserror`（非 `anyhow`）做 library error、`tracing`（非 `log`）做日誌、Arrow 作為資料格式。

## Goals / Non-Goals

**Goals:**

- 建立 Cargo workspace 結構，讓後續 crate 可直接加入
- 提供統一的錯誤型別，各 crate 可組合使用且保留完整錯誤鏈
- 定義 SQL 型別系統與 Arrow 型別的雙向對應
- 提供表格/欄位識別的共用型別
- 建立可從 TOML 檔案 + 環境變數載入的組態系統
- 每個模組都有完整的單元測試

**Non-Goals:**

- 不實作任何 SQL 解析、查詢規劃或執行邏輯
- 不處理分散式相關型別（Phase 2 範疇）
- 不建立 RPC/網路通訊型別
- 不實作 logging/tracing 的 subscriber 設定（由 server binary 負責）

## Decisions

### D1: 模組結構 — 單一 crate 多模組 vs 多個 micro-crate

**選擇**: 單一 `trino-common` crate，內含 `error`、`types`、`config` 三個模組。

**理由**: 這三個模組高度耦合（error types 引用 data types、config 用於全域設定），拆成多個 crate 會增加 workspace 管理負擔和循環依賴風險，沒有實質好處。

**替代方案**: 拆成 `trino-error`、`trino-types`、`trino-config` 三個獨立 crate。不採用，因為增加管理複雜度且沒有獨立發佈需求。

### D2: 錯誤型別設計 — 單一 enum vs 分層 enum

**選擇**: 分層設計。每個領域有自己的 error enum（`ParseError`、`PlanError`、`ExecutionError`、`ConnectorError`、`CatalogError`、`ConfigError`），再由頂層 `TrinoError` enum 以 `#[from]` 組合。

**理由**: 各 crate 只需要依賴自己領域的 error type，不被迫引入不相關的錯誤變體。頂層 `TrinoError` 用於 server binary 統一處理。

**替代方案**: 單一巨大 `Error` enum 包含所有變體。不採用，因為違反 Single Responsibility 且隨著專案成長會變得臃腫。

### D3: DataType 定義 — 自訂 vs 直接用 Arrow DataType

**選擇**: 自訂 `DataType` enum，提供 `impl From<DataType> for arrow::datatypes::DataType` 和反向轉換。

**理由**: SQL 型別系統和 Arrow 型別系統有語義差異（例如 SQL 的 `VARCHAR(255)` vs Arrow 的 `Utf8`、SQL 的 `DECIMAL(p,s)` 精度資訊）。自訂型別保留 SQL 語義，轉換層處理對應關係。

**替代方案**: 直接用 `arrow::datatypes::DataType`。不採用，因為會丟失 SQL 特有的型別資訊（長度限制、精度等）。

### D4: ScalarValue 表示 — 自訂 vs 用 DataFusion 的 ScalarValue

**選擇**: 自訂 `ScalarValue` enum，MVP 只支援基本型別（Null、Boolean、Int32/64、Float32/64、Utf8、Binary、Decimal128、Date32、Timestamp）。

**理由**: DataFusion 的 ScalarValue 帶入過多依賴且包含我們不需要的變體。自訂版本保持精簡，未來需要時可擴充。

**替代方案**: 依賴 `datafusion-common::ScalarValue`。不採用，因為引入整個 DataFusion 依賴樹太重。

### D5: TableReference 結構 — 三段式識別

**選擇**: `TableReference` 包含 `catalog: Option<String>`、`schema: Option<String>`、`table: String`。支援 `table`、`schema.table`、`catalog.schema.table` 三種格式。

**理由**: 與 Trino 的 naming convention 一致，支援多 catalog 的聯邦查詢場景。

### D6: 組態系統 — serde + toml vs 專門的 config library

**選擇**: 使用 `serde` + `toml` crate 反序列化，自行實作環境變數覆蓋邏輯。

**理由**: 需求簡單（一個 TOML 檔 + env vars），不需要 `config-rs` 等套件的多層合併功能。減少依賴。

**替代方案**: 使用 `config-rs`。不採用，因為我們的需求足夠簡單，直接實作更透明。

### D7: Crate 命名 — `common` vs `trino-common`

**選擇**: Crate 名稱為 `trino-common`（在 Cargo.toml 的 `[package] name`），目錄保持 `crates/common/`。

**理由**: 避免與 crates.io 上的 `common` 衝突，且明確歸屬專案。目錄用簡短名稱保持路徑簡潔。

## Risks / Trade-offs

**[API 過早凍結]** → 由於所有 crate 依賴 common，修改公開 API 影響範圍大。**緩解**: MVP 階段保持型別簡潔，只暴露確定需要的。使用 `#[non_exhaustive]` 標記可能擴充的 enum。

**[Arrow 版本耦合]** → `common` 中的 Arrow 型別對應鎖定特定 Arrow 版本，升級時所有 crate 必須同步。**緩解**: 在 workspace `Cargo.toml` 統一管理 Arrow 版本。

**[錯誤型別前置定義]** → 在實際實作各 crate 前就定義其錯誤型別，可能與實際需求不符。**緩解**: 先定義最基本的變體，各 crate 實作時再擴充。使用 `#[non_exhaustive]` 保留擴充彈性。

**[Config 過度設計]** → MVP 階段可能不需要完整的 config 系統。**緩解**: 只定義最基本的參數（bind address、port），其餘在需要時再加。提供合理的 default 值。
