## Why

trino-alt 是一個從零開始的 Rust 分散式 SQL 查詢引擎。所有後續 crate（sql-parser、planner、optimizer、execution、connectors、catalog、protocol、server）都需要一組共用的型別定義、錯誤處理機制和組態管理。`common` crate 是整個專案的基礎層，必須先建立才能進行任何其他開發。

## What Changes

- 建立 Cargo workspace，設定 `crates/common/` 作為第一個 crate
- 定義統一的錯誤型別層級（使用 `thiserror`），涵蓋 SQL 解析、規劃、執行、連接器等各階段的錯誤分類
- 定義共用資料型別：`DataType`（SQL 型別系統）、`TableReference`（catalog.schema.table 識別）、`ColumnInfo`（欄位元資料）、`ScalarValue`（常數值表示）
- 建立組態系統，支援從檔案和環境變數載入伺服器設定
- 整合 `tracing` 作為統一的日誌與追蹤框架

## Capabilities

### New Capabilities

- `error-types`: 統一的錯誤型別層級，使用 `thiserror` 定義各模組可組合的錯誤類型（ParseError、PlanError、ExecutionError、ConnectorError 等），支援錯誤鏈和上下文傳遞
- `common-data-types`: 共用資料型別定義，包括 SQL 型別系統（DataType）、表格與欄位識別（TableReference、ColumnInfo）、純量值（ScalarValue）、以及與 Arrow 型別的對應轉換
- `server-config`: 伺服器組態管理，支援 TOML 設定檔和環境變數覆蓋，涵蓋監聽地址、執行緒數、記憶體限制等參數

### Modified Capabilities

（無既有 capability，全部新建）

## Impact

- **新增 crate**: `crates/common/`
- **Cargo workspace**: 專案根目錄新增 `Cargo.toml` workspace 設定
- **關鍵依賴**: `thiserror`、`serde`/`toml`（config）、`tracing`、`arrow`（型別對應）
- **影響所有後續 crate**: 所有其他 crate 都將 `depend on common`，此處的 API 設計直接影響整個專案的 ergonomics
