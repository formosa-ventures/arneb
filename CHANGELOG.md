# Changelog

## [0.1.1](https://github.com/formosa-ventures/arneb/compare/v0.1.0...v0.1.1) (2026-10-03)


### ⚠ BREAKING CHANGES

* binary renamed from trino-alt to arneb, config file from trino-alt.toml to arneb.toml, and env var prefix from TRINO_ to ARNEB_

### Features

* add Phase 2 distributed architecture and advanced SQL ([eea8f31](https://github.com/formosa-ventures/arneb/commit/eea8f31479fcdde87a3d44ad136924c7347afc79))
* add serde serialization to plan types ([6ac0ce9](https://github.com/formosa-ventures/arneb/commit/6ac0ce9cd8d1652993934b504be5fe4a4c76789c))
* **benchmark:** add TPC-H runner with 16 queries and data generation ([22e87a7](https://github.com/formosa-ventures/arneb/commit/22e87a72bf402f7d35f5c2f7b42b2c3eb78465f2))
* **catalog:** add catalog system with traits, in-memory impl, and manager ([69ca5c1](https://github.com/formosa-ventures/arneb/commit/69ca5c1154d461f4b06384107020d02722eb7e62))
* **common:** add trino-common crate with error types, data types, and config ([ec2985c](https://github.com/formosa-ventures/arneb/commit/ec2985c16b251aa2591947342faa4afdeff273f6))
* **connectors:** add DDLProvider trait and memory connector writes ([ac43595](https://github.com/formosa-ventures/arneb/commit/ac43595961a365d716ea13ca8d03db049454697a))
* **connectors:** add memory and file connectors with CSV/Parquet support ([9ddcff8](https://github.com/formosa-ventures/arneb/commit/9ddcff80627feaa95e324d07cfb679f615219e33))
* **connectors:** Parquet pushdown, type support, and benchmark infra ([#30](https://github.com/formosa-ventures/arneb/issues/30)) ([ae161bb](https://github.com/formosa-ventures/arneb/commit/ae161bbc42de5e9a00505473705840bf7dd676dc))
* **execution:** add 71 Trino scalar functions (batch 1) ([#79](https://github.com/formosa-ventures/arneb/issues/79)) ([12589f6](https://github.com/formosa-ventures/arneb/commit/12589f6437268a1b8577461b5dda142d4d2e0f73))
* **execution:** add async streaming execution and pushdown ([5b46b27](https://github.com/formosa-ventures/arneb/commit/5b46b27dd559078942c40003105d521dc4967399))
* **execution:** add execution engine with physical operators and expression evaluator ([6a558f2](https://github.com/formosa-ventures/arneb/commit/6a558f2cd305c5127c796473ea449675dd49a3ca))
* **execution:** add hash join operator and logical optimizer ([5effd29](https://github.com/formosa-ventures/arneb/commit/5effd290b74d63fa13c99a4e482ce49b15bd2dba))
* **execution:** add scalar functions, set ops, window, semi-join operators ([0fa554e](https://github.com/formosa-ventures/arneb/commit/0fa554e2054f4e4a73dcc83fb4cee0eb2734dcb7))
* **hive:** object store support, hive connector, and hms 4.x ([#18](https://github.com/formosa-ventures/arneb/issues/18)) ([6fd9a31](https://github.com/formosa-ventures/arneb/commit/6fd9a31fb417c83710b430671d35c739b45ee35b))
* **iceberg:** read-only Apache Iceberg tables via Hive Metastore ([#80](https://github.com/formosa-ventures/arneb/issues/80)) ([e13a2ab](https://github.com/formosa-ventures/arneb/commit/e13a2abb94ada3f7d6b78a16439688e702a776fe))
* modern Web UI with Vite 8, React 19, and shadcn/ui ([#9](https://github.com/formosa-ventures/arneb/issues/9)) ([d10554f](https://github.com/formosa-ventures/arneb/commit/d10554f712e2cfd49acdcd658a5c9ebf09639f6e))
* **parser:** add DDL/DML, CASE, subquery, CTE, window, set ops to AST ([71742b9](https://github.com/formosa-ventures/arneb/commit/71742b9eea95376cd823a1db6f5a37006749a9fa))
* **planner:** add aggregate fixes, subquery, set ops, window planning ([9973f04](https://github.com/formosa-ventures/arneb/commit/9973f04c60a3ca4620e3c6ff3520a39b0a045893))
* **planner:** add distributed planning infrastructure ([60eed62](https://github.com/formosa-ventures/arneb/commit/60eed6208cc681ab0935c90eb57bf308ac0418f2))
* **planner:** add query planner with AST-to-LogicalPlan conversion ([d134ec5](https://github.com/formosa-ventures/arneb/commit/d134ec58554f769841021ba63af2880b19ff1404))
* **planner:** source spans and type-coercion analyzer ([#42](https://github.com/formosa-ventures/arneb/issues/42)) ([bfb3e43](https://github.com/formosa-ventures/arneb/commit/bfb3e433878cc902bb384fdbe81c23cd8838a228))
* **protocol:** add extended query, pg_catalog metadata, SET/SHOW ([3125fc7](https://github.com/formosa-ventures/arneb/commit/3125fc78787bc02bc75dd132d281595ba0a39c6a))
* **protocol:** add PostgreSQL wire protocol handler with pgwire ([3def001](https://github.com/formosa-ventures/arneb/commit/3def001da175e37151210aa614d92482e7c787ed))
* **protocol:** add SCRAM-SHA-256 password authentication for pgwire ([#77](https://github.com/formosa-ventures/arneb/issues/77)) ([a1f0017](https://github.com/formosa-ventures/arneb/commit/a1f0017c1fd6ccf6cf30968f001ad2b9cc74e7fd))
* **protocol:** serve the Trino client REST protocol ([#78](https://github.com/formosa-ventures/arneb/issues/78)) ([c0e0d50](https://github.com/formosa-ventures/arneb/commit/c0e0d505bb0aac17b3f1f536bd352254a054370a))
* **protocol:** wire distributed execution path ([c28998f](https://github.com/formosa-ventures/arneb/commit/c28998fdd91a25e140acc28e0cd67b7a9c648518))
* **rpc:** add flight server and coordinator/worker split ([7a378a4](https://github.com/formosa-ventures/arneb/commit/7a378a471a3860eaff8d25b9a7b5855494e50dbc))
* **rpc:** add task submission and worker task manager ([10954cd](https://github.com/formosa-ventures/arneb/commit/10954cd6ee7b5de31b55a0424d1901377912b430))
* **server:** add --iterations to arneb hash-password ([#84](https://github.com/formosa-ventures/arneb/issues/84)) ([bc5d1a4](https://github.com/formosa-ventures/arneb/commit/bc5d1a476ac5ebbe30a9083759c9b1e2f3003c01))
* **server:** add trino-alt binary with end-to-end query pipeline ([1a431d7](https://github.com/formosa-ventures/arneb/commit/1a431d779efca932fb10d9cb572aaba1357428f1))
* **server:** add web UI with dashboard, REST API, cluster view ([dbb8525](https://github.com/formosa-ventures/arneb/commit/dbb85252567744db25862fb853900f6593ebfbad))
* **sql-parser:** add SQL parsing crate with AST types and sqlparser-rs conversion ([6aa8741](https://github.com/formosa-ventures/arneb/commit/6aa8741050ef6cf2adf7ab8605c8d525912d6690))
* **sql-parser:** native DATE 'YYYY-MM-DD' literal support ([#33](https://github.com/formosa-ventures/arneb/issues/33)) ([4a3b322](https://github.com/formosa-ventures/arneb/commit/4a3b3221653735742465c55398f1fdced6007bfd))


### Bug Fixes

* **benchmark:** correct local sf001 parquet types ([#41](https://github.com/formosa-ventures/arneb/issues/41)) ([c374fb7](https://github.com/formosa-ventures/arneb/commit/c374fb72a6268b38b2ee4b064a51254f379126c7))
* **ci:** patch h2/rustls advisories and clippy 1.99 double_must_use ([#87](https://github.com/formosa-ventures/arneb/issues/87)) ([45add6c](https://github.com/formosa-ventures/arneb/commit/45add6cf9bda827abdef678ddfd34166582c55d0))
* **docker:** pull MinIO images from quay.io ([#73](https://github.com/formosa-ventures/arneb/issues/73)) ([91aac59](https://github.com/formosa-ventures/arneb/commit/91aac59dac1d978e68e5bb6f86931d62067f06e0))
* **docs:** remove root CNAME that triggers Jekyll fallback ([e98d294](https://github.com/formosa-ventures/arneb/commit/e98d2946f6d3b3307a4afdf9cba131eba804ecbd))
* **execution:** emit typed NULLs from aggregates over empty input ([#82](https://github.com/formosa-ventures/arneb/issues/82)) ([8642801](https://github.com/formosa-ventures/arneb/commit/864280152905d0ed1a2b0799482a8863f208f810))
* **execution:** use Kleene three-valued logic for AND/OR ([#81](https://github.com/formosa-ventures/arneb/issues/81)) ([5a74191](https://github.com/formosa-ventures/arneb/commit/5a7419161586f4513aead86f11f8a7cae446910a))
* **planner:** fold comparisons with a NULL literal to NULL ([#83](https://github.com/formosa-ventures/arneb/issues/83)) ([2452024](https://github.com/formosa-ventures/arneb/commit/2452024d4a094af3a127beb9b0c3f142aa9a4a0d))
* **pr30:** CI failures, TPC-H correctness, auto-gen rustfmt skip ([#31](https://github.com/formosa-ventures/arneb/issues/31)) ([4a8221d](https://github.com/formosa-ventures/arneb/commit/4a8221df52d9b0a755de86d4b3cac916be4744c9))
* **protocol:** hide empty schemas from metadata queries ([#27](https://github.com/formosa-ventures/arneb/issues/27)) ([8f45210](https://github.com/formosa-ventures/arneb/commit/8f452101ab877cf358b8500f78d7f2f3a2431a45))
* **release:** tag releases as vX.Y.Z without the component name ([#89](https://github.com/formosa-ventures/arneb/issues/89)) ([28b1751](https://github.com/formosa-ventures/arneb/commit/28b1751e4c4d3506f09123414abbcc95e24d5f3e))
* **release:** use simple release type with workspace version jsonpath ([#86](https://github.com/formosa-ventures/arneb/issues/86)) ([0033ab4](https://github.com/formosa-ventures/arneb/commit/0033ab4c005074fa6503e2ee514e5c9756e8657c))


### Performance Improvements

* **execution:** TPC-H 16/16 vs Trino + 1.17x quick-win speedup ([#47](https://github.com/formosa-ventures/arneb/issues/47)) ([06329c0](https://github.com/formosa-ventures/arneb/commit/06329c063b28e1c30f59f5f43527dfb9e26c340c))
* ship the validated TPC-H config and fix the estimate behind it ([#74](https://github.com/formosa-ventures/arneb/issues/74)) ([f7e46e2](https://github.com/formosa-ventures/arneb/commit/f7e46e28f94c4fde52b9ada989a1c6689dab8702))
* TPC-H engine optimizations and arneb-vs-Trino benchmark ([#62](https://github.com/formosa-ventures/arneb/issues/62)) ([aab31fb](https://github.com/formosa-ventures/arneb/commit/aab31fb46423574d6150342634610cca5e4c1474))


### Code Refactoring

* rename project from trino-alt to Arneb ([b9a52cf](https://github.com/formosa-ventures/arneb/commit/b9a52cf37565d7fb51bec993415e353564b5791a))

## Changelog
