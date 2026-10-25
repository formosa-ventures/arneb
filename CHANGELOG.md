# Changelog

## [0.1.6](https://github.com/formosa-ventures/arneb/compare/v0.1.5...v0.1.6) (2026-10-25)


### Features

* cluster version handshake (G5) ([#178](https://github.com/formosa-ventures/arneb/issues/178)) ([a2534d1](https://github.com/formosa-ventures/arneb/commit/a2534d153ea89c7401887c69e28df6a1663fa83c))
* exchange observability, cancel race fix and concurrency soak test (Phase 3, G1/G2) ([#177](https://github.com/formosa-ventures/arneb/issues/177)) ([c001243](https://github.com/formosa-ventures/arneb/commit/c0012438cfd6a818ed6ee7c11866c81ff8d23763))
* worker graceful drain and release of finished queries (G4) ([#179](https://github.com/formosa-ventures/arneb/issues/179)) ([b3735e7](https://github.com/formosa-ventures/arneb/commit/b3735e748adcd9b05b2b4cb61bcbaee0d196eab6))

## [0.1.5](https://github.com/formosa-ventures/arneb/compare/v0.1.4...v0.1.5) (2026-10-22)


### Features

* bounded query history and system.runtime.queries (Phase 3 M3, P5) ([#173](https://github.com/formosa-ventures/arneb/issues/173)) ([1ac1e1b](https://github.com/formosa-ventures/arneb/commit/1ac1e1b2e5dd1e8819ea309d037a015ed7b73738))
* cross-source joins — PostgreSQL dimensions × Hive facts (Phase 3 M2, F4) ([#167](https://github.com/formosa-ventures/arneb/issues/167)) ([db6b2b6](https://github.com/formosa-ventures/arneb/commit/db6b2b6ee3fea0c22d72c33dd67fa21ec66f0274))
* fail queries cleanly when a worker is lost (Phase 3 M3, P4 stage 1) ([#171](https://github.com/formosa-ventures/arneb/issues/171)) ([79fe426](https://github.com/formosa-ventures/arneb/commit/79fe4263529dcf1864f7b097212003e808e72494))
* Prometheus /metrics, slow-query log and a Grafana dashboard (Phase 3 M3, P5) ([#172](https://github.com/formosa-ventures/arneb/issues/172)) ([29ac414](https://github.com/formosa-ventures/arneb/commit/29ac4141cd7ccb2a84d7f17679a17939128e3168))


### Bug Fixes

* **memory:** count a batch's shared allocations once (exec-memory-accounting 4.5) ([#169](https://github.com/formosa-ventures/arneb/issues/169)) ([00ce541](https://github.com/formosa-ventures/arneb/commit/00ce541c36589344db4f4b1132fec070920e452d))
* **rpc:** set TCP_NODELAY on accepted Flight connections (SF10 regression from [#136](https://github.com/formosa-ventures/arneb/issues/136)) ([#170](https://github.com/formosa-ventures/arneb/issues/170)) ([5d49c9f](https://github.com/formosa-ventures/arneb/commit/5d49c9fdc902653703f743167c5e9e8871f92680))

## [0.1.4](https://github.com/formosa-ventures/arneb/compare/v0.1.3...v0.1.4) (2026-10-20)


### Features

* PostgreSQL connector on a shared RDBMS framework (Phase 3 M2, F1+F2) ([#158](https://github.com/formosa-ventures/arneb/issues/158)) ([5c5ada9](https://github.com/formosa-ventures/arneb/commit/5c5ada9c6da7e76a89e0bdd15079e54589ab77f7))


### Bug Fixes

* **hive,iceberg:** every node computes the same scan partitions ([#160](https://github.com/formosa-ventures/arneb/issues/160)) ([cebfe9e](https://github.com/formosa-ventures/arneb/commit/cebfe9ef5028f1b8d41310517781e851c3684596)), closes [#157](https://github.com/formosa-ventures/arneb/issues/157)

## [0.1.3](https://github.com/formosa-ventures/arneb/compare/v0.1.2...v0.1.3) (2026-10-18)


### Features

* admission control — per-query memory limit, cluster cancel, query queue, statement timeout ([#138](https://github.com/formosa-ventures/arneb/issues/138)) ([656b747](https://github.com/formosa-ventures/arneb/commit/656b7477f91c382cef62f8e259f234f85c2ed25f))
* **connectors:** account Parquet scan decode buffers in the memory pool ([#137](https://github.com/formosa-ventures/arneb/issues/137)) ([33d675c](https://github.com/formosa-ventures/arneb/commit/33d675c576c20749cbc7b43cde0969bbe1f3d2e1))
* TLS, Trino Basic auth, and cluster RPC authentication (Phase 3 P3) ([#136](https://github.com/formosa-ventures/arneb/issues/136)) ([f9d85df](https://github.com/formosa-ventures/arneb/commit/f9d85dff59c627039b5479241c9325538bf0f3ad))


### Bug Fixes

* admission review follow-ups ([#145](https://github.com/formosa-ventures/arneb/issues/145)) ([#150](https://github.com/formosa-ventures/arneb/issues/150)) ([2c29cba](https://github.com/formosa-ventures/arneb/commit/2c29cbadc7fb0c6d631292543d729a0f9cf0b252))
* admission review nits from [#150](https://github.com/formosa-ventures/arneb/issues/150) ([#152](https://github.com/formosa-ventures/arneb/issues/152)) ([166247a](https://github.com/formosa-ventures/arneb/commit/166247a266b6dd98957b70cd847156fd764d7cf5))
* **connectors:** scan memory accounting follow-ups from [#137](https://github.com/formosa-ventures/arneb/issues/137) ([#148](https://github.com/formosa-ventures/arneb/issues/148)) ([6654007](https://github.com/formosa-ventures/arneb/commit/6654007b90853f9735560f90d9d9ba7c334882a5))
* **rpc:** refuse a broadcast resume past the end; cover the mid-batch resume ([#153](https://github.com/formosa-ventures/arneb/issues/153)) ([a7a9aeb](https://github.com/formosa-ventures/arneb/commit/a7a9aeb8a8e622aae88bb717d218f023295545eb))
* **rpc:** resume a broadcast exchange read after a transport break ([#140](https://github.com/formosa-ventures/arneb/issues/140)) ([4681f79](https://github.com/formosa-ventures/arneb/commit/4681f79784a3ee2fee96a16ea0c132fd6b61bbca))
* **rpc:** resume a broken broadcast read from the first undelivered row ([#147](https://github.com/formosa-ventures/arneb/issues/147)) ([dba1074](https://github.com/formosa-ventures/arneb/commit/dba107499264cad24012b5014232dda7f128281a))
* security review follow-ups ([#145](https://github.com/formosa-ventures/arneb/issues/145)) ([#149](https://github.com/formosa-ventures/arneb/issues/149)) ([135594b](https://github.com/formosa-ventures/arneb/commit/135594b45cd17c99b1e2b099cc8c838c4f28ec7b))
* **trino:** bound the auth verification queue and dedupe concurrent misses ([#151](https://github.com/formosa-ventures/arneb/issues/151)) ([13f2aae](https://github.com/formosa-ventures/arneb/commit/13f2aae0d12b29c885ce735613e632c3763d0219))

## [0.1.2](https://github.com/formosa-ventures/arneb/compare/v0.1.1...v0.1.2) (2026-10-15)


### Bug Fixes

* **compose:** initialize HMS Derby at build time for fresh hms-db volumes ([#125](https://github.com/formosa-ventures/arneb/issues/125)) ([1a12cd8](https://github.com/formosa-ventures/arneb/commit/1a12cd83ca76395549b1088dcf447ca8ae51ab35))
* **execution:** map cross-fragment filter consumers by source column ([#131](https://github.com/formosa-ventures/arneb/issues/131)) ([55a776f](https://github.com/formosa-ventures/arneb/commit/55a776f0ead63a9f769b336e03be01a7c6b41ab2))


### Performance Improvements

* **planner,server:** flip TPC-H q18 by building the smaller join side and parallelizing the IN-subquery final aggregate ([#118](https://github.com/formosa-ventures/arneb/issues/118)) ([6499e68](https://github.com/formosa-ventures/arneb/commit/6499e68b3b7c6ccd7c25e55e72724cfa8fa92b09))
* **planner:** default hash-partitioned semi/anti joins on (q21 -21%, q04 -22%, q22 -20% @ SF10) ([#127](https://github.com/formosa-ventures/arneb/issues/127)) ([fd4d292](https://github.com/formosa-ventures/arneb/commit/fd4d292bc0bee3a141c3a44934b014658d15fd91))
* **planner:** pre-filter decorrelated aggregates by the outer semi-join key set ([#123](https://github.com/formosa-ventures/arneb/issues/123)) ([7afe347](https://github.com/formosa-ventures/arneb/commit/7afe347639813b2b62a92be6da83ad0f0124430b))
* probe-side dynamic filters for partitioned semi/anti joins (q21 0.8x → 2.0x vs Trino @ SF10) ([#129](https://github.com/formosa-ventures/arneb/issues/129)) ([07adde7](https://github.com/formosa-ventures/arneb/commit/07adde7507ec7423331f58768b21b758a3245478))

## [0.1.1](https://github.com/formosa-ventures/arneb/compare/v0.1.0...v0.1.1) (2026-10-10)


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
* **hive:** read ORC tables and partitioned Hive tables ([#113](https://github.com/formosa-ventures/arneb/issues/113)) ([f633055](https://github.com/formosa-ventures/arneb/commit/f6330558fd2d6d8379c2d5101794ed7525ab4e4c))
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

* align SUM/AVG result types with Trino and support decimal/timestamp DISTINCT group keys ([#91](https://github.com/formosa-ventures/arneb/issues/91)) ([0169b72](https://github.com/formosa-ventures/arneb/commit/0169b7222f3bd8b594539eed0db3975a98020793))
* **benchmark:** correct local sf001 parquet types ([#41](https://github.com/formosa-ventures/arneb/issues/41)) ([c374fb7](https://github.com/formosa-ventures/arneb/commit/c374fb72a6268b38b2ee4b064a51254f379126c7))
* **ci:** patch h2/rustls advisories and clippy 1.99 double_must_use ([#87](https://github.com/formosa-ventures/arneb/issues/87)) ([45add6c](https://github.com/formosa-ventures/arneb/commit/45add6cf9bda827abdef678ddfd34166582c55d0))
* **docker:** pull MinIO images from quay.io ([#73](https://github.com/formosa-ventures/arneb/issues/73)) ([91aac59](https://github.com/formosa-ventures/arneb/commit/91aac59dac1d978e68e5bb6f86931d62067f06e0))
* **docs:** remove root CNAME that triggers Jekyll fallback ([e98d294](https://github.com/formosa-ventures/arneb/commit/e98d2946f6d3b3307a4afdf9cba131eba804ecbd))
* **execution:** emit typed NULLs from aggregates over empty input ([#82](https://github.com/formosa-ventures/arneb/issues/82)) ([8642801](https://github.com/formosa-ventures/arneb/commit/864280152905d0ed1a2b0799482a8863f208f810))
* **execution:** NULL join conditions, decimal SUM overflow, window decimal aggregates ([#94](https://github.com/formosa-ventures/arneb/issues/94)) ([c865ec1](https://github.com/formosa-ventures/arneb/commit/c865ec1b4531875a289f810428d57d6c6b116e45))
* **execution:** SUM/AVG over TINYINT/SMALLINT ([#102](https://github.com/formosa-ventures/arneb/issues/102)) ([bf9005e](https://github.com/formosa-ventures/arneb/commit/bf9005e8fc594928bcd0808c54c7b71ff015efff)), closes [#93](https://github.com/formosa-ventures/arneb/issues/93)
* **execution:** use Kleene three-valued logic for AND/OR ([#81](https://github.com/formosa-ventures/arneb/issues/81)) ([5a74191](https://github.com/formosa-ventures/arneb/commit/5a7419161586f4513aead86f11f8a7cae446910a))
* **execution:** window aggregates return Trino result types ([#108](https://github.com/formosa-ventures/arneb/issues/108)) ([6acd213](https://github.com/formosa-ventures/arneb/commit/6acd213f7c0ce8adfa8dccc25c2a751bb5b41135))
* **hive:** resolve table location per scan, not from a name-keyed cache ([#112](https://github.com/formosa-ventures/arneb/issues/112)) ([558f928](https://github.com/formosa-ventures/arneb/commit/558f928b04eece95e034800967a7c3b32a8572bd))
* **planner,execution:** Trino decimal arithmetic result types ([#100](https://github.com/formosa-ventures/arneb/issues/100)) ([8928952](https://github.com/formosa-ventures/arneb/commit/892895226362f39a320eec4e4bab9cfa0c1f849f))
* **planner:** coerce untyped NULL operands and stop folding overflowing ints ([#90](https://github.com/formosa-ventures/arneb/issues/90)) ([ce94267](https://github.com/formosa-ventures/arneb/commit/ce942677ccdb1a473e84bb6f08c4c86a7a4fc7dc))
* **planner:** fold comparisons with a NULL literal to NULL ([#83](https://github.com/formosa-ventures/arneb/issues/83)) ([2452024](https://github.com/formosa-ventures/arneb/commit/2452024d4a094af3a127beb9b0c3f142aa9a4a0d))
* **planner:** honor SELECT DISTINCT and ORDER BY/GROUP BY ordinals ([#105](https://github.com/formosa-ventures/arneb/issues/105)) ([f0d3e5e](https://github.com/formosa-ventures/arneb/commit/f0d3e5e6d1e17995e1fee56ad29e7f18cb9731b8))
* **planner:** plan SQL window functions into Window nodes ([#106](https://github.com/formosa-ventures/arneb/issues/106)) ([fcc334f](https://github.com/formosa-ventures/arneb/commit/fcc334ff04142784cde9edd0796f29d8ceb3da93)), closes [#104](https://github.com/formosa-ventures/arneb/issues/104)
* **planner:** POWER/POW return DOUBLE for integer arguments ([#109](https://github.com/formosa-ventures/arneb/issues/109)) ([2630a42](https://github.com/formosa-ventures/arneb/commit/2630a421df31d46d6fae324361b78644af714165))
* **pr30:** CI failures, TPC-H correctness, auto-gen rustfmt skip ([#31](https://github.com/formosa-ventures/arneb/issues/31)) ([4a8221d](https://github.com/formosa-ventures/arneb/commit/4a8221df52d9b0a755de86d4b3cac916be4744c9))
* **protocol:** hide empty schemas from metadata queries ([#27](https://github.com/formosa-ventures/arneb/issues/27)) ([8f45210](https://github.com/formosa-ventures/arneb/commit/8f452101ab877cf358b8500f78d7f2f3a2431a45))
* **protocol:** honor SET search_path per pgwire connection ([#101](https://github.com/formosa-ventures/arneb/issues/101)) ([e5aa0c9](https://github.com/formosa-ventures/arneb/commit/e5aa0c996f08c28e847d64dcb26310f514444963))
* **protocol:** send NULL-typed columns as SQL NULL over pgwire ([#110](https://github.com/formosa-ventures/arneb/issues/110)) ([6c877e2](https://github.com/formosa-ventures/arneb/commit/6c877e296445e549df15358c006266eeaeb99b43))
* **release:** tag releases as vX.Y.Z without the component name ([#89](https://github.com/formosa-ventures/arneb/issues/89)) ([28b1751](https://github.com/formosa-ventures/arneb/commit/28b1751e4c4d3506f09123414abbcc95e24d5f3e))
* **release:** use simple release type with workspace version jsonpath ([#86](https://github.com/formosa-ventures/arneb/issues/86)) ([0033ab4](https://github.com/formosa-ventures/arneb/commit/0033ab4c005074fa6503e2ee514e5c9756e8657c))
* **sql-parser:** type decimal literals as Trino DECIMAL(p,s) ([#103](https://github.com/formosa-ventures/arneb/issues/103)) ([1fab4b4](https://github.com/formosa-ventures/arneb/commit/1fab4b4e2c169c7a28eaed7caf3b3a8faa4f71a0))


### Performance Improvements

* **execution:** TPC-H 16/16 vs Trino + 1.17x quick-win speedup ([#47](https://github.com/formosa-ventures/arneb/issues/47)) ([06329c0](https://github.com/formosa-ventures/arneb/commit/06329c063b28e1c30f59f5f43527dfb9e26c340c))
* ship the validated TPC-H config and fix the estimate behind it ([#74](https://github.com/formosa-ventures/arneb/issues/74)) ([f7e46e2](https://github.com/formosa-ventures/arneb/commit/f7e46e28f94c4fde52b9ada989a1c6689dab8702))
* TPC-H engine optimizations and arneb-vs-Trino benchmark ([#62](https://github.com/formosa-ventures/arneb/issues/62)) ([aab31fb](https://github.com/formosa-ventures/arneb/commit/aab31fb46423574d6150342634610cca5e4c1474))


### Code Refactoring

* rename project from trino-alt to Arneb ([b9a52cf](https://github.com/formosa-ventures/arneb/commit/b9a52cf37565d7fb51bec993415e353564b5791a))

## Changelog
