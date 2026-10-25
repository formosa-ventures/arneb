## 1. pgwire

- [x] 1.1 Transaction statements as no-ops; `SHOW` values; `version()`; startup parameters from the same values; `current_user`
- [x] 1.2 Bind parameters decoded by declared type and format; undecodable binary types refused

## 2. Planner and Trino protocol

- [x] 2.1 `CatalogManager::fold_reference_case`, used before table resolution; unit test
- [x] 2.2 3- and 4-part column references; parser test
- [x] 2.3 `INNER` / `LEFT JOIN … USING`; quoted `USING` columns
- [x] 2.4 `current_catalog` / `current_schema`; quoted `"system"."metadata"`
- [x] 2.5 `crates/protocol/tests/client_compat.rs` end to end on both protocols

## 3. Smoke harness

- [x] 3.1 `scripts/client-smoke/{cluster.sh, clients.py, run.sh, requirements.txt, dbt/}`; `gen_tpch.py` rerun fix
- [x] 3.2 CI: release build uploads the binary; `Cluster smoke` job
- [x] 3.3 Run `run.sh` locally on TPC-H SF1 (Tier 2)

## 4. Docs

- [x] 4.1 `docs/guide/clients.md`, sidebar, `trino-clients.md`, `functions.md`
