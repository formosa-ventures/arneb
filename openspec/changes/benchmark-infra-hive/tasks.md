## 1. Docker Compose Trino Service

- [x] 1.1 Create docker/trino/catalog/tpch.properties (connector.name=tpch)
- [x] 1.2 Create docker/trino/catalog/hive.properties (connector.name=hive, metastore URI, MinIO S3 config)
- [x] 1.3 Create docker/trino/catalog/tpcds.properties (connector.name=tpcds)
- [x] 1.4 Add trino service to docker-compose.yml (trinodb/trino:latest, port 8080, mount catalog dir, depends_on hive-metastore healthy)
- [x] 1.5 Add health check for trino service (curl /v1/info)
- [x] 1.6 Verify Trino can connect to HMS and MinIO (docker compose up, query tpch.sf1.nation)

## 2. TPC-H Seed Service

- [x] 2.1 Create docker/tpch-seed/seed.sh (Trino CLI CTAS for 8 TPC-H tables: lineitem, orders, customer, part, partsupp, supplier, nation, region)
- [x] 2.2 Seed script drops hive.tpch schema if exists, then creates schema and runs CTAS from tpch.sf1 into hive.tpch
- [x] 2.3 Add tpch-seed service to docker-compose.yml (depends_on trino healthy, runs seed.sh)
- [x] 2.4 Support configurable scale factor via environment variable (default sf1)
- [x] 2.5 Verify: docker compose run tpch-seed creates 8 tables with Parquet in MinIO and metadata in HMS

## 3. TPC-DS Seed Service

- [x] 3.1 Create docker/tpcds-seed/seed.sh (Trino CLI CTAS for 24 TPC-DS tables from tpcds.sf1 into hive.tpcds)
- [x] 3.2 Add tpcds-seed service to docker-compose.yml (depends_on trino healthy)
- [x] 3.3 Verify: docker compose run tpcds-seed creates 24 tables in MinIO + HMS

## 4. Arneb Hive Config

- [x] 4.1 Create benchmarks/tpch/tpch-hive.toml with [[catalogs]] pointing to HMS, [storage.s3] for MinIO
- [x] 4.2 Verify Arneb can query TPC-H tables via Hive catalog (e.g., SELECT COUNT(*) FROM datalake.tpch.lineitem)

## 5. Benchmark Flow Update

- [x] 5.1 Rewrite benchmarks/tpch/scripts/run_benchmark.sh for Docker Compose workflow (up -d, run seed, run bench, report)
- [x] 5.2 Update benchmarks/tpch/scripts/run_trino.sh to use Docker Compose Trino on port 8080
- [x] 5.3 Update benchmarks/tpch/README.md with new setup and run instructions

## 6. Cleanup

- [x] 6.1 Delete benchmarks/tpch/scripts/generate_parquet.py
- [x] 6.2 Delete benchmarks/tpch/scripts/generate_data.sh
- [x] 6.3 Delete crates/server/src/bin/hive_demo_setup.rs
- [x] 6.4 Remove [[bin]] name = "hive-demo-setup" from crates/server/Cargo.toml
- [x] 6.5 Delete scripts/arneb-hive-demo.toml (replaced by tpch-hive.toml)
- [x] 6.6 Remove hive_demo_setup.rs references from documentation (CLAUDE.md, docs/, README.md)
