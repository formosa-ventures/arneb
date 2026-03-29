## 1. ShuffleWriteOperator

- [x] 1.1 Implement hash partitioning logic (murmur3 on partition columns, modulo num_partitions)
- [x] 1.2 Implement ShuffleWriteOperator that writes batches to partitioned OutputBuffer
- [x] 1.3 Handle null partition keys (assign to partition 0)
- [x] 1.4 Write tests with known hash distribution

## 2. BroadcastOperator

- [x] 2.1 Implement BroadcastOperator that replicates input to all output partitions
- [x] 2.2 Write tests verifying all partitions receive identical data

## 3. MergeOperator

- [x] 3.1 Implement K-way sorted merge using BinaryHeap
- [x] 3.2 Handle multiple sort keys with ASC/DESC/NULLS FIRST/LAST
- [x] 3.3 Write tests with pre-sorted inputs and verify merged order

## 4. Distribution Strategy

- [x] 4.1 Define DistributionStrategy enum: Broadcast, HashPartition
- [x] 4.2 Implement strategy selection based on TableStatistics and threshold
- [x] 4.3 Add broadcast_join_max_rows to ServerConfig (default 10000)
- [x] 4.4 Write tests for strategy selection logic

## 5. Distributed Join

- [x] 5.1 Update PlanFragmenter to generate broadcast fragments for small-table joins
- [x] 5.2 Update PlanFragmenter to generate hash-partitioned fragments for large-table joins
- [x] 5.3 Generate physical plans with ShuffleWrite + Exchange + HashJoin
- [x] 5.4 Write tests for both broadcast and partitioned join strategies

## 6. Distributed Aggregation

- [x] 6.1 Implement PartialHashAggregateExec (same as HashAggregate but outputs partial results)
- [x] 6.2 Implement FinalHashAggregateExec (combines partial aggregate results)
- [x] 6.3 Update PlanFragmenter for two-phase aggregation with shuffle on group keys
- [x] 6.4 Write tests for distributed aggregation with GROUP BY

## 7. Distributed Sort

- [x] 7.1 Generate local SortExec on workers + MergeOperator on coordinator
- [x] 7.2 Write tests for distributed ORDER BY

## 8. End-to-End Integration

- [x] 8.1 Multi-worker test: distributed JOIN query
- [x] 8.2 Multi-worker test: distributed GROUP BY query
- [x] 8.3 Multi-worker test: distributed ORDER BY query
- [x] 8.4 Verify single-node (standalone) mode still works

## 9. Quality

- [x] 9.1 `cargo build` compiles without warnings
- [x] 9.2 `cargo test` — all tests pass
- [x] 9.3 `cargo clippy -- -D warnings` — clean
- [x] 9.4 `cargo fmt -- --check` — clean
