## Why

With coordinator-worker architecture, RPC layer, and plan fragmentation in place, we need the actual distributed execution operators. Shuffle (hash-partitioned data exchange), broadcast (small table replication), and merge (sorted stream combination) are the building blocks that make joins, aggregations, and sorts work across multiple workers.

## What Changes

- Implement ShuffleWriteOperator: hash-partitions output by specified columns into OutputBuffer partitions
- Implement BroadcastOperator: replicates full output to all downstream partitions
- Implement MergeOperator: merges multiple sorted streams into one sorted stream (for distributed ORDER BY)
- Implement distributed hash join strategy: broadcast small side or hash-partition both sides
- Implement distributed aggregation: partial aggregate → shuffle → final aggregate
- Implement distributed sort: local sort → merge on coordinator
- Add distribution strategy selection based on TableStatistics and configurable thresholds

## Capabilities

### New Capabilities

- `shuffle-write-operator`: Hash-partitioned output writing to OutputBuffer
- `broadcast-operator`: Full data replication to all partitions
- `merge-operator`: Sorted merge of multiple remote streams
- `distributed-join`: Strategy selection and execution for distributed hash joins
- `distributed-aggregation`: Two-phase partial/final aggregation across workers
- `distributed-sort`: Local sort + merge exchange
- `distribution-strategy`: Broadcast vs partition decision logic based on statistics

### Modified Capabilities

- `execution-operators`: Add ShuffleWriteOperator, BroadcastOperator, MergeOperator
- `physical-planner`: Generate distributed physical plans from fragmented logical plans

## Impact

- **Crates**: execution, rpc, scheduler
- **Configuration**: broadcast_join_max_rows threshold (default 10000)
