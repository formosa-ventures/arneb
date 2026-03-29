## Context

After Changes 7 (flight-rpc) and 8 (coordinator-worker), the engine can execute fragments on workers and exchange data. Now we need operators that control HOW data flows between workers.

## Goals / Non-Goals

**Goals:**

- Three exchange operators: shuffle, broadcast, merge
- Distributed strategies for join, aggregation, sort
- Statistics-based strategy selection with fallback defaults
- End-to-end distributed query execution

**Non-Goals:**

- Dynamic partition pruning — Phase 3
- Adaptive query execution (changing strategy mid-query)
- Spill to disk for shuffle — in-memory only

## Decisions

### D1: ShuffleWriteOperator

**Choice**: Hashes each row's partition columns using murmur3, takes modulo num_partitions, writes batch slice to corresponding OutputBuffer partition. Produces no output stream (terminal operator on worker).

**Rationale**: Murmur3 is fast and provides good distribution. Modulo partitioning is simple and deterministic. Terminal operator model matches the worker execution lifecycle where shuffle output is consumed by exchange clients on other workers.

### D2: BroadcastOperator

**Choice**: Collects all input batches, then writes complete copy to each of N output buffer partitions. Used for small dimension tables in joins.

**Rationale**: Full replication avoids shuffle overhead for small tables. The memory cost is bounded by the broadcast threshold.

### D3: MergeOperator

**Choice**: K-way merge using BinaryHeap on sort keys. Each input is an ExchangeClient stream. Outputs merged sorted stream.

**Rationale**: BinaryHeap-based merge is O(N log K) where K is the number of streams. This is optimal for merging pre-sorted streams.

### D4: Distributed join strategy

**Choice**: If smaller side has row_count < broadcast_join_max_rows (from TableStatistics) → broadcast. Otherwise → hash-partition both sides on join keys. Default threshold: 10,000 rows.

**Rationale**: Broadcast avoids shuffle overhead for small tables. Hash-partition scales for large tables. The threshold is configurable to allow tuning.

**Alternative**: Always hash-partition. Rejected because broadcasting small dimension tables is significantly faster.

### D5: Distributed aggregation

**Choice**: Always two-phase. Phase 1 (workers): PartialHashAggregate with same group-by keys, producing partial results. Exchange: shuffle on group-by keys. Phase 2: FinalHashAggregate combining partial results.

**Rationale**: Two-phase aggregation reduces data volume before shuffle. Partial aggregates compress many rows into few groups, minimizing network transfer.

### D6: Distributed sort

**Choice**: Workers sort locally. Coordinator uses MergeOperator to merge N sorted streams.

**Rationale**: Local sort leverages worker parallelism. K-way merge on coordinator is efficient since each stream is already sorted.

## Risks / Trade-offs

**[Broadcast memory]** → Broadcasting a large table wastes memory. **Mitigation**: Statistics-based threshold limits broadcast to small tables. Default 10,000 rows is conservative.

**[Shuffle skew]** → Uneven hash distribution can cause hotspots. **Mitigation**: Monitoring needed but no mitigation in this change. Future work: range partitioning or adaptive repartitioning.

**[In-memory only]** → Large shuffles may exhaust worker memory. **Mitigation**: MVP operates on moderate datasets. Spill-to-disk is a Phase 3 concern.
