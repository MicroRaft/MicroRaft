# MicroRaft Audit: Blocking, Buffering, and Performance Issues

Date: 2026-07-28

Law: Durability and correctness are the top priority for a consensus library. Any performance optimizations must preserve correctness and durability guarantees.

Summary

This document lists bugs, performance pitfalls, and unoptimized areas found during a code review focused on small buffers, thread blocking, and inefficient wire/store handling. Each issue includes location, the problem, and a recommended mitigation that preserves durability and correctness.

1) SQLite durability settings and frequent commits

- Location: microraft-store-sqlite/src/main/java/.../RaftSqliteStore.java
- Problem: The store sets PRAGMA SYNCHRONOUS=EXTRA and EXCLUSIVE locking, and the code issues many immediate commits (dsl.connection(Connection::commit)) across small operations.
- Risk: High fsync and latency on write-heavy paths (AppendEntries). This preserves durability but can severely impact throughput.
- Recommendation: Keep durable defaults but expose PRAGMA tunables and coalesce commits where safe. Implement or ensure leader-side flush coalescing (batch multiple persistLogEntries calls before commit) while keeping correctness: only acknowledge entries when they are durable per the Raft durability policy.

2) Full restore loads all blobs into memory

- Location: RaftSqliteStore.getRestoredRaftState(...) reads and deserializes all log entries and snapshot chunks into memory.
- Problem: For large logs/snapshots this spikes memory and GC pressure during startup.
- Risk: OOM or long GC pauses at restart.
- Recommendation: Provide streaming or paginated restore and/or prune on-disk before in-memory reconstruction. Truncation (truncateStaleData) helps, but offer an option to lazily load log entries or to limit in-memory copies during restore.

3) Serialization overhead for DB BLOBs

- Location: RaftSqliteStore.JooqConverterAdapter -> serializer.serialize()/deserialize(); tests use JacksonSerializer.
- Problem: Every DB read/write serializes/deserializes whole objects (byte[] allocations). Jackson in tests is especially alloc-heavy.
- Risk: CPU and allocation pressure on hot paths.
- Recommendation: Consider a compact binary serializer for hot paths, reuse buffers where possible, and avoid unnecessary full-object reads when only metadata is needed. Keep tests simple but note test serializer differs from production.

4) ArrayRingbuffer truncation cost

- Location: impl/util/ArrayRingbuffer.java and RaftLog.truncateLogEntriesUntil
- Problem: Truncate loops set slots to null individually; for very large truncations this is O(n) work.
- Recommendation: Use Arrays.fill on contiguous ranges where possible, or swap backing array and reset indices for large truncations to reduce per-slot work. Ensure correctness of sequence indices.

Minor/Test-only notes

- Thread.sleep/interrupt usage appears in tests only.

Next steps

- Add configuration knobs for SQLite commit/coalescing and implement leader-side batching that still guarantees durability before acknowledging commits.
- Consider incremental restore API and review serializers used in production vs tests.

If desired, a pull request can be prepared to add a config option + simple coalescing commit in RaftSqliteStore. All changes will preserve durability and correctness as the highest priority.
