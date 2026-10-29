# LedgerLab: Financial Ledger Migration and Data Integrity Simulator
## Case Study & Academic Project Report

**Course**: Information Storage and Management (ISM)  
**Subject Focus**: Cloud Storage, Data Availability, and Integrity Verification  
**Industry Context**: Financial Data Migration Inspired by Uber Engineering Publications  

---

## 1. Executive Summary

In enterprise fintech and ride-hailing platforms, financial ledgers record billions of transactions representing rider payments, driver payouts, platform fees, and refunds. As storage systems evolve or migrate to tiered storage, copying historical records without silent data corruption, duplicates, or missing entries becomes a paramount challenge.

**LedgerLab** is a specialized simulator built to demonstrate how financial records can be moved between distinct databases while maintaining data completeness, correctness, and consistency. Using an append-only architecture, atomic batching, persistent checkpoints, and cryptographic reconciliation, LedgerLab validates that migrations can survive unexpected crashes and detect tampered or corrupted records.

---

## 2. Problem Statement

A naive approach to database migration treats the operation as a monolithic file copy or bulk database dump. In production environments, this fails for several critical reasons:
1. **Network or Node Interruptions**: If a migration worker fails after copying 60% of records, restarting from scratch introduces massive I/O overhead and lock contention, while resuming naively risks duplicate rows.
2. **Silent Data Corruption**: Record counts alone are deceptive. A source and target database may both contain 100,000 records, but a single bit flip or truncated decimal in a financial ledger can cause severe accounting discrepancies.
3. **Audit History Destruction**: Overwriting records directly destroys audit trails. Financial compliance requires that errors be corrected through compensating append-only entries.

---

## 3. Real-World Case Study: Uber LedgerStore

Uber's infrastructure engineering team published detailed accounts of building **LedgerStore**, an append-only ledger storage system designed for extreme scale and auditable financial accuracy. Key insights from Uber's architecture that inspired LedgerLab:
- **Append-Only Immutability**: Historical ledger records are never modified in-place. All state updates are signed, immutable log entries.
- **Strict Reconciliation**: Continual background reconciliation jobs verify that secondary indexes and historical cold storage match the primary ledger down to the cryptographic hash.
- **Progress Tracking**: Long-running backfills and migrations rely on durable checkpoints so interrupted tasks resume safely without duplicating data.

LedgerLab demonstrates these foundational principles using an accessible, lightweight local architecture.

---

## 4. Experimental Results & Analysis

LedgerLab's experiment suite executes four controlled experiments producing measurable empirical data:

### Experiment A: Dataset Scalability
Evaluated migration throughput and duration across growing dataset sizes with a constant batch size of 1,000 entries.

| Dataset Size (Txns) | Total Entries | Migration Time (ms) | Throughput (rec/sec) | Reconciliation Status | Discrepancies |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1,000 | 2,000 | 42.7 ms | 46,875 | PASS | 0 |
| 5,000 | 10,000 | 205.1 ms | 48,752 | PASS | 0 |
| 10,000 | 20,000 | 409.1 ms | 48,883 | PASS | 0 |

**Finding**: Throughput scales efficiently with dataset size as SQLite WAL (Write-Ahead Logging) amortization stabilizes over larger transaction sets.

### Experiment B: Failure Recovery & Checkpointing
A migration of 10,000 entries with batch size 1,000 was subjected to an intentional worker crash at Batch 4.
- **Work completed before crash**: 3,000 entries (Batches 1–3).
- **Recorded Checkpoints**: Exactly 3 confirmed commits in `metadata.db` (Batch 4 flagged pending/aborted).
- **Resumption**: Re-opened at Batch 4 without re-executing Batches 1–3. Completed all 10 batches.
- **Final Reconciliation**: **PASS** (Missing: 0, Duplicates: 0).
- **Hypothesis Confirmed**: Persistent checkpointing prevents redundant I/O and guarantees zero data loss and zero duplicates on recovery.

### Experiment C: Tamper Detection & Cryptographic Integrity
Controlled faults were injected into the destination storage:
- 3 random records were deleted (IDs: 5157, 248, 9758).
- 2 records had their `debit_amount` field tampered (+₹9.99; IDs: 6315, 177).
- **Reconciliation Audit**: Instantly flagged 3 missing records and 2 hash mismatches. Total discrepancies: 5.
- **Repair Phase**: Missing records were safely re-ingested; altered records were restored and supplemented with immutable compensating audit entries referencing `reference_entry_id`.
- **Post-Repair Status**: **PASS** (0 discrepancies).

### Experiment D: Batch Size Optimization
Evaluating performance on a fixed 10,000-entry dataset across batch sizes:

| Batch Size | Duration (ms) | Throughput (rec/sec) | Observation |
| :--- | :--- | :--- | :--- |
| 100 | 1,226.2 ms | 8,156 | High transaction commit and checkpoint overhead |
| 500 | 324.0 ms | 30,861 | Reduced I/O sync points |
| 1,000 | 223.3 ms | 44,781 | Optimal balance of memory & disk writes |
| 2,500 | 121.9 ms | 82,053 | High write throughput; low per-batch overhead |
| 5,000 | 100.0 ms | 100,032 | Peak raw throughput with larger memory buffer |

---

## 5. Limitations

1. **Local vs Distributed**: LedgerLab runs locally on SQLite rather than a distributed cloud datastore (such as AWS DynamoDB, GCP Spanner, or Cassandra).
2. **Simulated Failure Modes**: Focuses on worker interruptions and storage mutations; does not simulate split-brain network partitions.
3. **Single Process Worker**: Demonstrates sequential batch checkpointing; distributed multi-worker concurrency would require distributed consensus (e.g., Raft or ZooKeeper).

---

## 6. Conclusion

LedgerLab successfully demonstrates that storage migration is not merely a data copying exercise, but a data integrity discipline. By combining append-only ledgers, atomic batch commits, persistent checkpoints, and cryptographic reconciliation, storage engineers can move critical financial records with complete auditability, verifiable correctness, and deterministic crash recovery.

