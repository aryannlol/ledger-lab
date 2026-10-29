# LedgerLab — Viva Presentation & Demonstration Guide

**Duration**: 5–7 Minutes  
**Goal**: Clearly explain the ISM challenge, demonstrate the working system live, walk through failure recovery and reconciliation, and defend technical choices during questioning.

---

## 1. Step-by-Step Live Demonstration Script

| Time | Phase | Action / Screen | What to Say to the Evaluator |
| :--- | :--- | :--- | :--- |
| **0:00 - 1:00** | **Introduction & Problem** | Header / System Overview | *"LedgerLab simulates large-scale financial ledger migration inspired by Uber's LedgerStore. In financial systems, moving records cannot rely on simple row counts — an incomplete write or a single altered rupee causes silent corruption."* |
| **1:00 - 2:00** | **Step 0: Data Generation** | Step 0 Banner (Top of Migration Stage) | *"First, we generate 1,000 synthetic transactions. Double-entry bookkeeping creates 2 ledger entries per transaction (1 debit, 1 credit), giving us 2,000 rows in our Source database. Each record has a canonical SHA-256 hash and integer amounts in paise to prevent floating-point rounding errors."* |
| **2:00 - 3:30** | **Migration & Controlled Crash** | Tab 1: Migration Stage | *"We start migration in batches of 200, simulating a worker crash at Batch 3 before COMMIT. Notice that Batch 3's unsaved rows roll back cleanly — zero corrupted rows enter the Destination. We then click 'RESUME', and the engine uses the checkpoint bookmark in metadata.db to resume via keyset cursor (`WHERE entry_id > :last_id`) without starting over or creating duplicates."* |
| **3:30 - 4:45** | **Tamper Injection & 4-Level Audit** | Tab 2: Reconciliation Stage | *"To prove row counts are insufficient, we click 'SIMULATE: DELETE A ROW' — Row #0452 shatters into pieces. Then we click 'RUN AUDIT'. Rather than a slow row-by-row scan, our 4-level funnel runs: Count Check flags 1 missing row, Chunk Hash Radar sweeps and flags Chunk #3 in red, and Bisection drilldown binary-searches to isolate row #0452 in ~8 steps. Clicking 'REPAIR' recovers the genuine record from Source DB and logs an immutable audit trail entry."* |
| **4:45 - 5:30** | **Raw Records Verification & Conclusion** | Tab 3: View Ledger Records | *"Finally, Tab 3 lets evaluators inspect the physical SQLite tables to verify that debit/credit balances and SHA-256 digests match bit-for-bit across both databases. LedgerLab proves migration is an integrity discipline, not a bulk copy."* |

---

## 2. Likely Viva Questions & Model Answers

### Q1: Why use separate SQLite database files instead of tables in a single database?
> **Answer**: In real-world migrations, the source and destination are separate storage clusters (often across different datacenters or tiers). Using separate files (`source.db`, `destination.db`, `metadata.db`) enforces real filesystem isolation, separate connection handles, and independent locking boundaries, preventing artificial transaction sharing.

### Q2: Why store monetary values in paise rather than float or double?
> **Answer**: Standard IEEE 754 floating-point numbers cannot accurately represent base-10 fractions (e.g., `0.1 + 0.2 != 0.3`). In financial storage, using fixed-precision integer units (paise) completely eliminates rounding errors and guarantees exact balance reconciliation.

### Q3: How do checkpoints prevent duplicates upon worker resumption?
> **Answer**: Each checkpoint is committed only after the destination writes for that batch succeed. When resuming, the engine queries the checkpoint log for the last successful batch $N$, and offsets the source reader to batch $N+1$. In addition, the destination uses `INSERT OR IGNORE` on primary keys to provide idempotency.

### Q4: Why isn't a row count comparison sufficient for data integrity?
> **Answer**: A source and destination could both report 10,000 records if one record was missing but another was accidentally duplicated. Furthermore, a record could be present with an altered monetary amount or mismatched account reference. LedgerLab computes record-level SHA-256 hashes to guarantee content integrity down to the individual bit.

### Q5: How does your repair mechanism follow the "Append-Only" principle?
> **Answer**: In a financial ledger, you must never execute a destructive `UPDATE` or `DELETE` on historical entries because that destroys the audit trail. When LedgerLab repairs a mismatched entry, it inserts a new compensating transaction referencing the original `reference_entry_id`, preserving full historical traceability.

### Q6: Why is a crash between COMMIT and CHECKPOINT write completely safe?
> **Answer**: `destination.db` and `metadata.db` are separate physical databases, so committing data and writing the checkpoint cannot happen in a single atomic transaction. If the worker crashes *after* `destination.db` commits but *before* `metadata.db` records the checkpoint, the new worker re-runs that batch on resume. Because destination writes use `INSERT OR IGNORE` keyed on primary key (`entry_id`), the duplicate inserts are safely ignored (`ignored_duplicates = N`), the checkpoint is saved, and zero corruption occurs.

### Q7: Why do chunk hashes beat row-by-row comparison?
> **Answer**: Row-by-row comparison requires transmitting and evaluating every single column of millions of records across the network ($O(N)$ payload transfer). By computing a canonical SHA-256 hash for chunks of 1,000 records, matching chunks are verified in a single comparison and skipped without reading individual rows. This reduces network payload by over 99% while maintaining 100% cryptographic certainty.

### Q8: How does Bisection isolate an anomaly in only log₂(N) steps?
> **Answer**: When a chunk hash mismatch is detected, rather than scanning all rows linearly, the Auditor applies binary search (the Merkle tree principle): it compares the top half vs bottom half hashes, identifies which half contains the mismatch, and recurses. For a chunk of 1,000 rows, bisection pinpoints the exact corrupted row in $\lceil \log_2(1000) \rceil = 10$ comparisons instead of 1,000.

### Q9: Why use Keyset Pagination instead of SQL OFFSET?
> **Answer**: `OFFSET :n` forces the database engine to scan and discard $:n$ rows on disk for every page, leading to $O(N^2)$ cumulative read latency. Worse, if rows are inserted or deleted during a long migration, `OFFSET` skips or duplicates records. Keyset pagination (`WHERE entry_id > :last_id ORDER BY entry_id ASC LIMIT :batch_size`) uses the B-Tree primary index in $O(\log N)$ time and remains strictly deterministic under concurrent changes.

