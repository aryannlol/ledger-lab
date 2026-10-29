# LedgerLab — Technical Documentation

## 1. Information Storage & Management (ISM) Mapping

LedgerLab directly applies fundamental ISM principles:

| ISM Core Concept | Real-World Enterprise Equivalent | LedgerLab Implementation |
| :--- | :--- | :--- |
| **Data Integrity** | End-to-end checksumming & block-level audits | Record-level SHA-256 hash chaining across financial attributes (paise amounts, account refs, transaction IDs). |
| **Data Availability & Recovery** | Point-in-time recovery & write-ahead logging | Persistent checkpoint table tracking committed batch ranges; automatic retry from `last_checkpoint + 1`. |
| **Storage Infrastructure Isolation** | Dual-cluster replication / multi-region migration | Completely independent SQLite databases (`source.db`, `destination.db`, `metadata.db`). |
| **Batch Processing & I/O Optimization** | Chunked streaming ETL pipelines | Parameterized batch sizes with transactional atomic commits preventing lock contention. |
| **Auditability & Append-Only Storage** | Immutable WORM (Write Once, Read Many) ledgers | Discrepancies repaired via compensating ledger entries referencing original `reference_entry_id` without destructive mutations. |
| **Storage Monitoring & Telemetry** | Storage metrics, latency SLAs, and throughput | High-resolution performance timers (`time.perf_counter`) measuring migration duration and records/sec. |

---

## 2. Database Schema & Data Models

All monetary values are strictly represented as 64-bit integer units (**paise**, where 100 paise = 1 INR) to completely eliminate binary floating-point rounding errors common in IEEE 754 implementations.

### 2.1 Primary Ledger Schema (`source.db` & `destination.db`)

```sql
CREATE TABLE transactions (
    entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    transaction_type TEXT NOT NULL,
    entry_type TEXT NOT NULL,         -- 'debit' or 'credit'
    account_reference TEXT NOT NULL,
    debit_amount INTEGER NOT NULL DEFAULT 0,
    credit_amount INTEGER NOT NULL DEFAULT 0,
    reference_entry_id INTEGER,       -- NULL for originals; points to entry_id for corrections
    record_hash TEXT NOT NULL         -- SHA-256 hash of record fields
);
```

### 2.2 Metadata & Migration Schema (`metadata.db`)

#### `migration_jobs`
```sql
CREATE TABLE migration_jobs (
    job_id TEXT PRIMARY KEY,
    source_database TEXT,
    target_database TEXT,
    dataset_size INTEGER,
    batch_size INTEGER,
    status TEXT DEFAULT 'pending',     -- 'running', 'completed', 'failed'
    started_at TEXT,
    completed_at TEXT,
    duration_ms REAL,
    throughput REAL,
    inject_failure INTEGER DEFAULT 0,
    failure_at_batch INTEGER
);
```

#### `checkpoints`
```sql
CREATE TABLE checkpoints (
    checkpoint_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    batch_number INTEGER NOT NULL,
    start_record_id INTEGER,
    end_record_id INTEGER,
    status TEXT DEFAULT 'pending',     -- 'completed', 'pending'
    updated_at TEXT,
    FOREIGN KEY (job_id) REFERENCES migration_jobs(job_id)
);
```

#### `reconciliation_results`
```sql
CREATE TABLE reconciliation_results (
    result_id TEXT PRIMARY KEY,
    job_id TEXT,
    check_type TEXT,
    records_checked INTEGER DEFAULT 0,
    discrepancies_found INTEGER DEFAULT 0,
    discrepancies_repaired INTEGER DEFAULT 0,
    missing_records TEXT,              -- JSON serialized list of entry IDs
    duplicate_records TEXT,            -- JSON serialized list of entry IDs
    mismatched_records TEXT,           -- JSON serialized list of {entry_id, fields}
    hash_mismatches INTEGER DEFAULT 0,
    status TEXT,                       -- 'pass' or 'fail'
    checked_at TEXT
);
```

---

## 3. Cryptographic Integrity & Hashing

To ensure data cannot be silently modified in transit or resting in storage:
1. Every ledger row computes a SHA-256 digest over the pipe-delimited canonical string:
   $$\text{Hash} = \text{SHA256}(\text{txn\_id} \mid \text{timestamp} \mid \text{txn\_type} \mid \text{entry\_type} \mid \text{account\_ref} \mid \text{debit} \mid \text{credit} \mid \text{ref\_entry\_id})$$
2. During reconciliation, LedgerLab executes a two-phase check:
   - **Existence Check**: Identifies missing IDs in Destination or extraneous IDs not originating in Source.
   - **Content & Hash Check**: Recalculates the SHA-256 digest on both sides. Even if row count and IDs match, a single altered digit in `debit_amount` or `account_reference` causes a hash mismatch and is flagged.

---

## 4. Failure Recovery & Resumption Flow

1. **Atomic Commits**: Destination writes occur inside a transaction block for each batch.
2. **Post-Commit Checkpointing**: The checkpoint record is updated to `completed` **only after** `destination.db` successfully commits.
3. **Resumption Strategy**: Upon worker crash or user interruption:
   - Query `checkpoints` for the highest `batch_number` where `status = 'completed'`.
   - Calculate `next_batch = last_batch + 1` and corresponding offset.
   - Continue processing without re-inserting already committed records (`INSERT OR IGNORE` provides secondary idempotency).

---

## 5. Failure Modes & Recovery Guarantees

| Failure Scenario | Exact Behavior | Recovery Guarantee |
| :--- | :--- | :--- |
| **Crash Before Commit** (e.g. out of memory during batch insert) | Uncommitted buffer is rolled back from SQLite volatile RAM. Zero dirty rows exist in `destination.db`. | Checkpoint in `metadata.db` remains at batch $N-1$. Worker resumes cleanly at batch $N$. |
| **Crash Between Commit & Checkpoint** | Data batch is written and committed in `destination.db`, but the worker dies before updating `metadata.db`. | Checkpoint in `metadata.db` stays at batch $N-1$. On resume, the worker replays batch $N$. `INSERT OR IGNORE` detects existing primary keys and flags `ignored_duplicates = N`. Checkpoint $N$ is committed without data duplication. |
| **Crash During Checkpoint Write** | Checkpoint row remains in `pending` status or transaction aborts. | Resumption queries only `status = 'completed'` checkpoints, safely re-running from the last fully acknowledged bookmark. |

---

## 6. The 4-Level Reconciliation Detection Funnel

Instead of running an expensive row-by-row full table scan, LedgerLab uses a tiered funnel:
1. **Count Check ($O(1)$)**: Fast `SELECT COUNT(*)` comparison.
2. **Double-Entry Balance Check ($O(1)$)**: Confirms $\sum \text{Debit} = \sum \text{Credit}$.
3. **Chunk Hash Radar ($O(N)$ with Early Skip)**: Evaluates SHA-256 digests across 1,000-record chunks. Matching chunks are skipped without examining individual rows, reducing I/O by >95%.
4. **Bisection Drilldown ($O(\log N)$)**: When a chunk hash mismatches, binary search splits the range into halves, recurses into the bad half, and pinpoints the corrupted row in $\lceil \log_2 N \rceil$ comparisons (10 steps for 1,000 rows).

