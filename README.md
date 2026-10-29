# LedgerLab: Financial Ledger Migration and Data Integrity Simulator

**Information Storage and Management (ISM) Project**  
*Case Study: Financial Data Migration Inspired by Large-Scale Ledger Systems (e.g., Uber LedgerStore/DocStore principles)*

---

## 1. Project Overview

LedgerLab is an interactive, technically defensible simulation platform demonstrating how large financial datasets are migrated between distinct storage systems while guaranteeing completeness, correctness, and consistency.

Unlike naive bulk-copy scripts, LedgerLab demonstrates:
- **Append-Only Ledger Architecture**: Double-entry bookkeeping where corrections are recorded as compensating transactions rather than destructive mutations.
- **Batched Atomic Processing**: Chunked reading and insertion with transactional integrity.
- **Checkpointing & State Tracking**: Persistent progress markers recording committed batches, enabling deterministic failure recovery without data duplication.
- **End-to-End Cryptographic Reconciliation**: Record-level comparison combining SHA-256 field hashing with balance verification.
- **Controlled Failure Injection & Tamper Simulation**: Intentional worker crash simulation and database corruption to benchmark detection and recovery mechanisms.

---

## 2. Architecture & Data Flow

```
┌─────────────────┐       ┌─────────────────┐       ┌──────────────────────┐
│  Source SQLite  │ ───>  │ Migration Engine│ ───>  │  Destination SQLite  │
│  (Authoritative)│       │ (Batch Reader)  │       │  (Migrated Ledger)   │
└─────────────────┘       └────────┬────────┘       └──────────┬───────────┘
                                   │                           │
                                   ▼                           ▼
                          ┌─────────────────┐       ┌──────────────────────┐
                          │Checkpoint Mgr   │       │Reconciliation Engine │
                          │(metadata.db)    │       │(Content & Hash Check)│
                          └─────────────────┘       └──────────────────────┘
```

The system strictly isolates storage targets into three distinct SQLite databases:
1. `source.db`: The authoritative primary transaction ledger.
2. `destination.db`: The migration target storage system.
3. `metadata.db`: Independent catalog maintaining migration jobs, batch checkpoints, reconciliation audits, activity logs, and experiment runs.

---

## 3. Technology Stack

- **Backend**: Python 3.10+, Flask
- **Storage**: SQLite 3 (WAL mode enabled for concurrent read/write isolation)
- **Frontend**: Single-Page Application (HTML5, Vanilla JavaScript ES6+, Responsive CSS)
- **Cryptographic Hashing**: Standard Library `hashlib.sha256`
- **Dependencies**: Minimal (`Flask` only — stdlib-first design)

---

## 4. Setup and Installation

### Prerequisites
- Python 3.8 or newer
- `pip`

### Installation Steps
```bash
# 1. Clone or navigate to the project directory
cd "c:/Users/Aryan lol/Desktop/ism-project"

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the application
python app.py
```

Open your browser at:
```
http://127.0.0.1:5000
```

---

## 5. Running the Experiments

To run the automated suite testing scalability, checkpoint resumption, tamper detection, and batch size comparisons:

```bash
python run_experiments.py
```

The experiment suite will execute:
- **Experiment A**: Dataset Scalability (1,000 to 10,000 transactions)
- **Experiment B**: Worker Interruption & Checkpoint Resumption
- **Experiment C**: Cryptographic Tamper Detection & Discrepancy Repair
- **Experiment D**: Batch Size Comparison (100 to 5,000 batch size)

Results and JSON telemetry are exported directly to `experiments/results_summary.json`.

---

## 6. Project Structure

```
ism-project/
├── app.py                      # Flask backend, APIs, Migration Engine, Reconciliation
├── run_experiments.py          # Benchmark and experiment test suite
├── requirements.txt            # Minimal dependencies (Flask)
├── document                    # Academic ISM Project specification
├── templates/
│   └── index.html              # Interactive Dark-themed Dashboard UI
├── docs/
│   ├── TECHNICAL_DOCUMENTATION.md  # Deep dive into ISM principles and DB schema
│   ├── CASE_STUDY_REPORT.md        # Comprehensive academic project report
│   └── VIVA_PRESENTATION.md        # 5-7 min demonstration & defense guide
└── .agents/                    # Antigravity agent configuration and custom skills
```

