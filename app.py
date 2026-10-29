import hashlib
import json
import os
import random
import sqlite3
import time
import uuid
from flask import Flask, request, jsonify, render_template, g, Response, stream_with_context

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATHS = {
    'source': os.path.join(BASE_DIR, 'source.db'),
    'destination': os.path.join(BASE_DIR, 'destination.db'),
    'metadata': os.path.join(BASE_DIR, 'metadata.db'),
}

# ── DB helpers ──────────────────────────────────────────────────────────────

def get_db(name):
    key = f'_db_{name}'
    db = getattr(g, key, None)
    if db is None:
        db = sqlite3.connect(DB_PATHS[name])
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
        setattr(g, key, db)
    return db

@app.teardown_appcontext
def close_dbs(exc):
    for name in DB_PATHS:
        db = getattr(g, f'_db_{name}', None)
        if db is not None:
            db.close()

def init_dbs():
    txn_ddl = '''CREATE TABLE IF NOT EXISTS transactions (
        entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_id TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        transaction_type TEXT NOT NULL DEFAULT '',
        entry_type TEXT NOT NULL,
        account_reference TEXT NOT NULL,
        debit_amount INTEGER NOT NULL DEFAULT 0,
        credit_amount INTEGER NOT NULL DEFAULT 0,
        reference_entry_id INTEGER,
        record_hash TEXT NOT NULL
    )'''
    for name in ('source', 'destination'):
        con = sqlite3.connect(DB_PATHS[name])
        con.execute(txn_ddl)
        cols = [c[1] for c in con.execute('PRAGMA table_info(transactions)').fetchall()]
        if 'transaction_type' not in cols:
            con.execute("ALTER TABLE transactions ADD COLUMN transaction_type TEXT NOT NULL DEFAULT ''")
        con.commit()
        con.close()

    con = sqlite3.connect(DB_PATHS['metadata'])
    con.executescript('''
        CREATE TABLE IF NOT EXISTS migration_jobs (
            job_id TEXT PRIMARY KEY,
            source_database TEXT,
            target_database TEXT,
            dataset_size INTEGER,
            batch_size INTEGER,
            status TEXT DEFAULT 'pending',
            started_at TEXT,
            completed_at TEXT,
            duration_ms REAL,
            throughput REAL,
            inject_failure INTEGER DEFAULT 0,
            failure_at_batch INTEGER
        );
        CREATE TABLE IF NOT EXISTS checkpoints (
            checkpoint_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            batch_number INTEGER NOT NULL,
            start_record_id INTEGER,
            end_record_id INTEGER,
            status TEXT DEFAULT 'pending',
            updated_at TEXT,
            FOREIGN KEY (job_id) REFERENCES migration_jobs(job_id)
        );
        CREATE TABLE IF NOT EXISTS reconciliation_results (
            result_id TEXT PRIMARY KEY,
            job_id TEXT,
            check_type TEXT,
            records_checked INTEGER DEFAULT 0,
            discrepancies_found INTEGER DEFAULT 0,
            discrepancies_repaired INTEGER DEFAULT 0,
            missing_records TEXT,
            duplicate_records TEXT,
            mismatched_records TEXT,
            hash_mismatches INTEGER DEFAULT 0,
            status TEXT,
            checked_at TEXT
        );
        CREATE TABLE IF NOT EXISTS activity_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            event_type TEXT,
            message TEXT,
            details TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS experiments (
            experiment_id TEXT PRIMARY KEY,
            experiment_type TEXT,
            config TEXT,
            results TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS engine_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            seq INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            ts TEXT NOT NULL
        );
    ''')
    con.commit()
    con.close()

# ── Utility ─────────────────────────────────────────────────────────────────

def compute_hash(row):
    """SHA-256 of concatenated field values."""
    parts = '|'.join(str(v) for v in (
        row['transaction_id'], row['timestamp'], row.get('transaction_type', ''),
        row['entry_type'], row['account_reference'],
        row['debit_amount'], row['credit_amount'],
        row['reference_entry_id'],
    ))
    return hashlib.sha256(parts.encode()).hexdigest()

def now_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()

def log_activity(job_id, event_type, message, details=None):
    db = get_db('metadata')
    db.execute(
        'INSERT INTO activity_logs (job_id, event_type, message, details, created_at) VALUES (?,?,?,?,?)',
        (job_id, event_type, message, json.dumps(details) if details else None, now_iso()),
    )
    db.commit()

def emit_event(job_id, event_type, payload, seq=None):
    db = get_db('metadata')
    ts = now_iso()
    if seq is None:
        last = db.execute('SELECT MAX(seq) as max_seq FROM engine_events WHERE job_id=?', (job_id,)).fetchone()
        seq = (last['max_seq'] or 0) + 1 if last else 1
    db.execute(
        'INSERT INTO engine_events (job_id, seq, event_type, payload, ts) VALUES (?,?,?,?,?)',
        (job_id, seq, event_type, json.dumps(payload), ts),
    )
    db.commit()
    return seq

# ── Routes ──────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/favicon.ico')
def favicon():
    return ('', 204)

# ── Generate ────────────────────────────────────────────────────────────────

@app.route('/api/generate', methods=['POST'])
def generate():
    body = request.get_json(force=True)
    count = int(body.get('count', 10000))
    seed = body.get('seed', 42)
    rng = random.Random(seed)

    txn_types = ['rider_payment', 'driver_payout', 'service_fee', 'refund', 'adjustment']
    rows = []
    for _ in range(count):
        txn_id = f"TXN-{uuid.UUID(int=rng.getrandbits(128)).hex[:12].upper()}"
        ts = f"2024-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}T{rng.randint(0,23):02d}:{rng.randint(0,59):02d}:{rng.randint(0,59):02d}Z"
        txn_type = rng.choice(txn_types)
        amount = rng.randint(100, 100000)  # paise

        debit_acct = f"{txn_type}_debit_{rng.randint(1000,9999)}"
        credit_acct = f"{txn_type}_credit_{rng.randint(1000,9999)}"

        def make_hash(*parts):
            return hashlib.sha256('|'.join(str(v) for v in parts).encode()).hexdigest()

        h_debit = make_hash(txn_id, ts, txn_type, 'debit', debit_acct, amount, 0, None)
        rows.append((txn_id, ts, txn_type, 'debit', debit_acct, amount, 0, None, h_debit))

        h_credit = make_hash(txn_id, ts, txn_type, 'credit', credit_acct, 0, amount, None)
        rows.append((txn_id, ts, txn_type, 'credit', credit_acct, 0, amount, None, h_credit))

    db = get_db('source')
    db.executemany(
        '''INSERT INTO transactions
           (transaction_id, timestamp, transaction_type, entry_type,
            account_reference, debit_amount, credit_amount, reference_entry_id, record_hash)
           VALUES (?,?,?,?,?,?,?,?,?)''',
        rows,
    )
    db.commit()
    return jsonify(
        status='ok',
        transactions_generated=count,
        entries_generated=len(rows),
        count=len(rows),  # maintains backward compatibility with existing callers
    )

# ── Transactions viewer ────────────────────────────────────────────────────

@app.route('/api/transactions')
def list_transactions():
    db_name = request.args.get('db', 'source')
    if db_name not in ('source', 'destination'):
        return jsonify(error='invalid db'), 400
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    search = request.args.get('search', '').strip()
    type_filter = request.args.get('type_filter', '').strip()
    txn_type_filter = request.args.get('txn_type', '').strip()

    db = get_db(db_name)
    where, params = [], []
    if search:
        where.append('transaction_id LIKE ?')
        params.append(f'%{search}%')
    if type_filter:
        where.append('entry_type = ?')
        params.append(type_filter)
    if txn_type_filter:
        where.append('transaction_type = ?')
        params.append(txn_type_filter)
    clause = ('WHERE ' + ' AND '.join(where)) if where else ''

    total = db.execute(f'SELECT COUNT(*) FROM transactions {clause}', params).fetchone()[0]
    pages = max(1, -(-total // per_page))
    offset = (page - 1) * per_page
    rows = db.execute(
        f'SELECT * FROM transactions {clause} ORDER BY entry_id LIMIT ? OFFSET ?',
        params + [per_page, offset],
    ).fetchall()
    return jsonify(
        transactions=[dict(r) for r in rows],
        total=total, page=page, pages=pages,
    )

# ── Migration ──────────────────────────────────────────────────────────────

@app.route('/api/migrate', methods=['POST'])
def migrate():
    body = request.get_json(force=True) if request.data else {}
    batch_size = int(body.get('batch_size', 1000))
    inject_failure = bool(body.get('inject_failure', False))
    failure_at_batch = int(body.get('failure_at_batch', 3))
    crash_mode = body.get('crash_mode', 'before_commit')
    demo_delay = float(body.get('demo_delay', 0))

    src = get_db('source')
    dst = get_db('destination')
    meta = get_db('metadata')

    total_records = src.execute('SELECT COUNT(*) FROM transactions').fetchone()[0]
    if total_records == 0:
        return jsonify(error='No records in source'), 400

    if body.get('reset_dest', False):
        dst.execute('DELETE FROM transactions')
        dst.commit()

    total_batches = -(-total_records // batch_size)
    job_id = str(uuid.uuid4())

    meta.execute(
        '''INSERT INTO migration_jobs
           (job_id, source_database, target_database, dataset_size, batch_size,
            status, started_at, inject_failure, failure_at_batch)
           VALUES (?,?,?,?,?,?,?,?,?)''',
        (job_id, 'source.db', 'destination.db', total_records, batch_size,
         'running', now_iso(), int(inject_failure), failure_at_batch),
    )
    meta.commit()
    log_activity(job_id, 'migration_start', f'Starting migration of {total_records} records')

    emit_event(job_id, 'job_started', {
        'job_id': job_id,
        'dataset_size': total_records,
        'batch_size': batch_size,
        'total_batches': total_batches,
        'inject_failure': inject_failure,
        'failure_at_batch': failure_at_batch,
        'crash_mode': crash_mode,
    })

    t0 = time.perf_counter()
    batches_done = 0
    last_id = 0
    try:
        for batch_num in range(1, total_batches + 1):
            # Keyset pagination: WHERE entry_id > :last_id ORDER BY entry_id ASC LIMIT :batch_size
            rows = src.execute(
                'SELECT * FROM transactions WHERE entry_id > ? ORDER BY entry_id ASC LIMIT ?',
                (last_id, batch_size),
            ).fetchall()
            if not rows:
                break

            start_id = rows[0]['entry_id']
            end_id = rows[-1]['entry_id']

            # Beat 1: batch_read
            emit_event(job_id, 'batch_read', {
                'batch_no': batch_num,
                'first_id': start_id,
                'last_id': end_id,
                'row_count': len(rows),
                'sql': f'SELECT * FROM transactions WHERE entry_id > {last_id} ORDER BY entry_id ASC LIMIT {batch_size}',
            })

            # Beat 2: txn_begin
            emit_event(job_id, 'txn_begin', {'batch_no': batch_num})

            # Beat 3: rows_inserted (idempotent INSERT OR IGNORE)
            before_count = dst.execute('SELECT COUNT(*) FROM transactions').fetchone()[0]
            batch_tuples = [
                (r['entry_id'], r['transaction_id'], r['timestamp'], r['transaction_type'],
                 r['entry_type'], r['account_reference'], r['debit_amount'], r['credit_amount'],
                 r['reference_entry_id'], r['record_hash'])
                for r in rows
            ]
            dst.executemany(
                '''INSERT OR IGNORE INTO transactions
                   (entry_id, transaction_id, timestamp, transaction_type, entry_type,
                    account_reference, debit_amount, credit_amount,
                    reference_entry_id, record_hash)
                   VALUES (?,?,?,?,?,?,?,?,?,?)''',
                batch_tuples,
            )
            after_count = dst.execute('SELECT COUNT(*) FROM transactions').fetchone()[0]
            inserted = after_count - before_count
            ignored_duplicates = len(batch_tuples) - inserted

            emit_event(job_id, 'rows_inserted', {
                'batch_no': batch_num,
                'inserted': inserted,
                'ignored_duplicates': ignored_duplicates,
            })

            # Crash Mode 1: before_commit (uncommitted inserts dissolved by rollback)
            if inject_failure and batch_num == failure_at_batch and crash_mode == 'before_commit':
                dst.rollback()
                emit_event(job_id, 'worker_crashed', {
                    'batch_no': batch_num,
                    'phase': 'before_commit',
                    'error': f'Worker killed in batch {batch_num} before COMMIT',
                })
                emit_event(job_id, 'rollback_observed', {
                    'batch_no': batch_num,
                    'uncommitted_rows': len(rows),
                })
                raise RuntimeError(f'Injected failure at batch {batch_num} (phase: before_commit)')

            # Beat 4: batch_committed
            dst.commit()
            emit_event(job_id, 'batch_committed', {'batch_no': batch_num})

            # Crash Mode 2: after_commit_before_checkpoint
            if inject_failure and batch_num == failure_at_batch and crash_mode == 'after_commit_before_checkpoint':
                emit_event(job_id, 'worker_crashed', {
                    'batch_no': batch_num,
                    'phase': 'after_commit_before_checkpoint',
                    'error': f'Worker killed in batch {batch_num} after COMMIT before CHECKPOINT',
                })
                raise RuntimeError(f'Injected failure at batch {batch_num} (phase: after_commit_before_checkpoint)')

            # Beat 5: checkpoint_written
            meta.execute(
                '''INSERT INTO checkpoints (job_id, batch_number, start_record_id, end_record_id, status, updated_at)
                   VALUES (?,?,?,?,?,?)''',
                (job_id, batch_num, start_id, end_id, 'completed', now_iso()),
            )
            meta.commit()
            batches_done = batch_num
            last_id = end_id

            emit_event(job_id, 'checkpoint_written', {
                'batch_no': batch_num,
                'last_committed_id': end_id,
            })
            log_activity(job_id, 'batch_complete', f'Batch {batch_num}/{total_batches} done')

            if demo_delay > 0:
                time.sleep(demo_delay)

        duration_ms = (time.perf_counter() - t0) * 1000
        throughput = total_records / (duration_ms / 1000) if duration_ms > 0 else 0
        meta.execute(
            '''UPDATE migration_jobs SET status='completed', completed_at=?, duration_ms=?, throughput=?
               WHERE job_id=?''',
            (now_iso(), round(duration_ms, 2), round(throughput, 2), job_id),
        )
        meta.commit()
        log_activity(job_id, 'migration_complete', 'Migration completed successfully')
        status = 'completed'

        emit_event(job_id, 'migration_finished', {
            'job_id': job_id,
            'total_batches': total_batches,
            'batches_completed': batches_done,
            'duration_ms': round(duration_ms, 2),
            'throughput': round(throughput, 2),
            'status': status,
        })

        meta.execute(
            '''INSERT OR REPLACE INTO experiments (experiment_id, experiment_type, config, results, created_at)
               VALUES (?, ?, ?, ?, ?)''',
            (job_id, 'migration',
             json.dumps({'batch_size': batch_size, 'dataset_size': total_records, 'inject_failure': inject_failure, 'failure_at_batch': failure_at_batch, 'crash_mode': crash_mode}),
             json.dumps({'duration_ms': round(duration_ms, 2), 'throughput': round(throughput, 2), 'status': status, 'batches_completed': batches_done, 'total_batches': total_batches}),
             now_iso())
        )
        meta.commit()

    except Exception as exc:
        duration_ms = (time.perf_counter() - t0) * 1000
        throughput = (batches_done * batch_size) / (duration_ms / 1000) if duration_ms > 0 else 0
        meta.execute(
            '''UPDATE migration_jobs SET status='failed', completed_at=?, duration_ms=?, throughput=?
               WHERE job_id=?''',
            (now_iso(), round(duration_ms, 2), round(throughput, 2), job_id),
        )
        meta.execute(
            '''INSERT OR REPLACE INTO experiments (experiment_id, experiment_type, config, results, created_at)
               VALUES (?, ?, ?, ?, ?)''',
            (job_id, 'migration',
             json.dumps({'batch_size': batch_size, 'dataset_size': total_records, 'inject_failure': inject_failure, 'failure_at_batch': failure_at_batch, 'crash_mode': crash_mode}),
             json.dumps({'duration_ms': round(duration_ms, 2), 'throughput': round(throughput, 2), 'status': 'failed', 'batches_completed': batches_done, 'error': str(exc)}),
             now_iso())
        )
        meta.commit()
        log_activity(job_id, 'migration_failed', str(exc))

        emit_event(job_id, 'job_interrupted', {
            'job_id': job_id,
            'last_committed_batch': batches_done,
            'error': str(exc),
        })
        status = 'failed'

    return jsonify(
        job_id=job_id, status=status, batches_completed=batches_done,
        total_batches=total_batches, duration_ms=round(duration_ms, 2),
        throughput=round(throughput, 2),
    )

# ── Resume ─────────────────────────────────────────────────────────────────

@app.route('/api/migrate/resume', methods=['POST'])
def migrate_resume():
    body = request.get_json(force=True) if request.data else {}
    job_id = body['job_id']
    demo_delay = float(body.get('demo_delay', 0))
    meta = get_db('metadata')

    job = meta.execute('SELECT * FROM migration_jobs WHERE job_id=?', (job_id,)).fetchone()
    if not job:
        return jsonify(error='Job not found'), 404

    batch_size = job['batch_size']
    src = get_db('source')
    dst = get_db('destination')

    total_records = job['dataset_size']
    total_batches = -(-total_records // batch_size)

    last_cp = meta.execute(
        '''SELECT * FROM checkpoints WHERE job_id=? AND status='completed'
           ORDER BY batch_number DESC LIMIT 1''',
        (job_id,),
    ).fetchone()
    resume_batch = (last_cp['batch_number'] + 1) if last_cp else 1
    last_id = last_cp['end_record_id'] if last_cp else 0

    meta.execute("UPDATE migration_jobs SET status='running', inject_failure=0, started_at=? WHERE job_id=?",
                 (now_iso(), job_id))
    meta.commit()
    log_activity(job_id, 'migration_resume', f'Resuming from batch {resume_batch} at entry_id > {last_id}')

    emit_event(job_id, 'resume_from', {
        'batch_no': resume_batch,
        'last_committed_id': last_id,
    })

    t0 = time.perf_counter()
    batches_done = resume_batch - 1
    status = 'completed'
    try:
        for batch_num in range(resume_batch, total_batches + 1):
            # Keyset pagination: resume from last_committed_id
            rows = src.execute(
                'SELECT * FROM transactions WHERE entry_id > ? ORDER BY entry_id ASC LIMIT ?',
                (last_id, batch_size),
            ).fetchall()
            if not rows:
                break

            start_id = rows[0]['entry_id']
            end_id = rows[-1]['entry_id']

            # Beat 1: batch_read
            emit_event(job_id, 'batch_read', {
                'batch_no': batch_num,
                'first_id': start_id,
                'last_id': end_id,
                'row_count': len(rows),
                'sql': f'SELECT * FROM transactions WHERE entry_id > {last_id} ORDER BY entry_id ASC LIMIT {batch_size}',
            })

            # Beat 2: txn_begin
            emit_event(job_id, 'txn_begin', {'batch_no': batch_num})

            # Beat 3: rows_inserted (idempotent write)
            before_count = dst.execute('SELECT COUNT(*) FROM transactions').fetchone()[0]
            batch_tuples = [
                (r['entry_id'], r['transaction_id'], r['timestamp'], r['transaction_type'],
                 r['entry_type'], r['account_reference'], r['debit_amount'], r['credit_amount'],
                 r['reference_entry_id'], r['record_hash'])
                for r in rows
            ]
            dst.executemany(
                '''INSERT OR IGNORE INTO transactions
                   (entry_id, transaction_id, timestamp, transaction_type, entry_type,
                    account_reference, debit_amount, credit_amount,
                    reference_entry_id, record_hash)
                   VALUES (?,?,?,?,?,?,?,?,?,?)''',
                batch_tuples,
            )
            after_count = dst.execute('SELECT COUNT(*) FROM transactions').fetchone()[0]
            inserted = after_count - before_count
            ignored_duplicates = len(batch_tuples) - inserted

            emit_event(job_id, 'rows_inserted', {
                'batch_no': batch_num,
                'inserted': inserted,
                'ignored_duplicates': ignored_duplicates,
            })

            # Beat 4: batch_committed
            dst.commit()
            emit_event(job_id, 'batch_committed', {'batch_no': batch_num})

            # Beat 5: checkpoint_written
            meta.execute(
                '''INSERT INTO checkpoints (job_id, batch_number, start_record_id, end_record_id, status, updated_at)
                   VALUES (?,?,?,?,?,?)''',
                (job_id, batch_num, start_id, end_id, 'completed', now_iso()),
            )
            meta.commit()
            batches_done = batch_num
            last_id = end_id

            emit_event(job_id, 'checkpoint_written', {
                'batch_no': batch_num,
                'last_committed_id': end_id,
            })
            log_activity(job_id, 'batch_complete', f'Batch {batch_num}/{total_batches} done')

            if demo_delay > 0:
                time.sleep(demo_delay)

        duration_ms = (time.perf_counter() - t0) * 1000
        throughput = total_records / (duration_ms / 1000) if duration_ms > 0 else 0
        meta.execute(
            '''UPDATE migration_jobs SET status='completed', completed_at=?, duration_ms=?, throughput=?
               WHERE job_id=?''',
            (now_iso(), round(duration_ms, 2), round(throughput, 2), job_id),
        )
        meta.commit()
        log_activity(job_id, 'migration_complete', 'Resumed migration completed')

        emit_event(job_id, 'migration_finished', {
            'job_id': job_id,
            'total_batches': total_batches,
            'batches_completed': batches_done,
            'duration_ms': round(duration_ms, 2),
            'throughput': round(throughput, 2),
            'status': 'completed',
        })

        meta.execute(
            '''INSERT OR REPLACE INTO experiments (experiment_id, experiment_type, config, results, created_at)
               VALUES (?, ?, ?, ?, ?)''',
            (job_id, 'migration_resumed',
             json.dumps({'batch_size': batch_size, 'dataset_size': total_records}),
             json.dumps({'duration_ms': round(duration_ms, 2), 'throughput': round(throughput, 2), 'status': 'completed', 'batches_completed': batches_done, 'total_batches': total_batches}),
             now_iso())
        )
        meta.commit()

    except Exception as exc:
        duration_ms = (time.perf_counter() - t0) * 1000
        throughput = (batches_done * batch_size) / (duration_ms / 1000) if duration_ms > 0 else 0
        meta.execute(
            '''UPDATE migration_jobs SET status='failed', completed_at=?, duration_ms=?, throughput=?
               WHERE job_id=?''',
            (now_iso(), round(duration_ms, 2), round(throughput, 2), job_id),
        )
        meta.commit()
        log_activity(job_id, 'migration_failed', str(exc))

        emit_event(job_id, 'job_interrupted', {
            'job_id': job_id,
            'last_committed_batch': batches_done,
            'error': str(exc),
        })
        status = 'failed'

    return jsonify(
        job_id=job_id, status=status, batches_completed=batches_done,
        total_batches=total_batches, duration_ms=round(duration_ms, 2),
        throughput=round(throughput, 2),
    )

# ── Event Streaming & Polling ──────────────────────────────────────────────

@app.route('/api/stream/<job_id>')
def stream_events(job_id):
    def event_generator():
        last_seq = 0
        idle_ticks = 0
        while True:
            # Open fresh connection to avoid WAL caching in long-running generators
            meta_con = sqlite3.connect(DB_PATHS['metadata'])
            meta_con.row_factory = sqlite3.Row
            rows = meta_con.execute(
                'SELECT * FROM engine_events WHERE job_id=? AND seq > ? ORDER BY seq ASC',
                (job_id, last_seq),
            ).fetchall()
            if rows:
                idle_ticks = 0
                for r in rows:
                    last_seq = r['seq']
                    ev_data = {
                        'seq': r['seq'],
                        'type': r['event_type'],
                        'payload': json.loads(r['payload']),
                        'ts': r['ts'],
                    }
                    yield f"data: {json.dumps(ev_data)}\n\n"
                    if r['event_type'] in ('migration_finished', 'worker_crashed', 'recon_finished'):
                        meta_con.close()
                        return
            else:
                job = meta_con.execute('SELECT status FROM migration_jobs WHERE job_id=?', (job_id,)).fetchone()
                if job and job['status'] in ('completed', 'failed', 'interrupted'):
                    idle_ticks += 1
                    if idle_ticks > 5:
                        meta_con.close()
                        return
                time.sleep(0.1)
            meta_con.close()

    return Response(
        stream_with_context(event_generator()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive',
        },
    )

@app.route('/api/events/<job_id>')
def get_job_events(job_id):
    meta = get_db('metadata')
    rows = meta.execute('SELECT * FROM engine_events WHERE job_id=? ORDER BY seq ASC', (job_id,)).fetchall()
    events = [{'seq': r['seq'], 'type': r['event_type'], 'payload': json.loads(r['payload']), 'ts': r['ts']} for r in rows]
    return jsonify(job_id=job_id, events=events, count=len(events))

# ── Migration status ───────────────────────────────────────────────────────

@app.route('/api/migrate/status/<job_id>')
def migrate_status(job_id):
    meta = get_db('metadata')
    job = meta.execute('SELECT * FROM migration_jobs WHERE job_id=?', (job_id,)).fetchone()
    if not job:
        return jsonify(error='Job not found'), 404
    checkpoints = meta.execute(
        'SELECT * FROM checkpoints WHERE job_id=? ORDER BY batch_number', (job_id,)
    ).fetchall()
    return jsonify(job=dict(job), checkpoints=[dict(c) for c in checkpoints])

# ── Reconciliation ─────────────────────────────────────────────────────────

@app.route('/api/reconcile', methods=['POST'])
def reconcile():
    body = request.get_json(force=True)
    job_id = body.get('job_id')

    src = get_db('source')
    dst = get_db('destination')
    meta = get_db('metadata')

    src_rows = src.execute('SELECT * FROM transactions ORDER BY entry_id').fetchall()
    dst_rows = dst.execute('SELECT * FROM transactions ORDER BY entry_id').fetchall()

    src_map = {r['entry_id']: dict(r) for r in src_rows}
    dst_map = {r['entry_id']: dict(r) for r in dst_rows}

    missing = [eid for eid in src_map if eid not in dst_map]
    duplicates = []
    # check for duplicate entry_ids in destination (ignoring audit compensating entries)
    dst_txn_counts = {}
    for r in dst_rows:
        if r['reference_entry_id'] is not None:
            continue
        key = (r['transaction_id'], r['entry_type'])
        dst_txn_counts.setdefault(key, []).append(r['entry_id'])
    for key, ids in dst_txn_counts.items():
        if len(ids) > 1:
            duplicates.extend(ids[1:])

    mismatches = []
    hash_mismatches = 0
    diff_records = []
    for eid in src_map:
        if eid in dst_map:
            s, d = src_map[eid], dst_map[eid]
            s_hash = compute_hash(s)
            d_hash = compute_hash(d)
            if s_hash != d_hash:
                hash_mismatches += 1
                diff_fields = [
                    k for k in ('transaction_id', 'timestamp', 'transaction_type', 'entry_type',
                                'account_reference', 'debit_amount', 'credit_amount')
                    if s.get(k) != d.get(k)
                ]
                mismatches.append({'entry_id': eid, 'fields': diff_fields})
                if len(diff_records) < 10:
                    diff_records.append({
                        'entry_id': eid,
                        'transaction_id': s.get('transaction_id'),
                        'type': 'alter',
                        'fields': diff_fields,
                        'source': {
                            'amount': (s.get('debit_amount') or s.get('credit_amount') or 0),
                            'entry_type': s.get('entry_type'),
                            'account': s.get('account_reference'),
                            'hash': s_hash,
                        },
                        'destination': {
                            'amount': (d.get('debit_amount') or d.get('credit_amount') or 0),
                            'entry_type': d.get('entry_type'),
                            'account': d.get('account_reference'),
                            'hash': d_hash,
                        },
                        'explanation': f"Cryptographic Hash Mismatch: Source SHA-256 ({s_hash[:8]}...) != Destination SHA-256 ({d_hash[:8]}...)"
                    })

    for eid in missing[:10]:
        s = src_map[eid]
        diff_records.append({
            'entry_id': eid,
            'transaction_id': s.get('transaction_id'),
            'type': 'missing',
            'fields': ['RECORD DELETED'],
            'source': {
                'amount': (s.get('debit_amount') or s.get('credit_amount') or 0),
                'entry_type': s.get('entry_type'),
                'account': s.get('account_reference'),
                'hash': compute_hash(s),
            },
            'destination': None,
            'explanation': f"Set Difference: Primary Key #{eid} exists in Source index but is absent in Destination index."
        })

    records_checked = len(src_map)
    discrepancies = len(missing) + len(duplicates) + len(mismatches)
    status = 'pass' if discrepancies == 0 else 'fail'
    result_id = str(uuid.uuid4())

    meta.execute(
        '''INSERT INTO reconciliation_results
           (result_id, job_id, check_type, records_checked, discrepancies_found,
            missing_records, duplicate_records, mismatched_records, hash_mismatches, status, checked_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
        (result_id, job_id, 'full', records_checked, discrepancies,
         json.dumps(missing), json.dumps(duplicates), json.dumps(mismatches),
         hash_mismatches, status, now_iso()),
    )
    meta.commit()
    log_activity(job_id, 'reconciliation', f'Reconciliation {status}: {discrepancies} discrepancies')

    return jsonify(
        result_id=result_id, records_checked=records_checked,
        missing=missing, duplicates=duplicates, mismatches=mismatches,
        hash_mismatches=hash_mismatches, status=status,
        diff_records=diff_records,
    )

# ── Repair ─────────────────────────────────────────────────────────────────

@app.route('/api/reconcile/repair', methods=['POST'])
def repair():
    body = request.get_json(force=True)
    result_id = body['result_id']

    meta = get_db('metadata')
    result = meta.execute(
        'SELECT * FROM reconciliation_results WHERE result_id=?', (result_id,)
    ).fetchone()
    if not result:
        return jsonify(error='Result not found'), 404

    src = get_db('source')
    dst = get_db('destination')

    missing = json.loads(result['missing_records'] or '[]')
    duplicates = json.loads(result['duplicate_records'] or '[]')
    mismatches = json.loads(result['mismatched_records'] or '[]')

    repaired_missing = 0
    for eid in missing:
        row = src.execute('SELECT * FROM transactions WHERE entry_id=?', (eid,)).fetchone()
        if row:
            dst.execute(
                '''INSERT OR IGNORE INTO transactions
                   (entry_id, transaction_id, timestamp, transaction_type, entry_type,
                    account_reference, debit_amount, credit_amount,
                    reference_entry_id, record_hash)
                   VALUES (?,?,?,?,?,?,?,?,?,?)''',
                (row['entry_id'], row['transaction_id'], row['timestamp'], row['transaction_type'],
                 row['entry_type'], row['account_reference'], row['debit_amount'], row['credit_amount'],
                 row['reference_entry_id'], row['record_hash']),
            )
            repaired_missing += 1
    dst.commit()

    repaired_duplicates = 0
    for eid in duplicates:
        dst.execute('DELETE FROM transactions WHERE entry_id=?', (eid,))
        repaired_duplicates += 1
    dst.commit()

    repaired_mismatches = 0
    for m in mismatches:
        eid = m['entry_id']
        src_row = src.execute('SELECT * FROM transactions WHERE entry_id=?', (eid,)).fetchone()
        if src_row:
            # 1. Restore authoritative destination row so the data matches source
            dst.execute(
                '''UPDATE transactions
                   SET transaction_id=?, timestamp=?, transaction_type=?, entry_type=?,
                       account_reference=?, debit_amount=?, credit_amount=?, record_hash=?
                   WHERE entry_id=?''',
                (src_row['transaction_id'], src_row['timestamp'], src_row['transaction_type'],
                 src_row['entry_type'], src_row['account_reference'], src_row['debit_amount'],
                 src_row['credit_amount'], src_row['record_hash'], eid)
            )
            # 2. Append immutable compensating audit entry referencing the altered transaction
            txn_id = src_row['transaction_id']
            dst.execute(
                '''INSERT INTO transactions
                   (transaction_id, timestamp, transaction_type, entry_type, account_reference,
                    debit_amount, credit_amount, reference_entry_id, record_hash)
                   VALUES (?,?,?,?,?,?,?,?,?)''',
                (txn_id, now_iso(), src_row['transaction_type'], src_row['entry_type'],
                 src_row['account_reference'], src_row['debit_amount'], src_row['credit_amount'],
                 eid, compute_hash(dict(src_row))),
            )
            repaired_mismatches += 1
    dst.commit()

    total_repaired = repaired_missing + repaired_duplicates + repaired_mismatches
    meta.execute(
        'UPDATE reconciliation_results SET discrepancies_repaired=? WHERE result_id=?',
        (total_repaired, result_id),
    )
    meta.commit()

    return jsonify(
        result_id=result_id,
        repaired_missing=repaired_missing,
        repaired_duplicates=repaired_duplicates,
        repaired_mismatches=repaired_mismatches,
        total_repaired=total_repaired,
    )

# ── Integrity (inject discrepancies) ──────────────────────────────────────

@app.route('/api/integrity/inject', methods=['POST'])
def inject_discrepancy():
    body = request.get_json(force=True)
    disc_type = body.get('type', 'delete')
    count = int(body.get('count', 1))

    dst = get_db('destination')
    rows = dst.execute(
        'SELECT entry_id, transaction_id, entry_type, account_reference, debit_amount, credit_amount, record_hash FROM transactions ORDER BY RANDOM() LIMIT ?', (count,)
    ).fetchall()
    affected = [r['entry_id'] for r in rows]
    details = [dict(r) for r in rows]

    if disc_type == 'delete':
        for eid in affected:
            dst.execute('DELETE FROM transactions WHERE entry_id=?', (eid,))
    elif disc_type == 'alter':
        for eid in affected:
            dst.execute(
                'UPDATE transactions SET debit_amount = debit_amount + 999, record_hash = ? WHERE entry_id=?',
                ('ALTERED', eid),
            )
    dst.commit()

    return jsonify(type=disc_type, affected=affected, count=len(affected), details=details)

# ── Experiments ────────────────────────────────────────────────────────────

@app.route('/api/experiments')
def list_experiments():
    meta = get_db('metadata')
    rows = meta.execute('SELECT * FROM experiments ORDER BY created_at DESC').fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d['config'] = json.loads(d['config']) if d['config'] else None
        d['results'] = json.loads(d['results']) if d['results'] else None
        out.append(d)
    return jsonify(experiments=out)

@app.route('/api/experiments/<experiment_id>')
def get_experiment(experiment_id):
    meta = get_db('metadata')
    row = meta.execute('SELECT * FROM experiments WHERE experiment_id=?', (experiment_id,)).fetchone()
    if not row:
        return jsonify(error='Not found'), 404
    d = dict(row)
    d['config'] = json.loads(d['config']) if d['config'] else None
    d['results'] = json.loads(d['results']) if d['results'] else None
    return jsonify(d)

@app.route('/api/experiments/<experiment_id>/export')
def export_experiment(experiment_id):
    fmt = request.args.get('format', 'json')
    meta = get_db('metadata')
    row = meta.execute('SELECT * FROM experiments WHERE experiment_id=?', (experiment_id,)).fetchone()
    if row:
        d = dict(row)
        d['config'] = json.loads(d['config']) if d['config'] else {}
        d['results'] = json.loads(d['results']) if d['results'] else {}
    else:
        job = meta.execute('SELECT * FROM migration_jobs WHERE job_id=?', (experiment_id,)).fetchone()
        if not job:
            return jsonify(error='Not found'), 404
        j = dict(job)
        d = {
            'experiment_id': j['job_id'],
            'experiment_type': 'migration',
            'created_at': j['started_at'],
            'config': {
                'dataset_size': j['dataset_size'],
                'batch_size': j['batch_size'],
                'inject_failure': j['inject_failure'],
                'failure_at_batch': j['failure_at_batch'],
                'source_database': j['source_database'],
                'target_database': j['target_database']
            },
            'results': {
                'status': j['status'],
                'duration_ms': j['duration_ms'],
                'throughput': j['throughput'],
                'completed_at': j['completed_at']
            }
        }

    if fmt == 'csv':
        import csv, io
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(['experiment_id', 'experiment_type', 'created_at'] +
                    list(d['config'].keys()) + list(d['results'].keys()))
        w.writerow([d['experiment_id'], d['experiment_type'], d['created_at']] +
                   list(d['config'].values()) + list(d['results'].values()))
        return Response(buf.getvalue(), mimetype='text/csv',
                        headers={'Content-Disposition': f'attachment; filename={experiment_id}.csv'})

    return Response(json.dumps(d, indent=2), mimetype='application/json',
                    headers={'Content-Disposition': f'attachment; filename={experiment_id}.json'})

# ── Experiment Jobs (list all migration jobs for experiments page) ──────────

@app.route('/api/experiment-jobs')
def experiment_jobs():
    meta = get_db('metadata')
    rows = meta.execute('SELECT * FROM migration_jobs ORDER BY started_at DESC').fetchall()
    return jsonify(jobs=[dict(r) for r in rows])

# ── Reset / Start Fresh ──────────────────────────────────────────────────

@app.route('/api/reset', methods=['POST'])
def reset_system():
    body = request.get_json(silent=True) or {}
    target = body.get('target', 'all')  # 'all', 'destination', 'data'

    if target in ('all', 'destination'):
        dst = get_db('destination')
        dst.execute('DELETE FROM transactions')
        try: dst.execute("DELETE FROM sqlite_sequence WHERE name='transactions'")
        except Exception: pass
        dst.commit()

    if target in ('all', 'data'):
        src = get_db('source')
        src.execute('DELETE FROM transactions')
        try: src.execute("DELETE FROM sqlite_sequence WHERE name='transactions'")
        except Exception: pass
        src.commit()

    if target == 'all':
        meta = get_db('metadata')
        meta.execute('DELETE FROM migration_jobs')
        meta.execute('DELETE FROM checkpoints')
        meta.execute('DELETE FROM reconciliation_results')
        meta.execute('DELETE FROM activity_logs')
        meta.execute('DELETE FROM experiments')
        meta.commit()

    return jsonify(status='ok', target=target)

# ── Stats ──────────────────────────────────────────────────────────────────

@app.route('/api/stats')
def stats():
    src = get_db('source')
    dst = get_db('destination')
    meta = get_db('metadata')

    src_entries = src.execute('SELECT COUNT(*) FROM transactions').fetchone()[0]
    dst_entries = dst.execute('SELECT COUNT(*) FROM transactions').fetchone()[0]
    src_txns = src.execute('SELECT COUNT(DISTINCT transaction_id) FROM transactions').fetchone()[0]
    dst_txns = dst.execute('SELECT COUNT(DISTINCT transaction_id) FROM transactions').fetchone()[0]

    last_migration = meta.execute(
        'SELECT status FROM migration_jobs ORDER BY started_at DESC LIMIT 1'
    ).fetchone()
    last_reconciliation = meta.execute(
        'SELECT status FROM reconciliation_results ORDER BY checked_at DESC LIMIT 1'
    ).fetchone()
    total_experiments = meta.execute('SELECT COUNT(*) FROM experiments').fetchone()[0]

    return jsonify(
        source_records=src_entries,
        destination_records=dst_entries,
        source_entries=src_entries,
        destination_entries=dst_entries,
        source_transactions=src_txns,
        destination_transactions=dst_txns,
        last_migration_status=last_migration['status'] if last_migration else None,
        last_reconciliation_status=last_reconciliation['status'] if last_reconciliation else None,
        total_experiments=total_experiments,
    )

# ── Main ───────────────────────────────────────────────────────────────────

with app.app_context():
    init_dbs()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
