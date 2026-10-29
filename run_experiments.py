"""
LedgerLab Experiment Suite
Executes Experiments A, B, C, and D as outlined in the ISM specification:
  - Experiment A: Dataset Scalability (1k, 5k, 10k transactions)
  - Experiment B: Failure Recovery (interruption at batch 4, resume from checkpoint)
  - Experiment C: Data Integrity (inject delete & alter, detect via reconciliation, repair)
  - Experiment D: Batch Size Comparison (batch sizes 100, 500, 1000, 2500, 5000)
"""
import os
import sys
import json
import time

# Ensure import works
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app import app, init_dbs

def run_suite():
    print("=" * 60)
    print("  LedgerLab: Financial Ledger Migration Experiment Suite")
    print("=" * 60)

    # Initialize databases
    with app.app_context():
        init_dbs()

    client = app.test_client()
    os.makedirs(os.path.join(os.path.dirname(__file__), 'experiments'), exist_ok=True)
    results = {}

    # ─────────────────────────────────────────────────────────────
    # Experiment A: Dataset Scalability
    # ─────────────────────────────────────────────────────────────
    print("\n[+] Running Experiment A: Dataset Scalability...")
    exp_a_data = []
    sizes = [1000, 5000, 10000]

    for count in sizes:
        # Full clean reset
        client.post('/api/reset', json={'target': 'all'})

        print(f"  -> Generating {count} transactions ({count*2} ledger entries)...")
        t_gen_0 = time.perf_counter()
        gen_res = client.post('/api/generate', json={'count': count, 'seed': 42}).get_json()
        gen_duration = (time.perf_counter() - t_gen_0) * 1000

        print(f"  -> Migrating {gen_res['count']} entries with batch_size=1000...")
        mig_res = client.post('/api/migrate', json={'batch_size': 1000}).get_json()

        print(f"  -> Reconciling dataset...")
        t_rec_0 = time.perf_counter()
        rec_res = client.post('/api/reconcile', json={'job_id': mig_res['job_id']}).get_json()
        rec_duration = (time.perf_counter() - t_rec_0) * 1000

        discrepancies = len(rec_res['missing']) + len(rec_res['duplicates']) + len(rec_res['mismatches'])
        exp_a_data.append({
            'transaction_count': count,
            'entries_count': gen_res['count'],
            'generation_time_ms': round(gen_duration, 2),
            'migration_duration_ms': mig_res['duration_ms'],
            'migration_throughput_rec_sec': mig_res['throughput'],
            'reconciliation_duration_ms': round(rec_duration, 2),
            'reconciliation_status': rec_res['status'],
            'discrepancies': discrepancies
        })
        print(f"     Throughput: {mig_res['throughput']} rec/s | Status: {rec_res['status']} | Discrepancies: {discrepancies}")

    results['experiment_a_scalability'] = exp_a_data

    # ─────────────────────────────────────────────────────────────
    # Experiment B: Failure Recovery
    # ─────────────────────────────────────────────────────────────
    print("\n[+] Running Experiment B: Failure Recovery & Checkpointing...")
    client.post('/api/reset', json={'target': 'all'})
    gen_b = client.post('/api/generate', json={'count': 5000, 'seed': 101}).get_json()
    total_entries_b = gen_b['count']  # 10,000 entries

    print(f"  -> Starting migration of {total_entries_b} entries with injected failure at batch 4 (batch size 1000)...")
    fail_res = client.post('/api/migrate', json={
        'batch_size': 1000,
        'inject_failure': True,
        'failure_at_batch': 4
    }).get_json()

    print(f"  -> Interrupted as expected. Completed {fail_res['batches_completed']} batches.")
    fail_job_id = fail_res['job_id']

    # Inspect checkpoints
    status_res = client.get(f'/api/migrate/status/{fail_job_id}').get_json()
    checkpoints_count = len(status_res['checkpoints'])
    print(f"  -> Checkpoints recorded: {checkpoints_count}")

    print("  -> Resuming migration from checkpoint...")
    resume_res = client.post('/api/migrate/resume', json={'job_id': fail_job_id}).get_json()
    print(f"  -> Resumption finished. Status: {resume_res['status']} | Completed batches: {resume_res['batches_completed']}/{resume_res['total_batches']}")

    print("  -> Verifying integrity via full reconciliation...")
    rec_b = client.post('/api/reconcile', json={'job_id': fail_job_id}).get_json()
    print(f"  -> Reconciliation outcome: {rec_b['status'].upper()} (Missing: {len(rec_b['missing'])}, Duplicates: {len(rec_b['duplicates'])})")

    results['experiment_b_failure_recovery'] = {
        'initial_failure_batch': 4,
        'batches_before_failure': fail_res['batches_completed'],
        'checkpoints_saved': checkpoints_count,
        'resumption_status': resume_res['status'],
        'resumed_total_batches': resume_res['total_batches'],
        'final_reconciliation_status': rec_b['status'],
        'missing_count': len(rec_b['missing']),
        'duplicate_count': len(rec_b['duplicates'])
    }

    # ─────────────────────────────────────────────────────────────
    # Experiment C: Data Integrity & Tamper Detection
    # ─────────────────────────────────────────────────────────────
    print("\n[+] Running Experiment C: Tamper Detection & Discrepancy Repair...")
    print("  -> Injecting deletion of 3 random records in destination DB...")
    del_res = client.post('/api/integrity/inject', json={'type': 'delete', 'count': 3}).get_json()
    print(f"     Deleted IDs: {del_res['affected']}")

    print("  -> Injecting alteration (amount tamper) into 2 records...")
    alt_res = client.post('/api/integrity/inject', json={'type': 'alter', 'count': 2}).get_json()
    print(f"     Altered IDs: {alt_res['affected']}")

    print("  -> Running reconciliation...")
    rec_c = client.post('/api/reconcile', json={'job_id': fail_job_id}).get_json()
    print(f"     Outcome: {rec_c['status'].upper()} | Missing detected: {len(rec_c['missing'])} | Hash mismatches: {rec_c['hash_mismatches']}")

    print("  -> Executing discrepancy repair...")
    rep_res = client.post('/api/reconcile/repair', json={'result_id': rec_c['result_id']}).get_json()
    print(f"     Repaired missing: {rep_res['repaired_missing']}, Repaired mismatches: {rep_res['repaired_mismatches']}")

    print("  -> Re-running post-repair reconciliation...")
    rec_c_post = client.post('/api/reconcile', json={'job_id': fail_job_id}).get_json()
    post_discrepancies = len(rec_c_post['missing']) + len(rec_c_post['mismatches']) + len(rec_c_post['duplicates'])
    print(f"     Post-repair Status: {rec_c_post['status'].upper()} (Discrepancies: {post_discrepancies})")

    results['experiment_c_integrity'] = {
        'injected_deletions': 3,
        'injected_alterations': 2,
        'detected_missing': len(rec_c['missing']),
        'detected_hash_mismatches': rec_c['hash_mismatches'],
        'pre_repair_status': rec_c['status'],
        'repaired_total': rep_res['total_repaired'],
        'post_repair_status': rec_c_post['status'],
        'post_repair_discrepancies': post_discrepancies
    }

    # ─────────────────────────────────────────────────────────────
    # Experiment D: Batch Size Comparison
    # ─────────────────────────────────────────────────────────────
    print("\n[+] Running Experiment D: Batch Size Comparison (Fixed 10,000 Entries)...")
    batch_sizes = [100, 500, 1000, 2500, 5000]
    exp_d_data = []

    for bs in batch_sizes:
        client.post('/api/reset', json={'target': 'destination'})
        mig_d = client.post('/api/migrate', json={'batch_size': bs}).get_json()
        print(f"  -> Batch size {bs:5d}: Duration = {mig_d['duration_ms']:.2f}ms | Throughput = {mig_d['throughput']:.0f} rec/s")
        exp_d_data.append({
            'batch_size': bs,
            'duration_ms': mig_d['duration_ms'],
            'throughput_rec_sec': mig_d['throughput'],
            'status': mig_d['status']
        })

    results['experiment_d_batch_size'] = exp_d_data

    # Save summary JSON
    out_file = os.path.join(os.path.dirname(__file__), 'experiments', 'results_summary.json')
    with open(out_file, 'w') as f:
        json.dump(results, f, indent=2)

    print("\n" + "=" * 60)
    print(f"  Experiment Suite Finished. Results written to:")
    print(f"  {out_file}")
    print("=" * 60)

if __name__ == '__main__':
    run_suite()
