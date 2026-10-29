"""
Safe Smoke Test for LedgerLab API
Uses an isolated temporary directory so it NEVER deletes or modifies working database files!
"""
import os
import sys
import tempfile
import app as ledger_app

def run_smoke_test():
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Override DB paths to completely isolated temporary files
        ledger_app.DB_PATHS = {
            'source': os.path.join(tmp_dir, 'test_source.db'),
            'destination': os.path.join(tmp_dir, 'test_destination.db'),
            'metadata': os.path.join(tmp_dir, 'test_metadata.db'),
        }

        with ledger_app.app.app_context():
            ledger_app.init_dbs()

        c = ledger_app.app.test_client()
        passed = 0

        def check(label, resp, expect=200):
            nonlocal passed
            d = resp.get_json() if resp.is_json else None
            ok = resp.status_code == expect
            print(f"{'OK' if ok else 'FAIL'} {label}")
            if not ok:
                print(f"  Error [{resp.status_code}]: {d}")
                sys.exit(1)
            passed += 1
            return d

        # 1. Initial empty stats
        d = check("stats_empty", c.get('/api/stats'))
        assert d['source_records'] == 0

        # 2. Generate 100 transactions (200 ledger entries)
        d = check("generate", c.post('/api/generate', json={'count': 100, 'seed': 42}))
        assert d['count'] == 200

        # 3. View transactions
        d = check("transactions", c.get('/api/transactions?db=source&page=1&per_page=5'))
        assert len(d['transactions']) == 5

        # 4. Clean migration
        d = check("migrate", c.post('/api/migrate', json={'batch_size': 50}))
        assert d['status'] == 'completed'
        job = d['job_id']

        # 5. Checkpoints
        d = check("status", c.get(f'/api/migrate/status/{job}'))
        assert len(d['checkpoints']) == 4

        # 6. Reconciliation (initial clean pass)
        d = check("reconcile", c.post('/api/reconcile', json={'job_id': job}))
        assert d['status'] == 'pass'

        # 7. Inject deletion
        d = check("inject_delete", c.post('/api/integrity/inject', json={'type': 'delete', 'count': 1}))
        assert d['count'] == 1

        # 8. Reconciliation detects deletion
        d = check("recon_detect_delete", c.post('/api/reconcile', json={'job_id': job}))
        assert d['status'] == 'fail'
        assert len(d['missing']) == 1
        rid = d['result_id']

        # 9. Repair missing record
        d = check("repair_delete", c.post('/api/reconcile/repair', json={'result_id': rid}))
        assert d['repaired_missing'] == 1

        # 10. Post-repair reconciliation passes
        d = check("recon_after_repair_delete", c.post('/api/reconcile', json={'job_id': job}))
        assert d['status'] == 'pass'

        # 11. Inject alteration (tamper amount)
        d = check("inject_alter", c.post('/api/integrity/inject', json={'type': 'alter', 'count': 1}))
        assert d['count'] == 1

        # 12. Reconciliation detects alteration via hash mismatch
        d = check("recon_detect_alter", c.post('/api/reconcile', json={'job_id': job}))
        assert d['status'] == 'fail'
        assert d['hash_mismatches'] == 1
        rid2 = d['result_id']

        # 13. Repair altered record (restores row + appends compensating entry)
        d = check("repair_alter", c.post('/api/reconcile/repair', json={'result_id': rid2}))
        assert d['repaired_mismatches'] == 1

        # 14. Post-repair reconciliation passes
        d = check("recon_after_repair_alter", c.post('/api/reconcile', json={'job_id': job}))
        assert d['status'] == 'pass'
        assert d['hash_mismatches'] == 0

        # 15. Injected failure migration
        d = check("fail_migrate", c.post('/api/migrate', json={'batch_size': 50, 'inject_failure': True, 'failure_at_batch': 2}))
        assert d['status'] == 'failed'
        fj = d['job_id']

        # 16. Resumption succeeds without re-failing
        d = check("resume_success", c.post('/api/migrate/resume', json={'job_id': fj}))
        assert d['status'] == 'completed'

        # 17. Verify resumed migration has 0 missing and 0 duplicates
        d = check("recon_after_resumption", c.post('/api/reconcile', json={'job_id': fj}))
        assert d['status'] == 'pass'
        assert len(d['missing']) == 0
        assert len(d['duplicates']) == 0

        # 18. Experiments and Index
        d = check("experiment_jobs", c.get('/api/experiment-jobs'))
        assert len(d['jobs']) >= 2
        check("index_page", c.get('/'))

        print(f"\n[+] All {passed} smoke tests passed successfully in isolated sandbox!")

if __name__ == '__main__':
    run_smoke_test()
