"""Render the pinned online migration transaction; this module never connects.

Execution against production requires separate, explicit user confirmation.
There is no down migration: rollback keeps the compatible schema and data.
"""
import hashlib
import json

PENDING = {
    '241_add_payment_order_bonus_amount.sql': '18b6a524a9873d3e4a45e6f9bd03b6e69f4384b701825c659817e017f2e8244f',
    '241_add_typesafe_platform.sql': 'b6559525bf8d0b5d7c617e7415944f0d1fae8c408e9131ace84139795f66dcd2',
    '242_drop_platform_check_constraints.sql': 'a7725f31f637208d200e0f37d5270e9f263743d99fa9e4f1bb9a7387e8be5a61',
}


def render(payload):
    before = payload['baseline']
    pending = payload['pending']
    if len(before) != 290 or payload['expected_post_ledger'] != {**before, **PENDING}:
        raise ValueError('unexpected_ledger_delta')
    if set(pending) != set(PENDING):
        raise ValueError('unexpected_migration_files')
    for name, raw in pending.items():
        if hashlib.sha256(raw.strip().encode()).hexdigest() != PENDING[name]:
            raise ValueError('migration_checksum_changed')
    expected = json.dumps(before, sort_keys=True, separators=(',', ':'))
    # Refuse SQL literal injection even if an assessment file was replaced.
    if any(char in expected for char in ("'", '$', '\\')):
        raise ValueError('unexpected_baseline_names')
    statements = [
        'BEGIN;',
        "SET LOCAL lock_timeout='500ms';",
        "SET LOCAL statement_timeout='5s';",
        "SET LOCAL idle_in_transaction_session_timeout='5s';",
        "DO $bg$ BEGIN IF NOT pg_try_advisory_xact_lock(694208311321144027) THEN RAISE EXCEPTION 'another migration is running'; END IF; END $bg$;",
        # Acquire all needed locks immediately, rather than queue behind a long
        # transaction and make unrelated production requests queue behind DDL.
        'LOCK TABLE payment_orders, user_platform_quotas, composite_model_routes, schema_migrations IN ACCESS EXCLUSIVE MODE NOWAIT;',
        "DO $bg$ BEGIN IF (SELECT count(*) FROM schema_migrations) <> 290 OR (SELECT jsonb_object_agg(filename,checksum) FROM schema_migrations) IS DISTINCT FROM '" + expected + "'::jsonb THEN RAISE EXCEPTION 'migration ledger drift'; END IF; END $bg$;",
    ]
    for name, raw in sorted(pending.items()):
        statements += [raw, "INSERT INTO schema_migrations(filename,checksum) VALUES('" + name + "','" + PENDING[name] + "');"]
    statements.append('COMMIT;')
    return '\n'.join(statements)
