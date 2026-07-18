#!/usr/bin/env node

require('dotenv').config()
const { Pool } = require('pg')

function quoteIdentifier(value) {
  return `"${String(value).replace(/"/g, '""')}"`
}

function indexDefinitions(partition) {
  return [
    {
      name: `${partition}_query_order_idx`,
      columns: '(created_at DESC, request_id DESC)'
    },
    {
      name: `${partition}_request_id_idx`,
      columns: '(request_id)'
    },
    {
      name: `${partition}_user_query_idx`,
      columns: '(user_id, created_at DESC, request_id DESC)',
      where: 'WHERE user_id IS NOT NULL'
    },
    {
      name: `${partition}_api_key_query_idx`,
      columns: '(api_key_id, created_at DESC, request_id DESC)',
      where: 'WHERE api_key_id IS NOT NULL'
    }
  ]
}

async function getState(pool) {
  const tables = await pool.query(`
    SELECT
      to_regclass('public.audit_calls') IS NOT NULL AS calls_ready,
      to_regclass('public.audit_artifacts') IS NOT NULL AS artifacts_ready,
      EXISTS (
        SELECT 1
        FROM pg_attribute
        WHERE attrelid = to_regclass('public.audit_artifacts')
          AND attname = 'sequence'
          AND NOT attisdropped
      ) AS sequence_ready
  `)
  const partitions = await pool.query(`
    SELECT child.relname AS partition_name, namespace.nspname AS schema_name
    FROM pg_inherits inheritance
    JOIN pg_class parent ON parent.oid = inheritance.inhparent
    JOIN pg_class child ON child.oid = inheritance.inhrelid
    JOIN pg_namespace namespace ON namespace.oid = child.relnamespace
    WHERE parent.oid = to_regclass('public.audit_calls')
    ORDER BY child.relname
  `)
  const indexes = await pool.query(`
    SELECT index_class.relname AS indexname
    FROM pg_index index_state
    JOIN pg_class index_class ON index_class.oid = index_state.indexrelid
    JOIN pg_class table_class ON table_class.oid = index_state.indrelid
    JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace
    WHERE namespace.nspname = 'public'
      AND index_state.indisvalid = true
      AND (table_class.relname = 'audit_artifacts' OR table_class.relname LIKE 'audit_calls_%')
  `)
  return {
    ...tables.rows[0],
    partitions: partitions.rows,
    indexes: new Set(indexes.rows.map((row) => row.indexname))
  }
}

function findMissingIndexes(state) {
  const expected = ['audit_artifacts_request_id_id_idx']
  for (const { partition_name: partition } of state.partitions) {
    expected.push(...indexDefinitions(partition).map((definition) => definition.name))
  }
  return expected.filter((name) => !state.indexes.has(name))
}

async function applyMigration(pool, state) {
  if (!state.calls_ready || !state.artifacts_ready) {
    throw new Error('audit_calls and audit_artifacts must exist before applying this migration')
  }

  await pool.query(`
    ALTER TABLE public.audit_artifacts
    ADD COLUMN IF NOT EXISTS sequence INTEGER NOT NULL DEFAULT 0
  `)
  await pool.query(`
    CREATE INDEX CONCURRENTLY IF NOT EXISTS audit_artifacts_request_id_id_idx
    ON public.audit_artifacts (request_id, id)
  `)

  for (const { partition_name: partition, schema_name: schema } of state.partitions) {
    const tableName = `${quoteIdentifier(schema)}.${quoteIdentifier(partition)}`
    for (const definition of indexDefinitions(partition)) {
      await pool.query(
        `CREATE INDEX CONCURRENTLY IF NOT EXISTS ${quoteIdentifier(definition.name)} ` +
          `ON ${tableName} ${definition.columns} ${definition.where || ''}`
      )
    }
  }
}

async function main() {
  const apply = process.argv.includes('--apply')
  const postgresUrl =
    process.env.AUDIT_QUERY_MIGRATION_POSTGRES_URL || process.env.AUDIT_POSTGRES_URL || ''
  if (!postgresUrl) {
    throw new Error('AUDIT_QUERY_MIGRATION_POSTGRES_URL or AUDIT_POSTGRES_URL is required')
  }

  const pool = new Pool({
    connectionString: postgresUrl,
    max: 1,
    application_name: 'sub2api-audit-query-migration'
  })
  try {
    let state = await getState(pool)
    if (apply) {
      await applyMigration(pool, state)
      state = await getState(pool)
    }

    const missingIndexes = findMissingIndexes(state)
    const ready =
      state.calls_ready === true &&
      state.artifacts_ready === true &&
      state.sequence_ready === true &&
      missingIndexes.length === 0
    process.stdout.write(
      `${JSON.stringify(
        {
          mode: apply ? 'apply' : 'check',
          ready,
          callsReady: state.calls_ready === true,
          artifactsReady: state.artifacts_ready === true,
          sequenceReady: state.sequence_ready === true,
          partitions: state.partitions.map((row) => row.partition_name),
          missingIndexes
        },
        null,
        2
      )}\n`
    )
    if (!ready) {
      process.exitCode = 1
    }
  } finally {
    await pool.end()
  }
}

if (require.main === module) {
  main().catch((error) => {
    process.stderr.write(`${error.message}\n`)
    process.exit(1)
  })
}

module.exports = {
  applyMigration,
  findMissingIndexes,
  getState,
  indexDefinitions,
  quoteIdentifier
}
