const {
  findMissingIndexes,
  indexDefinitions,
  quoteIdentifier
} = require('../scripts/migrate-audit-query-schema')

describe('audit query schema migration', () => {
  test('defines all required indexes for every existing audit partition', () => {
    const definitions = indexDefinitions('audit_calls_2026_07')
    expect(definitions.map((definition) => definition.name)).toEqual([
      'audit_calls_2026_07_query_order_idx',
      'audit_calls_2026_07_request_id_idx',
      'audit_calls_2026_07_user_query_idx',
      'audit_calls_2026_07_api_key_query_idx'
    ])

    const missing = findMissingIndexes({
      partitions: [{ partition_name: 'audit_calls_2026_07' }],
      indexes: new Set(['audit_artifacts_request_id_id_idx'])
    })
    expect(missing).toEqual(definitions.map((definition) => definition.name))
  })

  test('quotes PostgreSQL identifiers safely', () => {
    expect(quoteIdentifier('public')).toBe('"public"')
    expect(quoteIdentifier('unexpected"name')).toBe('"unexpected""name"')
  })
})
