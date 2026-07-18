const {
  getAuditQueryConfig,
  validateAuditQueryConfig
} = require('../src/services/audit/auditQueryConfig')

describe('auditQueryConfig', () => {
  test('uses safe local defaults and validates required connection settings', () => {
    const config = getAuditQueryConfig({
      AUDIT_QUERY_TOKEN_SHA256: 'a'.repeat(64),
      AUDIT_QUERY_POSTGRES_URL: 'postgresql://reader@db/ai_audit',
      AUDIT_QUERY_S3_ENDPOINT: 'https://s3.example.com',
      AUDIT_QUERY_S3_ACCESS_KEY: 'reader',
      AUDIT_QUERY_S3_SECRET_KEY: 'secret'
    })

    expect(validateAuditQueryConfig(config)).toBe(config)
    expect(config.host).toBe('127.0.0.1')
    expect(config.port).toBe(3100)
    expect(config.maxQueryRangeMs).toBe(7 * 24 * 60 * 60 * 1000)
    expect(config.maxExportRecords).toBe(10000)
  })

  test('rejects a plaintext token in the hash setting', () => {
    const config = getAuditQueryConfig({
      AUDIT_QUERY_TOKEN_SHA256: 'plaintext-token',
      AUDIT_QUERY_POSTGRES_URL: 'postgresql://reader@db/ai_audit',
      AUDIT_QUERY_S3_ENDPOINT: 'https://s3.example.com',
      AUDIT_QUERY_S3_ACCESS_KEY: 'reader',
      AUDIT_QUERY_S3_SECRET_KEY: 'secret'
    })

    expect(() => validateAuditQueryConfig(config)).toThrow(/AUDIT_QUERY_TOKEN_SHA256/)
  })

  test('cannot raise the public list range or page size above the compatibility contract', () => {
    const config = getAuditQueryConfig({
      AUDIT_QUERY_MAX_RANGE_MS: String(31 * 24 * 60 * 60 * 1000),
      AUDIT_QUERY_MAX_LIST_LIMIT: '1000'
    })
    expect(config.maxQueryRangeMs).toBe(7 * 24 * 60 * 60 * 1000)
    expect(config.maxListLimit).toBe(200)
  })
})
