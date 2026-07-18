const DEFAULT_MAX_QUERY_RANGE_MS = 7 * 24 * 60 * 60 * 1000

function parseInteger(
  value,
  fallback,
  { min = Number.MIN_SAFE_INTEGER, max = Number.MAX_SAFE_INTEGER } = {}
) {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed)) {
    return fallback
  }
  return Math.min(Math.max(parsed, min), max)
}

function normalizePrefix(value) {
  return String(value || 'ai-call-audit').replace(/^\/+|\/+$/g, '') || 'ai-call-audit'
}

function getAuditQueryConfig(env = process.env) {
  return {
    host: env.AUDIT_QUERY_HOST || '127.0.0.1',
    port: parseInteger(env.AUDIT_QUERY_PORT, 3100, { min: 1, max: 65535 }),
    tokenSha256: String(env.AUDIT_QUERY_TOKEN_SHA256 || '')
      .trim()
      .toLowerCase(),
    postgresUrl: env.AUDIT_QUERY_POSTGRES_URL || '',
    postgresPoolMax: parseInteger(env.AUDIT_QUERY_POSTGRES_POOL_MAX, 5, { min: 1, max: 20 }),
    postgresStatementTimeoutMs: parseInteger(env.AUDIT_QUERY_POSTGRES_STATEMENT_TIMEOUT_MS, 30000, {
      min: 1000,
      max: 300000
    }),
    s3Endpoint: env.AUDIT_QUERY_S3_ENDPOINT || '',
    s3Bucket: env.AUDIT_QUERY_S3_BUCKET || 'ai-call-audit',
    s3AccessKey: env.AUDIT_QUERY_S3_ACCESS_KEY || '',
    s3SecretKey: env.AUDIT_QUERY_S3_SECRET_KEY || '',
    s3Region: env.AUDIT_QUERY_S3_REGION || 'us-east-1',
    objectKeyPrefix: normalizePrefix(env.AUDIT_QUERY_OBJECT_KEY_PREFIX),
    defaultListRangeMs: parseInteger(env.AUDIT_QUERY_DEFAULT_LIST_RANGE_MS, 24 * 60 * 60 * 1000, {
      min: 60000,
      max: DEFAULT_MAX_QUERY_RANGE_MS
    }),
    maxQueryRangeMs: parseInteger(env.AUDIT_QUERY_MAX_RANGE_MS, DEFAULT_MAX_QUERY_RANGE_MS, {
      min: 60000,
      max: DEFAULT_MAX_QUERY_RANGE_MS
    }),
    defaultListLimit: parseInteger(env.AUDIT_QUERY_DEFAULT_LIST_LIMIT, 50, {
      min: 1,
      max: 200
    }),
    maxListLimit: parseInteger(env.AUDIT_QUERY_MAX_LIST_LIMIT, 200, { min: 1, max: 200 }),
    defaultExportLimit: parseInteger(env.AUDIT_QUERY_DEFAULT_EXPORT_LIMIT, 1000, {
      min: 1,
      max: 10000
    }),
    maxExportRecords: parseInteger(env.AUDIT_QUERY_MAX_EXPORT_RECORDS, 10000, {
      min: 1,
      max: 100000
    }),
    exportBatchSize: parseInteger(env.AUDIT_QUERY_EXPORT_BATCH_SIZE, 100, {
      min: 1,
      max: 500
    }),
    maxConcurrentExports: parseInteger(env.AUDIT_QUERY_MAX_CONCURRENT_EXPORTS, 2, {
      min: 1,
      max: 20
    }),
    s3Concurrency: parseInteger(env.AUDIT_QUERY_S3_CONCURRENCY, 2, { min: 1, max: 10 }),
    exportTimeoutMs: parseInteger(env.AUDIT_QUERY_EXPORT_TIMEOUT_MS, 30 * 60 * 1000, {
      min: 1000,
      max: 2 * 60 * 60 * 1000
    }),
    maxArtifactCompressedBytes: parseInteger(
      env.AUDIT_QUERY_MAX_ARTIFACT_COMPRESSED_BYTES,
      32 * 1024 * 1024,
      { min: 1024, max: 512 * 1024 * 1024 }
    ),
    maxArtifactUncompressedBytes: parseInteger(
      env.AUDIT_QUERY_MAX_ARTIFACT_UNCOMPRESSED_BYTES,
      100 * 1024 * 1024,
      { min: 1024, max: 1024 * 1024 * 1024 }
    )
  }
}

function validateAuditQueryConfig(config) {
  const missing = []
  if (!config.postgresUrl) {
    missing.push('AUDIT_QUERY_POSTGRES_URL')
  }
  if (!config.s3Endpoint) {
    missing.push('AUDIT_QUERY_S3_ENDPOINT')
  }
  if (!config.s3Bucket) {
    missing.push('AUDIT_QUERY_S3_BUCKET')
  }
  if (!config.s3AccessKey) {
    missing.push('AUDIT_QUERY_S3_ACCESS_KEY')
  }
  if (!config.s3SecretKey) {
    missing.push('AUDIT_QUERY_S3_SECRET_KEY')
  }
  if (!/^[a-f0-9]{64}$/.test(config.tokenSha256)) {
    missing.push('AUDIT_QUERY_TOKEN_SHA256 (64 lowercase hex characters)')
  }

  if (missing.length > 0) {
    const error = new Error(`Missing or invalid audit query configuration: ${missing.join(', ')}`)
    error.code = 'invalid_audit_query_config'
    throw error
  }

  return config
}

module.exports = {
  DEFAULT_MAX_QUERY_RANGE_MS,
  getAuditQueryConfig,
  validateAuditQueryConfig
}
