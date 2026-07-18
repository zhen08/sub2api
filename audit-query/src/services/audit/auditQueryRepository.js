const { Pool } = require('pg')
const { getAuditQueryConfig } = require('./auditQueryConfig')

const CALL_SELECT = `
  c.request_id, c.created_at, c.request_started_at, c.endpoint, c.method, c.protocol,
  c.api_key_id, c.api_key_name, c.user_id, c.user_username, c.account_id,
  c.account_type, c.model, c.status, c.status_code, c.stream, c.input_tokens,
  c.output_tokens, c.cache_read_tokens, c.cache_create_tokens, c.total_tokens,
  c.cost, c.real_cost, c.retention_until, c.capture_status, c.error, c.meta,
  c.updated_at
`

function toIsoString(value) {
  if (!value) {
    return null
  }
  const date = value instanceof Date ? value : new Date(value)
  return Number.isNaN(date.getTime()) ? null : date.toISOString()
}

function mapCallRow(row = {}) {
  return {
    requestId: row.request_id,
    createdAt: toIsoString(row.created_at),
    requestStartedAt: toIsoString(row.request_started_at),
    endpoint: row.endpoint,
    method: row.method,
    protocol: row.protocol,
    apiKeyId: row.api_key_id,
    apiKeyName: row.api_key_name,
    userId: row.user_id,
    userUsername: row.user_username,
    accountId: row.account_id,
    accountType: row.account_type,
    model: row.model,
    status: row.status,
    statusCode: row.status_code,
    stream: row.stream === true,
    inputTokens: String(row.input_tokens ?? '0'),
    outputTokens: String(row.output_tokens ?? '0'),
    cacheReadTokens: String(row.cache_read_tokens ?? '0'),
    cacheCreateTokens: String(row.cache_create_tokens ?? '0'),
    totalTokens: String(row.total_tokens ?? '0'),
    cost: String(row.cost ?? '0'),
    realCost: String(row.real_cost ?? '0'),
    retentionUntil: toIsoString(row.retention_until),
    captureStatus: row.capture_status,
    error: row.error,
    meta: row.meta || {},
    updatedAt: toIsoString(row.updated_at)
  }
}

function mapArtifactRow(row = {}) {
  return {
    id: String(row.id),
    requestId: row.request_id,
    kind: row.kind,
    sequence: Number(row.sequence || 0),
    bucket: row.bucket,
    objectKey: row.object_key,
    bytes: String(row.bytes ?? '0'),
    sha256: row.sha256,
    contentType: row.content_type,
    createdAt: toIsoString(row.created_at)
  }
}

class AuditQueryRepository {
  constructor(options = {}) {
    this.configProvider = options.configProvider || getAuditQueryConfig
    this.pool = options.pool || null
  }

  getPool() {
    if (this.pool) {
      return this.pool
    }

    const config = this.configProvider()
    if (!config.postgresUrl) {
      throw new Error('AUDIT_QUERY_POSTGRES_URL is required')
    }

    this.pool = new Pool({
      connectionString: config.postgresUrl,
      max: config.postgresPoolMax || 5,
      statement_timeout: config.postgresStatementTimeoutMs || 30000,
      query_timeout: config.postgresStatementTimeoutMs || 30000,
      application_name: 'sub2api-audit-query'
    })
    return this.pool
  }

  async query(sql, params = []) {
    return this.getPool().query(sql, params)
  }

  async checkReady() {
    const result = await this.query(`
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
    const state = result.rows[0] || {}
    const ready =
      state.calls_ready === true && state.artifacts_ready === true && state.sequence_ready === true
    if (!ready) {
      const error = new Error('Audit query schema is not ready')
      error.code = 'audit_query_schema_not_ready'
      throw error
    }
    await this.query(`
      SELECT c.request_id, a.sequence
      FROM audit_calls c
      LEFT JOIN audit_artifacts a ON a.request_id = c.request_id
      LIMIT 0
    `)
    return true
  }

  buildCallWhere(filters = {}, startIndex = 1) {
    const clauses = ['c.retention_until > now()']
    const params = []
    const add = (sql, value) => {
      params.push(value)
      clauses.push(sql.replace('?', `$${startIndex + params.length - 1}`))
    }

    add('c.created_at >= ?', filters.from)
    add('c.created_at <= ?', filters.to)

    const exactFilters = [
      ['requestId', 'c.request_id'],
      ['userId', 'c.user_id'],
      ['userUsername', 'c.user_username'],
      ['apiKeyId', 'c.api_key_id'],
      ['apiKeyName', 'c.api_key_name'],
      ['protocol', 'c.protocol'],
      ['model', 'c.model'],
      ['status', 'c.status'],
      ['captureStatus', 'c.capture_status']
    ]
    for (const [key, column] of exactFilters) {
      if (filters[key] !== undefined && filters[key] !== null && filters[key] !== '') {
        add(`${column} = ?`, filters[key])
      }
    }
    if (
      filters.statusCode !== undefined &&
      filters.statusCode !== null &&
      filters.statusCode !== ''
    ) {
      add('c.status_code = ?', filters.statusCode)
    }
    if (filters.cursor) {
      params.push(filters.cursor.createdAt, filters.cursor.requestId)
      const createdAtParam = `$${startIndex + params.length - 2}`
      const requestIdParam = `$${startIndex + params.length - 1}`
      clauses.push(`(c.created_at, c.request_id) < (${createdAtParam}, ${requestIdParam})`)
    }

    return { clauses, params }
  }

  async listCalls(filters = {}, limit = 50) {
    const { clauses, params } = this.buildCallWhere(filters)
    params.push(limit + 1)
    const result = await this.query(
      `
        SELECT ${CALL_SELECT}
        FROM audit_calls c
        WHERE ${clauses.join('\n          AND ')}
        ORDER BY c.created_at DESC, c.request_id DESC
        LIMIT $${params.length}
      `,
      params
    )
    const hasMore = result.rows.length > limit
    return {
      calls: result.rows.slice(0, limit).map(mapCallRow),
      hasMore
    }
  }

  async getCallByRequestId(requestId) {
    const result = await this.query(
      `
        SELECT ${CALL_SELECT}
        FROM audit_calls c
        WHERE c.request_id = $1
          AND c.retention_until > now()
        ORDER BY c.created_at DESC
        LIMIT 1
      `,
      [requestId]
    )
    return result.rows[0] ? mapCallRow(result.rows[0]) : null
  }

  async getArtifactsByRequestIds(requestIds = []) {
    if (requestIds.length === 0) {
      return []
    }
    const result = await this.query(
      `
        SELECT id, request_id, kind, sequence, bucket, object_key, bytes, sha256,
               content_type, created_at
        FROM audit_artifacts
        WHERE request_id = ANY($1::text[])
        ORDER BY request_id, kind, sequence, id
      `,
      [requestIds]
    )
    return result.rows.map(mapArtifactRow)
  }

  async getArtifactById(artifactId) {
    const result = await this.query(
      `
        SELECT a.id, a.request_id, a.kind, a.sequence, a.bucket, a.object_key,
               a.bytes, a.sha256, a.content_type, a.created_at
        FROM audit_artifacts a
        WHERE a.id = $1
          AND EXISTS (
            SELECT 1
            FROM audit_calls c
            WHERE c.request_id = a.request_id
              AND c.retention_until > now()
          )
        LIMIT 1
      `,
      [artifactId]
    )
    return result.rows[0] ? mapArtifactRow(result.rows[0]) : null
  }

  async close() {
    if (this.pool && typeof this.pool.end === 'function') {
      const { pool } = this
      this.pool = null
      await pool.end()
    }
  }
}

module.exports = {
  AuditQueryRepository,
  mapArtifactRow,
  mapCallRow
}
