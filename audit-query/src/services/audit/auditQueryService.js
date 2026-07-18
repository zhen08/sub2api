const { AuditQueryError, isAbortError } = require('./auditQueryErrors')
const { getAuditQueryConfig } = require('./auditQueryConfig')

const ALLOWED_ARTIFACT_KINDS = new Set(['client_request', 'upstream_request', 'response'])
const EXACT_FILTER_KEYS = [
  'requestId',
  'userId',
  'userUsername',
  'apiKeyId',
  'apiKeyName',
  'protocol',
  'model',
  'status',
  'captureStatus'
]

function parseDate(value, fieldName) {
  const date = new Date(value)
  if (!value || Number.isNaN(date.getTime())) {
    throw new AuditQueryError(400, 'invalid_query', `${fieldName} must be a valid date`)
  }
  return date
}

function encodeCursor(call) {
  if (!call?.createdAt || !call?.requestId) {
    return null
  }
  return Buffer.from(
    JSON.stringify({
      createdAt: call.createdAt,
      requestId: call.requestId
    }),
    'utf8'
  ).toString('base64url')
}

function decodeCursor(value) {
  if (!value) {
    return null
  }
  try {
    const parsed = JSON.parse(Buffer.from(String(value), 'base64url').toString('utf8'))
    const createdAt = parseDate(parsed.createdAt, 'cursor.createdAt').toISOString()
    if (
      typeof parsed.requestId !== 'string' ||
      !parsed.requestId ||
      parsed.requestId.length > 512
    ) {
      throw new Error('invalid request id')
    }
    return { createdAt, requestId: parsed.requestId }
  } catch (error) {
    if (error instanceof AuditQueryError) {
      throw error
    }
    throw new AuditQueryError(400, 'invalid_cursor', 'cursor is invalid')
  }
}

function normalizeLimit(value, fallback, maximum, fieldName = 'limit') {
  if (value === undefined || value === null || value === '') {
    return fallback
  }
  const parsed = Number.parseInt(value, 10)
  if (
    !Number.isInteger(parsed) ||
    parsed < 1 ||
    parsed > maximum ||
    String(parsed) !== String(value)
  ) {
    throw new AuditQueryError(400, 'invalid_query', `${fieldName} must be between 1 and ${maximum}`)
  }
  return parsed
}

function parseStatusCode(value) {
  const normalized = String(value)
  if (!/^\d{3}$/.test(normalized)) {
    throw new AuditQueryError(400, 'invalid_query', 'statusCode must be between 100 and 599')
  }
  const statusCode = Number(normalized)
  if (statusCode < 100 || statusCode > 599) {
    throw new AuditQueryError(400, 'invalid_query', 'statusCode must be between 100 and 599')
  }
  return statusCode
}

async function mapWithConcurrency(items, concurrency, mapper) {
  const results = new Array(items.length)
  let nextIndex = 0

  async function worker() {
    while (nextIndex < items.length) {
      const index = nextIndex
      nextIndex += 1
      results[index] = await mapper(items[index], index)
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(Math.max(concurrency, 1), items.length) }, () => worker())
  )
  return results
}

class AuditQueryService {
  constructor(options = {}) {
    this.repository = options.repository
    this.objectStorage = options.objectStorage
    this.configProvider = options.configProvider || getAuditQueryConfig
  }

  normalizeFilters(input = {}, options = {}) {
    const config = this.configProvider()
    const requireRange = options.requireRange === true
    const now = new Date()
    let from
    let to

    if (requireRange && (!input.from || !input.to)) {
      throw new AuditQueryError(400, 'invalid_query', 'from and to are required')
    }

    if (input.to) {
      to = parseDate(input.to, 'to')
    } else {
      to = now
    }
    if (input.from) {
      from = parseDate(input.from, 'from')
    } else {
      from = new Date(to.getTime() - config.defaultListRangeMs)
    }

    if (from.getTime() > to.getTime()) {
      throw new AuditQueryError(400, 'invalid_query', 'from must not be later than to')
    }
    if (to.getTime() - from.getTime() > config.maxQueryRangeMs) {
      throw new AuditQueryError(
        400,
        'query_range_too_large',
        'Query range exceeds configured limit'
      )
    }

    const filters = {
      from: from.toISOString(),
      to: to.toISOString(),
      cursor: decodeCursor(input.cursor)
    }
    for (const key of EXACT_FILTER_KEYS) {
      if (input[key] === undefined || input[key] === null || input[key] === '') {
        continue
      }
      const value = String(input[key])
      if (value.length > 512) {
        throw new AuditQueryError(400, 'invalid_query', `${key} is too long`)
      }
      filters[key] = value
    }
    if (input.statusCode !== undefined && input.statusCode !== null && input.statusCode !== '') {
      filters.statusCode = parseStatusCode(input.statusCode)
    }
    return filters
  }

  normalizeArtifactKinds(value) {
    const kinds = value === undefined ? ['client_request', 'response'] : value
    if (!Array.isArray(kinds) || kinds.length === 0) {
      throw new AuditQueryError(400, 'invalid_query', 'artifactKinds must be a non-empty array')
    }
    const unique = [...new Set(kinds.map((kind) => String(kind)))]
    if (unique.some((kind) => !ALLOWED_ARTIFACT_KINDS.has(kind))) {
      throw new AuditQueryError(400, 'invalid_query', 'artifactKinds contains an unsupported kind')
    }
    return unique
  }

  async listCalls(input = {}) {
    const config = this.configProvider()
    const filters = this.normalizeFilters(input)
    const limit = normalizeLimit(input.limit, config.defaultListLimit, config.maxListLimit)
    const result = await this.repository.listCalls(filters, limit)
    const last = result.calls[result.calls.length - 1]
    return {
      calls: result.calls,
      hasMore: result.hasMore,
      nextCursor: result.hasMore ? encodeCursor(last) : null
    }
  }

  async getCallDetails(requestId) {
    if (!requestId || String(requestId).length > 512) {
      throw new AuditQueryError(400, 'invalid_request_id', 'requestId is invalid')
    }
    const call = await this.repository.getCallByRequestId(String(requestId))
    if (!call) {
      throw new AuditQueryError(404, 'call_not_found', 'Audit call was not found')
    }
    const artifacts = await this.repository.getArtifactsByRequestIds([call.requestId])
    return { call, artifacts }
  }

  async getArtifact(artifactId, options = {}) {
    if (!/^[1-9]\d*$/.test(String(artifactId || ''))) {
      throw new AuditQueryError(400, 'invalid_artifact_id', 'artifactId is invalid')
    }
    const descriptor = await this.repository.getArtifactById(String(artifactId))
    if (!descriptor) {
      throw new AuditQueryError(404, 'artifact_not_found', 'Artifact was not found')
    }
    const payload = await this.objectStorage.readArtifact(descriptor, options)
    return { artifact: descriptor, payload }
  }

  async readArtifactsForCall(descriptors, signal) {
    const config = this.configProvider()
    const results = await mapWithConcurrency(
      descriptors,
      config.s3Concurrency,
      async (descriptor) => {
        try {
          const payload = await this.objectStorage.readArtifact(descriptor, { signal })
          return { ok: true, artifact: descriptor, payload }
        } catch (error) {
          if (isAbortError(error)) {
            throw error
          }
          const knownError = error instanceof AuditQueryError
          return {
            ok: false,
            error: {
              artifactId: descriptor.id,
              kind: descriptor.kind,
              sequence: descriptor.sequence,
              code: knownError ? error.code : 'artifact_read_failed',
              message: knownError ? error.message : 'Artifact could not be read'
            }
          }
        }
      }
    )
    return {
      artifacts: results
        .filter((result) => result.ok)
        .map((result) => ({ artifact: result.artifact, payload: result.payload })),
      artifactErrors: results.filter((result) => !result.ok).map((result) => result.error)
    }
  }

  prepareExport(input = {}) {
    const config = this.configProvider()
    if (
      input.filters !== undefined &&
      (input.filters === null || typeof input.filters !== 'object' || Array.isArray(input.filters))
    ) {
      throw new AuditQueryError(400, 'invalid_query', 'filters must be an object')
    }
    const rawFilters = input.filters || {}
    const filters = this.normalizeFilters(
      {
        ...rawFilters,
        from: input.from,
        to: input.to,
        cursor: input.cursor
      },
      { requireRange: true }
    )
    return {
      rawFilters,
      filters,
      artifactKinds: this.normalizeArtifactKinds(input.artifactKinds),
      limit: normalizeLimit(
        input.limit,
        config.defaultExportLimit,
        config.maxExportRecords,
        'limit'
      )
    }
  }

  async *exportRecords(input = {}, options = {}) {
    const config = this.configProvider()
    const prepared = options.prepared || this.prepareExport(input)
    const { rawFilters, filters, artifactKinds, limit } = prepared
    const startedAt = new Date().toISOString()
    let exportedRecords = 0
    let artifactFailures = 0
    let hasMore = false
    let currentCursor = filters.cursor
    let lastCall = null

    yield {
      type: 'header',
      schemaVersion: 1,
      startedAt,
      from: filters.from,
      to: filters.to,
      filters: rawFilters,
      artifactKinds,
      limit
    }

    while (exportedRecords < limit) {
      if (options.signal?.aborted) {
        const error = new Error('Audit export aborted')
        error.name = 'AbortError'
        throw error
      }

      const batchLimit = Math.min(config.exportBatchSize, limit - exportedRecords)
      const result = await this.repository.listCalls(
        { ...filters, cursor: currentCursor },
        batchLimit
      )
      if (result.calls.length === 0) {
        hasMore = false
        break
      }

      const descriptors = await this.repository.getArtifactsByRequestIds(
        result.calls.map((call) => call.requestId)
      )
      const descriptorsByRequest = new Map()
      for (const descriptor of descriptors) {
        if (!artifactKinds.includes(descriptor.kind)) {
          continue
        }
        const list = descriptorsByRequest.get(descriptor.requestId) || []
        list.push(descriptor)
        descriptorsByRequest.set(descriptor.requestId, list)
      }

      for (const call of result.calls) {
        const artifactResult = await this.readArtifactsForCall(
          descriptorsByRequest.get(call.requestId) || [],
          options.signal
        )
        artifactFailures += artifactResult.artifactErrors.length
        exportedRecords += 1
        lastCall = call
        yield {
          type: 'record',
          schemaVersion: 1,
          call,
          artifacts: artifactResult.artifacts,
          artifactErrors: artifactResult.artifactErrors
        }
      }

      const { hasMore: batchHasMore } = result
      hasMore = batchHasMore
      if (!hasMore || exportedRecords >= limit) {
        break
      }
      currentCursor = decodeCursor(encodeCursor(lastCall))
    }

    yield {
      type: 'summary',
      schemaVersion: 1,
      complete: true,
      exportedRecords,
      artifactFailures,
      truncated: hasMore,
      nextCursor: hasMore ? encodeCursor(lastCall) : null,
      finishedAt: new Date().toISOString()
    }
  }
}

module.exports = {
  ALLOWED_ARTIFACT_KINDS,
  AuditQueryService,
  decodeCursor,
  encodeCursor,
  mapWithConcurrency,
  normalizeLimit
}
