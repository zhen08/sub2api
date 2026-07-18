const crypto = require('crypto')
const express = require('express')
const helmet = require('helmet')
const zlib = require('zlib')
const { once } = require('events')
const { v4: uuidv4 } = require('uuid')
const logger = require('./services/audit/auditQueryLogger')
const {
  getAuditQueryConfig,
  validateAuditQueryConfig
} = require('./services/audit/auditQueryConfig')
const { AuditQueryError, isAbortError } = require('./services/audit/auditQueryErrors')
const { AuditQueryObjectStorage } = require('./services/audit/auditQueryObjectStorage')
const { AuditQueryRepository } = require('./services/audit/auditQueryRepository')
const { AuditQueryService } = require('./services/audit/auditQueryService')

function secureTokenMatches(token, expectedSha256) {
  if (typeof token !== 'string' || token.length < 32 || token.length > 512) {
    return false
  }
  if (!/^[a-f0-9]{64}$/.test(String(expectedSha256 || ''))) {
    return false
  }
  const actual = crypto.createHash('sha256').update(token).digest()
  const expected = Buffer.from(expectedSha256, 'hex')
  return actual.length === expected.length && crypto.timingSafeEqual(actual, expected)
}

function createAuditQueryAuth(configProvider) {
  return (req, res, next) => {
    const authorization = req.headers.authorization || ''
    const match = authorization.match(/^Bearer\s+(.+)$/i)
    if (!match || !secureTokenMatches(match[1].trim(), configProvider().tokenSha256)) {
      return res.status(401).json({
        success: false,
        error: {
          code: 'unauthorized',
          message: 'A valid audit query Bearer token is required',
          requestId: req.requestId
        }
      })
    }
    return next()
  }
}

function sendError(res, req, error) {
  const statusCode = error instanceof AuditQueryError ? error.statusCode : 500
  const code = error instanceof AuditQueryError ? error.code : 'internal_error'
  const message = error instanceof AuditQueryError ? error.message : 'Internal server error'
  return res.status(statusCode).json({
    success: false,
    error: {
      code,
      message,
      requestId: req.requestId,
      ...(error instanceof AuditQueryError && error.details ? { details: error.details } : {})
    }
  })
}

async function writeGzipLine(gzip, value) {
  if (!gzip.write(`${JSON.stringify(value)}\n`, 'utf8')) {
    await once(gzip, 'drain')
  }
}

function createAuditQueryApp(options = {}) {
  const configProvider = options.configProvider || getAuditQueryConfig
  const repository = options.repository || new AuditQueryRepository({ configProvider })
  const objectStorage = options.objectStorage || new AuditQueryObjectStorage({ configProvider })
  const service =
    options.service || new AuditQueryService({ repository, objectStorage, configProvider })
  const appLogger = options.logger || logger
  const app = express()
  let activeExports = 0

  app.disable('x-powered-by')
  app.set('trust proxy', 1)
  app.use(helmet({ contentSecurityPolicy: false }))
  app.use((req, res, next) => {
    const supplied = String(req.headers['x-request-id'] || '')
    req.requestId = /^[A-Za-z0-9._:-]{1,128}$/.test(supplied) ? supplied : uuidv4()
    req.requestStartedAt = Date.now()
    res.setHeader('X-Request-Id', req.requestId)
    res.setHeader('Cache-Control', 'no-store')
    res.once('finish', () => {
      appLogger.info('Audit query request completed', {
        requestId: req.requestId,
        method: req.method,
        path: req.path,
        statusCode: res.statusCode,
        durationMs: Date.now() - req.requestStartedAt,
        remoteAddress: req.ip || req.socket?.remoteAddress || null
      })
    })
    next()
  })

  app.get('/healthz', (_req, res) => {
    res.json({ status: 'ok', service: 'audit-query' })
  })

  app.get('/readyz', async (req, res) => {
    try {
      validateAuditQueryConfig(configProvider())
      await repository.checkReady()
      return res.json({ status: 'ready', service: 'audit-query' })
    } catch (error) {
      appLogger.warn('Audit query readiness check failed', {
        requestId: req.requestId,
        code: error.code || 'readiness_failed'
      })
      return res.status(503).json({ status: 'not_ready', service: 'audit-query' })
    }
  })

  app.use('/v1/audit', createAuditQueryAuth(configProvider))
  app.use('/v1/audit', express.json({ limit: '64kb' }))

  app.get('/v1/audit/calls', async (req, res) => {
    try {
      const data = await service.listCalls(req.query || {})
      return res.json({ success: true, data })
    } catch (error) {
      appLogger.warn('Audit call query failed', {
        requestId: req.requestId,
        code: error.code || 'query_failed'
      })
      return sendError(res, req, error)
    }
  })

  app.get('/v1/audit/calls/:requestId', async (req, res) => {
    try {
      const data = await service.getCallDetails(req.params.requestId)
      return res.json({ success: true, data })
    } catch (error) {
      appLogger.warn('Audit call detail query failed', {
        requestId: req.requestId,
        code: error.code || 'query_failed'
      })
      return sendError(res, req, error)
    }
  })

  app.get('/v1/audit/artifacts/:artifactId', async (req, res) => {
    try {
      const data = await service.getArtifact(req.params.artifactId)
      return res.json({ success: true, data })
    } catch (error) {
      appLogger.warn('Audit artifact query failed', {
        requestId: req.requestId,
        code: error.code || 'artifact_query_failed'
      })
      return sendError(res, req, error)
    }
  })

  app.post('/v1/audit/exports/stream', async (req, res) => {
    const config = configProvider()
    if (activeExports >= config.maxConcurrentExports) {
      return sendError(
        res,
        req,
        new AuditQueryError(
          429,
          'export_concurrency_exceeded',
          'Too many audit exports are running'
        )
      )
    }

    const abortController = new AbortController()
    const timeout = setTimeout(() => {
      const error = new Error('Audit export timed out')
      error.code = 'audit_export_timeout'
      abortController.abort(error)
    }, config.exportTimeoutMs)
    timeout.unref?.()
    const abortOnDisconnect = () => {
      if (!res.writableEnded) {
        abortController.abort()
      }
    }
    req.once('aborted', abortOnDisconnect)
    res.once('close', abortOnDisconnect)

    activeExports += 1
    let gzip = null
    let streamStarted = false
    try {
      const prepared = service.prepareExport(req.body || {})
      const records = service.exportRecords(req.body || {}, {
        signal: abortController.signal,
        prepared
      })
      res.status(200)
      res.setHeader('Content-Type', 'application/x-ndjson; charset=utf-8')
      res.setHeader('Content-Encoding', 'gzip')
      res.setHeader('X-Accel-Buffering', 'no')
      gzip = zlib.createGzip()
      gzip.on('error', (error) => {
        appLogger.warn('Audit export gzip stream failed', {
          requestId: req.requestId,
          code: error.code || 'gzip_stream_failed'
        })
        abortController.abort(error)
      })
      gzip.pipe(res)
      streamStarted = true

      for await (const record of records) {
        await writeGzipLine(gzip, record)
      }
    } catch (error) {
      if (!streamStarted) {
        return sendError(res, req, error)
      }
      if (!res.destroyed && !gzip.destroyed) {
        const timeoutReached = abortController.signal.reason?.code === 'audit_export_timeout'
        const knownError = error instanceof AuditQueryError
        await writeGzipLine(gzip, {
          type: 'summary',
          schemaVersion: 1,
          complete: false,
          error: {
            code: timeoutReached
              ? 'export_timeout'
              : isAbortError(error)
                ? 'export_aborted'
                : knownError
                  ? error.code
                  : 'export_failed',
            message: timeoutReached
              ? 'Audit export exceeded its time limit'
              : knownError
                ? error.message
                : 'Audit export failed'
          },
          finishedAt: new Date().toISOString()
        }).catch(() => {})
      }
    } finally {
      clearTimeout(timeout)
      req.removeListener('aborted', abortOnDisconnect)
      res.removeListener('close', abortOnDisconnect)
      activeExports -= 1
      if (gzip && !gzip.destroyed) {
        gzip.end()
      }
    }
    return undefined
  })

  app.use((req, res) =>
    sendError(res, req, new AuditQueryError(404, 'not_found', 'Route not found'))
  )

  app.use((error, req, res, next) => {
    if (res.headersSent) {
      return next(error)
    }
    if (error?.type === 'entity.too.large') {
      return sendError(
        res,
        req,
        new AuditQueryError(413, 'request_body_too_large', 'Request body exceeds 64kb')
      )
    }
    if (error instanceof SyntaxError && error.status === 400) {
      return sendError(
        res,
        req,
        new AuditQueryError(400, 'invalid_json', 'Request JSON is invalid')
      )
    }
    appLogger.error('Unhandled audit query request error', {
      requestId: req.requestId,
      code: error?.code || 'internal_error'
    })
    return sendError(res, req, error)
  })

  return { app, repository, service }
}

async function startAuditQueryServer() {
  const config = validateAuditQueryConfig(getAuditQueryConfig())
  const { app, repository } = createAuditQueryApp({ configProvider: () => config })
  const server = app.listen(config.port, config.host, () => {
    logger.info(`Audit query service listening on ${config.host}:${config.port}`)
  })
  server.requestTimeout = config.exportTimeoutMs + 60000
  server.headersTimeout = Math.min(server.requestTimeout, 65000)

  let shuttingDown = false
  const shutdown = async (signal) => {
    if (shuttingDown) {
      return
    }
    shuttingDown = true
    logger.info(`Audit query service received ${signal}`)
    server.close(async () => {
      await repository.close().catch((error) => {
        logger.warn(`Audit query PostgreSQL shutdown failed: ${error.message}`)
      })
      process.exit(0)
    })
    setTimeout(() => process.exit(1), 10000).unref()
  }
  process.once('SIGTERM', () => shutdown('SIGTERM'))
  process.once('SIGINT', () => shutdown('SIGINT'))
  return server
}

if (require.main === module) {
  startAuditQueryServer().catch((error) => {
    logger.error(`Audit query service failed to start: ${error.message}`)
    process.exit(1)
  })
}

module.exports = {
  createAuditQueryApp,
  createAuditQueryAuth,
  secureTokenMatches,
  startAuditQueryServer,
  writeGzipLine
}
