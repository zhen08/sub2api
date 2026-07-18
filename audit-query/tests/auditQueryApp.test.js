const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const zlib = require('zlib')
const request = require('supertest')
const { AuditQueryError } = require('../src/services/audit/auditQueryErrors')
const { createAuditQueryApp, secureTokenMatches } = require('../src/auditQueryApp')

const RAW_TOKEN = 'audit-query-test-token-that-is-longer-than-32-chars'
const TOKEN_SHA256 = crypto.createHash('sha256').update(RAW_TOKEN).digest('hex')

function createConfig() {
  return {
    tokenSha256: TOKEN_SHA256,
    maxConcurrentExports: 2,
    exportTimeoutMs: 30000
  }
}

function createLogger() {
  return {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  }
}

function bufferParser(res, callback) {
  const chunks = []
  res.on('data', (chunk) => chunks.push(Buffer.from(chunk)))
  res.on('end', () => callback(null, Buffer.concat(chunks)))
}

describe('auditQueryApp', () => {
  test('uses a constant-time compatible SHA-256 token check', () => {
    expect(secureTokenMatches(RAW_TOKEN, TOKEN_SHA256)).toBe(true)
    expect(secureTokenMatches(`${RAW_TOKEN}-wrong`, TOKEN_SHA256)).toBe(false)
  })

  test('protects audit APIs while keeping health checks available', async () => {
    const service = {
      listCalls: jest.fn().mockResolvedValue({ calls: [], hasMore: false, nextCursor: null })
    }
    const repository = { checkReady: jest.fn().mockResolvedValue(true) }
    const { app } = createAuditQueryApp({
      configProvider: createConfig,
      repository,
      service,
      logger: createLogger()
    })

    await request(app).get('/healthz').expect(200, { status: 'ok', service: 'audit-query' })
    await request(app).get('/v1/audit/calls').expect(401)
    const response = await request(app)
      .get('/v1/audit/calls')
      .set('Authorization', `Bearer ${RAW_TOKEN}`)
      .expect(200)

    expect(response.body.data.calls).toEqual([])
    expect(service.listCalls).toHaveBeenCalledTimes(1)
  })

  test('validates exports before starting the gzip stream', async () => {
    const service = {
      prepareExport: jest.fn(() => {
        throw new AuditQueryError(400, 'invalid_query', 'from and to are required')
      }),
      exportRecords: jest.fn()
    }
    const { app } = createAuditQueryApp({
      configProvider: createConfig,
      repository: { checkReady: jest.fn() },
      service,
      logger: createLogger()
    })

    const response = await request(app)
      .post('/v1/audit/exports/stream')
      .set('Authorization', `Bearer ${RAW_TOKEN}`)
      .send({})
      .expect(400)

    expect(response.headers['content-encoding']).toBeUndefined()
    expect(response.body.error.code).toBe('invalid_query')
    expect(service.exportRecords).not.toHaveBeenCalled()
  })

  test('returns a stable JSON error for malformed request JSON', async () => {
    const { app } = createAuditQueryApp({
      configProvider: createConfig,
      repository: { checkReady: jest.fn() },
      service: {},
      logger: createLogger()
    })

    const response = await request(app)
      .post('/v1/audit/exports/stream')
      .set('Authorization', `Bearer ${RAW_TOKEN}`)
      .set('Content-Type', 'application/json')
      .send('{broken')
      .expect(400)

    expect(response.body.error.code).toBe('invalid_json')
    expect(response.body.error.requestId).toBeTruthy()
  })

  test('returns gzip NDJSON with header, record, and summary entries', async () => {
    const service = {
      prepareExport: jest.fn(() => ({ prepared: true })),
      async *exportRecords() {
        yield { type: 'header', schemaVersion: 1 }
        yield { type: 'record', schemaVersion: 1, call: { requestId: 'req_1' } }
        yield { type: 'summary', schemaVersion: 1, complete: true }
      }
    }
    const { app } = createAuditQueryApp({
      configProvider: createConfig,
      repository: { checkReady: jest.fn() },
      service,
      logger: createLogger()
    })

    const response = await request(app)
      .post('/v1/audit/exports/stream')
      .set('Authorization', `Bearer ${RAW_TOKEN}`)
      .send({ from: '2026-07-09T00:00:00Z', to: '2026-07-10T00:00:00Z' })
      .buffer(true)
      .parse(bufferParser)
      .expect(200)

    const raw = response.body
    const decoded = raw[0] === 0x1f && raw[1] === 0x8b ? zlib.gunzipSync(raw) : raw
    const entries = decoded
      .toString('utf8')
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line))
    expect(response.headers['content-type']).toMatch(/application\/x-ndjson/)
    expect(entries.map((entry) => entry.type)).toEqual(['header', 'record', 'summary'])
  })

  test('ships an nginx path proxy that strips the public prefix and disables buffering', () => {
    const config = fs.readFileSync(
      path.join(__dirname, '..', 'config', 'nginx-audit-query.conf.example'),
      'utf8'
    )
    expect(config).toContain('location ^~ /audit-query/')
    expect(config).toContain('proxy_pass http://127.0.0.1:3100/;')
    expect(config).toContain('proxy_buffering off;')
    expect(config).toContain('proxy_read_timeout 1800s;')
  })
})
