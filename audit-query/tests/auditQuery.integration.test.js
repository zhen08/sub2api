const crypto = require('crypto')
const zlib = require('zlib')
const { Pool } = require('pg')
const { CreateBucketCommand, PutObjectCommand, S3Client } = require('@aws-sdk/client-s3')
const request = require('supertest')
const { createAuditQueryApp } = require('../src/auditQueryApp')
const { AuditQueryObjectStorage } = require('../src/services/audit/auditQueryObjectStorage')
const { AuditQueryRepository } = require('../src/services/audit/auditQueryRepository')

const runIntegration = process.env.RUN_AUDIT_QUERY_INTEGRATION === 'true'
const describeIntegration = runIntegration ? describe : describe.skip

const RAW_TOKEN = 'audit-query-integration-token-that-is-long-enough'

function bufferParser(res, callback) {
  const chunks = []
  res.on('data', (chunk) => chunks.push(Buffer.from(chunk)))
  res.on('end', () => callback(null, Buffer.concat(chunks)))
}

describeIntegration('audit query PostgreSQL and S3 integration', () => {
  const postgresUrl = process.env.AUDIT_QUERY_INTEGRATION_POSTGRES_URL
  const s3Endpoint = process.env.AUDIT_QUERY_INTEGRATION_S3_ENDPOINT
  const s3AccessKey = process.env.AUDIT_QUERY_INTEGRATION_S3_ACCESS_KEY
  const s3SecretKey = process.env.AUDIT_QUERY_INTEGRATION_S3_SECRET_KEY
  const bucket = 'ai-call-audit-test'
  const objectKey = 'ai-call-audit/dt=2026-07-10/request_id=req_integration/response.json.gz'
  const createdAt = new Date()
  const payload = { kind: 'response', requestId: 'req_integration', body: { answer: 'hello' } }
  const compressed = zlib.gzipSync(Buffer.from(JSON.stringify(payload)))
  const sha256 = crypto.createHash('sha256').update(compressed).digest('hex')
  let pool
  let app

  beforeAll(async () => {
    pool = new Pool({ connectionString: postgresUrl })
    await pool.query(`
      CREATE TABLE audit_calls (
        request_id TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        request_started_at TIMESTAMPTZ NULL,
        endpoint TEXT NULL,
        method TEXT NULL,
        protocol TEXT NULL,
        api_key_id TEXT NULL,
        api_key_name TEXT NULL,
        user_id TEXT NULL,
        user_username TEXT NULL,
        account_id TEXT NULL,
        account_type TEXT NULL,
        model TEXT NULL,
        status TEXT NULL,
        status_code INTEGER NULL,
        stream BOOLEAN NOT NULL DEFAULT FALSE,
        input_tokens BIGINT NOT NULL DEFAULT 0,
        output_tokens BIGINT NOT NULL DEFAULT 0,
        cache_read_tokens BIGINT NOT NULL DEFAULT 0,
        cache_create_tokens BIGINT NOT NULL DEFAULT 0,
        total_tokens BIGINT NOT NULL DEFAULT 0,
        cost NUMERIC(20, 10) NOT NULL DEFAULT 0,
        real_cost NUMERIC(20, 10) NOT NULL DEFAULT 0,
        retention_until TIMESTAMPTZ NOT NULL,
        capture_status TEXT NOT NULL,
        error TEXT NULL,
        meta JSONB NOT NULL DEFAULT '{}'::jsonb,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (request_id, created_at)
      );
      CREATE TABLE audit_artifacts (
        id BIGSERIAL PRIMARY KEY,
        call_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        sequence INTEGER NOT NULL DEFAULT 0,
        bucket TEXT NOT NULL,
        object_key TEXT NOT NULL,
        bytes BIGINT NOT NULL,
        sha256 TEXT NOT NULL,
        content_type TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `)
    await pool.query(
      `
        INSERT INTO audit_calls (
          request_id, created_at, request_started_at, endpoint, method, protocol,
          api_key_id, api_key_name, user_id, user_username, model, status, status_code,
          retention_until, capture_status
        ) VALUES ($1, $2, $2, '/api/v1/messages', 'POST', 'anthropic',
                  'key_1', 'Shared Key', 'user_1', 'alice', 'claude-test', 'ok', 200,
                  $3, 'stored')
      `,
      ['req_integration', createdAt, new Date(createdAt.getTime() + 24 * 60 * 60 * 1000)]
    )
    await pool.query(
      `
        INSERT INTO audit_artifacts (
          call_id, request_id, kind, sequence, bucket, object_key, bytes, sha256, content_type
        ) VALUES ($1, $1, 'response', 0, $2, $3, $4, $5, 'application/json')
      `,
      ['req_integration', bucket, objectKey, compressed.length, sha256]
    )

    const s3Client = new S3Client({
      endpoint: s3Endpoint,
      region: 'us-east-1',
      forcePathStyle: true,
      credentials: { accessKeyId: s3AccessKey, secretAccessKey: s3SecretKey }
    })
    await s3Client.send(new CreateBucketCommand({ Bucket: bucket }))
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: objectKey,
        Body: compressed,
        ContentType: 'application/json',
        ContentEncoding: 'gzip'
      })
    )

    const config = {
      host: '127.0.0.1',
      port: 3100,
      tokenSha256: crypto.createHash('sha256').update(RAW_TOKEN).digest('hex'),
      postgresUrl,
      postgresPoolMax: 2,
      postgresStatementTimeoutMs: 30000,
      s3Endpoint,
      s3Bucket: bucket,
      s3AccessKey,
      s3SecretKey,
      s3Region: 'us-east-1',
      objectKeyPrefix: 'ai-call-audit',
      defaultListRangeMs: 24 * 60 * 60 * 1000,
      maxQueryRangeMs: 7 * 24 * 60 * 60 * 1000,
      defaultListLimit: 50,
      maxListLimit: 200,
      defaultExportLimit: 1000,
      maxExportRecords: 10000,
      exportBatchSize: 100,
      maxConcurrentExports: 2,
      s3Concurrency: 2,
      exportTimeoutMs: 30000,
      maxArtifactCompressedBytes: 1024 * 1024,
      maxArtifactUncompressedBytes: 1024 * 1024
    }
    const repository = new AuditQueryRepository({ pool, configProvider: () => config })
    const objectStorage = new AuditQueryObjectStorage({ configProvider: () => config })
    ;({ app } = createAuditQueryApp({
      configProvider: () => config,
      repository,
      objectStorage,
      logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() }
    }))
  })

  afterAll(async () => {
    if (pool) {
      await pool.query('DROP TABLE IF EXISTS audit_artifacts; DROP TABLE IF EXISTS audit_calls;')
      await pool.end()
    }
  })

  test('queries metadata, reads the response artifact, and streams an export', async () => {
    const authorization = `Bearer ${RAW_TOKEN}`
    await request(app).get('/readyz').expect(200)

    const list = await request(app)
      .get('/v1/audit/calls')
      .set('Authorization', authorization)
      .query({
        from: new Date(createdAt.getTime() - 60000).toISOString(),
        to: new Date(createdAt.getTime() + 60000).toISOString(),
        userUsername: 'alice'
      })
      .expect(200)
    expect(list.body.data.calls).toHaveLength(1)

    const detail = await request(app)
      .get('/v1/audit/calls/req_integration')
      .set('Authorization', authorization)
      .expect(200)
    expect(detail.body.data.artifacts).toHaveLength(1)

    const artifact = await request(app)
      .get(`/v1/audit/artifacts/${detail.body.data.artifacts[0].id}`)
      .set('Authorization', authorization)
      .expect(200)
    expect(artifact.body.data.payload).toEqual(payload)

    const exported = await request(app)
      .post('/v1/audit/exports/stream')
      .set('Authorization', authorization)
      .send({
        from: new Date(createdAt.getTime() - 60000).toISOString(),
        to: new Date(createdAt.getTime() + 60000).toISOString(),
        artifactKinds: ['response']
      })
      .buffer(true)
      .parse(bufferParser)
      .expect(200)
    const raw = exported.body
    const decoded = raw[0] === 0x1f && raw[1] === 0x8b ? zlib.gunzipSync(raw) : raw
    const records = decoded
      .toString('utf8')
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line))
    expect(records.map((record) => record.type)).toEqual(['header', 'record', 'summary'])
    expect(records[1].artifacts[0].payload).toEqual(payload)
  })
})
