const crypto = require('crypto')
const zlib = require('zlib')
const { AuditQueryObjectStorage } = require('../src/services/audit/auditQueryObjectStorage')

function createConfig(overrides = {}) {
  return {
    s3Endpoint: 'https://s3.example.com',
    s3Bucket: 'ai-call-audit',
    s3AccessKey: 'reader',
    s3SecretKey: 'secret',
    s3Region: 'us-east-1',
    objectKeyPrefix: 'ai-call-audit',
    maxArtifactCompressedBytes: 1024 * 1024,
    maxArtifactUncompressedBytes: 1024 * 1024,
    ...overrides
  }
}

function createDescriptor(compressed, overrides = {}) {
  return {
    id: '1',
    requestId: 'req_1',
    kind: 'response',
    sequence: 0,
    bucket: 'ai-call-audit',
    objectKey: 'ai-call-audit/dt=2026-07-10/request_id=req_1/response.json.gz',
    bytes: String(compressed.length),
    sha256: crypto.createHash('sha256').update(compressed).digest('hex'),
    ...overrides
  }
}

describe('AuditQueryObjectStorage', () => {
  test('reads, verifies, decompresses, and parses an audit artifact', async () => {
    const payload = { kind: 'response', body: { answer: 'hello' } }
    const compressed = zlib.gzipSync(Buffer.from(JSON.stringify(payload)))
    const client = { send: jest.fn().mockResolvedValue({ Body: compressed }) }
    const storage = new AuditQueryObjectStorage({
      client,
      configProvider: () => createConfig()
    })

    await expect(storage.readArtifact(createDescriptor(compressed))).resolves.toEqual(payload)
    expect(client.send).toHaveBeenCalledTimes(1)
  })

  test('rejects checksum mismatches and objects outside the configured prefix', async () => {
    const compressed = zlib.gzipSync(Buffer.from('{}'))
    const client = { send: jest.fn().mockResolvedValue({ Body: compressed }) }
    const storage = new AuditQueryObjectStorage({
      client,
      configProvider: () => createConfig()
    })

    await expect(
      storage.readArtifact(createDescriptor(compressed, { sha256: 'a'.repeat(64) }))
    ).rejects.toMatchObject({ code: 'artifact_checksum_mismatch', statusCode: 422 })
    await expect(
      storage.readArtifact(createDescriptor(compressed, { objectKey: 'other/response.json.gz' }))
    ).rejects.toMatchObject({ code: 'artifact_location_not_allowed', statusCode: 403 })
  })

  test('maps missing S3 objects to a stable not-found error', async () => {
    const compressed = zlib.gzipSync(Buffer.from('{}'))
    const error = new Error('missing')
    error.name = 'NoSuchKey'
    const storage = new AuditQueryObjectStorage({
      client: { send: jest.fn().mockRejectedValue(error) },
      configProvider: () => createConfig()
    })

    await expect(storage.readArtifact(createDescriptor(compressed))).rejects.toMatchObject({
      code: 'artifact_not_found',
      statusCode: 404
    })
  })
})
