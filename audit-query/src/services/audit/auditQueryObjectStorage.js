const crypto = require('crypto')
const zlib = require('zlib')
const { promisify } = require('util')
const { GetObjectCommand, S3Client } = require('@aws-sdk/client-s3')
const { getAuditQueryConfig } = require('./auditQueryConfig')
const { AuditQueryError, isAbortError } = require('./auditQueryErrors')

const gunzip = promisify(zlib.gunzip)

async function readBodyWithLimit(body, maxBytes, signal) {
  if (!body) {
    return Buffer.alloc(0)
  }
  const chunks = []
  let size = 0

  const append = (chunk) => {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
    size += buffer.length
    if (size > maxBytes) {
      throw new AuditQueryError(422, 'artifact_too_large', 'Compressed artifact exceeds limit')
    }
    chunks.push(buffer)
  }

  if (Buffer.isBuffer(body) || body instanceof Uint8Array) {
    append(body)
  } else if (typeof body[Symbol.asyncIterator] === 'function') {
    for await (const chunk of body) {
      if (signal?.aborted) {
        const error = new Error('Artifact read aborted')
        error.name = 'AbortError'
        throw error
      }
      append(chunk)
    }
  } else if (typeof body.transformToByteArray === 'function') {
    append(await body.transformToByteArray())
  } else {
    throw new AuditQueryError(502, 'artifact_body_invalid', 'Artifact body is not readable')
  }

  return Buffer.concat(chunks, size)
}

class AuditQueryObjectStorage {
  constructor(options = {}) {
    this.configProvider = options.configProvider || getAuditQueryConfig
    this.client = options.client || null
  }

  getClient(config) {
    if (this.client) {
      return this.client
    }
    this.client = new S3Client({
      endpoint: config.s3Endpoint,
      region: config.s3Region,
      forcePathStyle: true,
      credentials: {
        accessKeyId: config.s3AccessKey,
        secretAccessKey: config.s3SecretKey
      }
    })
    return this.client
  }

  validateLocation(descriptor, config) {
    const prefix = `${String(config.objectKeyPrefix || '').replace(/^\/+|\/+$/g, '')}/`
    if (
      descriptor.bucket !== config.s3Bucket ||
      !String(descriptor.objectKey || '').startsWith(prefix)
    ) {
      throw new AuditQueryError(
        403,
        'artifact_location_not_allowed',
        'Artifact is outside the configured audit storage scope'
      )
    }
  }

  async readArtifact(descriptor, options = {}) {
    const config = this.configProvider()
    this.validateLocation(descriptor, config)

    const recordedBytes = Number(descriptor.bytes)
    if (Number.isFinite(recordedBytes) && recordedBytes > config.maxArtifactCompressedBytes) {
      throw new AuditQueryError(422, 'artifact_too_large', 'Compressed artifact exceeds limit')
    }

    let response
    try {
      response = await this.getClient(config).send(
        new GetObjectCommand({
          Bucket: descriptor.bucket,
          Key: descriptor.objectKey
        }),
        options.signal ? { abortSignal: options.signal } : undefined
      )
    } catch (error) {
      if (isAbortError(error)) {
        throw error
      }
      if (error?.name === 'NoSuchKey' || error?.$metadata?.httpStatusCode === 404) {
        throw new AuditQueryError(404, 'artifact_not_found', 'Artifact object was not found')
      }
      throw new AuditQueryError(
        502,
        'artifact_storage_unavailable',
        'Artifact storage is unavailable'
      )
    }

    const compressed = await readBodyWithLimit(
      response.Body,
      config.maxArtifactCompressedBytes,
      options.signal
    )
    const actualSha256 = crypto.createHash('sha256').update(compressed).digest('hex')
    const expectedSha256 = String(descriptor.sha256 || '').toLowerCase()
    if (!/^[a-f0-9]{64}$/.test(expectedSha256) || actualSha256 !== expectedSha256) {
      throw new AuditQueryError(422, 'artifact_checksum_mismatch', 'Artifact checksum mismatch')
    }

    let raw
    try {
      raw = await gunzip(compressed, {
        maxOutputLength: config.maxArtifactUncompressedBytes
      })
    } catch (error) {
      const tooLarge =
        error?.code === 'ERR_BUFFER_TOO_LARGE' || /larger than/i.test(error?.message || '')
      throw new AuditQueryError(
        422,
        tooLarge ? 'artifact_too_large' : 'artifact_decode_failed',
        tooLarge ? 'Uncompressed artifact exceeds limit' : 'Artifact gzip data is invalid'
      )
    }

    try {
      return JSON.parse(raw.toString('utf8'))
    } catch (error) {
      throw new AuditQueryError(422, 'artifact_json_invalid', 'Artifact JSON is invalid')
    }
  }
}

module.exports = {
  AuditQueryObjectStorage,
  readBodyWithLimit
}
