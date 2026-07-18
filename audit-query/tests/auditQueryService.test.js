const { AuditQueryError } = require('../src/services/audit/auditQueryErrors')
const {
  AuditQueryService,
  decodeCursor,
  encodeCursor
} = require('../src/services/audit/auditQueryService')

function createConfig(overrides = {}) {
  return {
    defaultListRangeMs: 24 * 60 * 60 * 1000,
    maxQueryRangeMs: 7 * 24 * 60 * 60 * 1000,
    defaultListLimit: 50,
    maxListLimit: 200,
    defaultExportLimit: 1000,
    maxExportRecords: 10000,
    exportBatchSize: 100,
    s3Concurrency: 2,
    ...overrides
  }
}

function createCall(requestId, createdAt) {
  return {
    requestId,
    createdAt,
    retentionUntil: '2026-12-31T00:00:00.000Z'
  }
}

describe('AuditQueryService', () => {
  test('uses a stable cursor and enforces the seven-day query range', async () => {
    const call = createCall('req_1', '2026-07-10T00:00:00.000Z')
    const repository = {
      listCalls: jest.fn().mockResolvedValue({ calls: [call], hasMore: true })
    }
    const service = new AuditQueryService({
      repository,
      objectStorage: {},
      configProvider: () => createConfig()
    })

    const result = await service.listCalls({
      from: '2026-07-09T00:00:00.000Z',
      to: '2026-07-10T00:00:00.000Z',
      userUsername: 'alice',
      limit: '1'
    })

    expect(result.nextCursor).toBeTruthy()
    expect(decodeCursor(result.nextCursor)).toEqual({
      requestId: 'req_1',
      createdAt: '2026-07-10T00:00:00.000Z'
    })
    expect(repository.listCalls).toHaveBeenCalledWith(
      expect.objectContaining({ userUsername: 'alice' }),
      1
    )
    await expect(
      service.listCalls({
        from: '2026-07-01T00:00:00.000Z',
        to: '2026-07-10T00:00:00.000Z'
      })
    ).rejects.toMatchObject({ code: 'query_range_too_large', statusCode: 400 })
  })

  test('streams records and continues when one artifact cannot be read', async () => {
    const call = createCall('req_1', '2026-07-10T00:00:00.000Z')
    const descriptors = [
      {
        id: '1',
        requestId: 'req_1',
        kind: 'client_request',
        sequence: 0
      },
      {
        id: '2',
        requestId: 'req_1',
        kind: 'response',
        sequence: 0
      }
    ]
    const repository = {
      listCalls: jest.fn().mockResolvedValue({ calls: [call], hasMore: false }),
      getArtifactsByRequestIds: jest.fn().mockResolvedValue(descriptors)
    }
    const objectStorage = {
      readArtifact: jest.fn(async (descriptor) => {
        if (descriptor.id === '2') {
          throw new AuditQueryError(404, 'artifact_not_found', 'missing')
        }
        return { body: { prompt: 'hello' } }
      })
    }
    const service = new AuditQueryService({
      repository,
      objectStorage,
      configProvider: () => createConfig()
    })

    const records = []
    for await (const record of service.exportRecords({
      from: '2026-07-09T00:00:00.000Z',
      to: '2026-07-10T00:00:00.000Z',
      limit: 10
    })) {
      records.push(record)
    }

    expect(records.map((record) => record.type)).toEqual(['header', 'record', 'summary'])
    expect(records[1].artifacts).toHaveLength(1)
    expect(records[1].artifactErrors).toEqual([
      expect.objectContaining({ artifactId: '2', code: 'artifact_not_found' })
    ])
    expect(records[2]).toEqual(
      expect.objectContaining({
        complete: true,
        exportedRecords: 1,
        artifactFailures: 1,
        truncated: false
      })
    )
  })

  test('requires explicit export dates and validates artifact kinds before streaming', () => {
    const service = new AuditQueryService({
      repository: {},
      objectStorage: {},
      configProvider: () => createConfig()
    })

    expect(() => service.prepareExport({})).toThrow(/from and to are required/)
    expect(() =>
      service.prepareExport({
        from: '2026-07-09T00:00:00.000Z',
        to: '2026-07-10T00:00:00.000Z',
        artifactKinds: ['unknown']
      })
    ).toThrow(/unsupported kind/)
    expect(() =>
      service.prepareExport({
        from: '2026-07-09T00:00:00.000Z',
        to: '2026-07-10T00:00:00.000Z',
        filters: 'userUsername=alice'
      })
    ).toThrow(/filters must be an object/)
  })

  test('encodes cursors without exposing mutable query state', () => {
    const encoded = encodeCursor(createCall('req_cursor', '2026-07-10T01:02:03.000Z'))
    expect(encoded).not.toContain('req_cursor')
    expect(decodeCursor(encoded)).toEqual({
      requestId: 'req_cursor',
      createdAt: '2026-07-10T01:02:03.000Z'
    })
  })
})
