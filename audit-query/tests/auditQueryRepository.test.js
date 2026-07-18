const { AuditQueryRepository } = require('../src/services/audit/auditQueryRepository')

describe('AuditQueryRepository', () => {
  test('builds a parameterized, retained, cursor-paginated call query', async () => {
    const pool = {
      query: jest.fn().mockResolvedValue({
        rows: [
          {
            request_id: 'req_1',
            created_at: new Date('2026-07-10T00:00:00.000Z'),
            retention_until: new Date('2026-12-31T00:00:00.000Z'),
            input_tokens: '9007199254740993',
            cost: '1.2300000000',
            meta: {}
          }
        ]
      })
    }
    const repository = new AuditQueryRepository({ pool })

    const result = await repository.listCalls(
      {
        from: '2026-07-09T00:00:00.000Z',
        to: '2026-07-10T23:59:59.000Z',
        userUsername: 'alice',
        cursor: {
          createdAt: '2026-07-10T01:00:00.000Z',
          requestId: 'req_cursor'
        }
      },
      50
    )

    const [sql, params] = pool.query.mock.calls[0]
    expect(sql).toContain('c.retention_until > now()')
    expect(sql).toContain('(c.created_at, c.request_id) <')
    expect(sql).not.toContain('alice')
    expect(params).toContain('alice')
    expect(params.at(-1)).toBe(51)
    expect(result.calls[0].inputTokens).toBe('9007199254740993')
    expect(result.calls[0].cost).toBe('1.2300000000')
  })

  test('requires both tables and the artifact sequence column for readiness', async () => {
    const pool = {
      query: jest.fn().mockResolvedValue({
        rows: [{ calls_ready: true, artifacts_ready: true, sequence_ready: false }]
      })
    }
    const repository = new AuditQueryRepository({ pool })

    await expect(repository.checkReady()).rejects.toMatchObject({
      code: 'audit_query_schema_not_ready'
    })
  })
})
