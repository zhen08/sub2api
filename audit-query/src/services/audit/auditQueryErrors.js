class AuditQueryError extends Error {
  constructor(statusCode, code, message, details = undefined) {
    super(message)
    this.name = 'AuditQueryError'
    this.statusCode = statusCode
    this.code = code
    this.details = details
  }
}

function isAbortError(error) {
  return error?.name === 'AbortError' || error?.code === 'ABORT_ERR'
}

module.exports = {
  AuditQueryError,
  isAbortError
}
