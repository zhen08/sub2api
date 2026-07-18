function serialize(level, message, metadata = {}) {
  return `${JSON.stringify({
    timestamp: new Date().toISOString(),
    level,
    message,
    ...metadata
  })}\n`
}

const auditQueryLogger = {
  info(message, metadata) {
    process.stdout.write(serialize('info', message, metadata))
  },
  warn(message, metadata) {
    process.stderr.write(serialize('warn', message, metadata))
  },
  error(message, metadata) {
    process.stderr.write(serialize('error', message, metadata))
  }
}

module.exports = auditQueryLogger
