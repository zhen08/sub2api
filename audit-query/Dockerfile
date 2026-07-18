ARG NODE_IMAGE=node:24-alpine

FROM ${NODE_IMAGE} AS dependencies
WORKDIR /app
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --omit=dev

FROM ${NODE_IMAGE}
LABEL org.opencontainers.image.title="Sub2API Audit Query"
LABEL org.opencontainers.image.description="Read-only query service for Sub2API call-audit data"

RUN addgroup -g 1000 audit-query && \
    adduser -u 1000 -G audit-query -s /sbin/nologin -D audit-query

WORKDIR /app
ENV NODE_ENV=production \
    AUDIT_QUERY_HOST=0.0.0.0 \
    AUDIT_QUERY_PORT=3100

COPY --from=dependencies --chown=audit-query:audit-query /app/node_modules ./node_modules
COPY --chown=audit-query:audit-query package.json ./
COPY --chown=audit-query:audit-query src ./src

USER audit-query
EXPOSE 3100
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD wget -q -T 5 -O /dev/null http://127.0.0.1:3100/readyz || exit 1

CMD ["node", "src/auditQueryApp.js"]
