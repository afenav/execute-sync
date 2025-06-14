FROM docker.io/alpine:latest

ENV EXECUTESYNC_STATE_DIR=/var/run/execute-sync

RUN apk --no-cache add ca-certificates libc6-compat \
    && addgroup -g 6001 -S appgroup \
    && adduser -u 6001 -S appuser -G appgroup \
    && mkdir -p /var/run/execute-sync \
    && chown appuser:appgroup /var/run/execute-sync

WORKDIR /app

COPY execute-sync .

USER appuser

ENTRYPOINT ["/app/execute-sync"]
CMD ["sync"]
