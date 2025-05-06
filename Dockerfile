FROM docker.io/alpine:latest

# default sync-state to /tmp since it's writeable
ENV EXECUTESYNC_STATE_DIR=/var/run/execute-sync

# Install certificate store / libc (required by Golang)
# Create non-root user
RUN apk --no-cache add ca-certificates libc6-compat \
    && addgroup -S appgroup \
    && adduser -S appuser -G appgroup \
    && mkdir -p /var/run/execute-sync \
    && chown appuser:appgroup /var/run/execute-sync

WORKDIR /app

COPY execute-sync .

USER appuser

CMD ["./execute-sync","sync"]
