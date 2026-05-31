FROM golang:1.22-alpine

WORKDIR /app
COPY . .
RUN chmod +x /app/start-worker.sh

RUN go mod tidy
RUN go build -o main ./cmd/worker

# Export environment variables from .env file
RUN if [ -f .env ]; then export $(cat .env | xargs); fi

# Create startup script
RUN echo '#!/bin/bash' > /app/startup.sh && \
    echo 'echo "⏳ Ожидание полной готовности Redis..."' >> /app/startup.sh && \
    echo '# Wait for Redis to be fully loaded and ready for write operations' >> /app/startup.sh && \
    echo 'timeout=120' >> /app/startup.sh && \
    echo 'elapsed=0' >> /app/startup.sh && \
    echo 'while [ $elapsed -lt $timeout ]; do' >> /app/startup.sh && \
    echo '  # Check if Redis is loading' >> /app/startup.sh && \
    echo '  loading_status=$(redis-cli -h redis-worker-1 -p 6379 INFO 2>/dev/null | grep -E "^loading:" | cut -d: -f2 | tr -d '\''\r\n'\'' || echo "1")' >> /app/startup.sh && \
    echo '  if [ "$loading_status" = "0" ]; then' >> /app/startup.sh && \
    echo '    # Redis is not loading, try a write operation to confirm it'\''s ready' >> /app/startup.sh && \
    echo '    if redis-cli -h redis-worker-1 -p 6379 XADD test:ready "*" ready true >/dev/null 2>&1; then' >> /app/startup.sh && \
    echo '      # Clean up the test stream entry' >> /app/startup.sh && \
    echo '      redis-cli -h redis-worker-1 -p 6379 DEL test:ready >/dev/null 2>&1 || true' >> /app/startup.sh && \
    echo '      echo "✅ Redis готов к работе"' >> /app/startup.sh && \
    echo '      break' >> /app/startup.sh && \
    echo '    fi' >> /app/startup.sh && \
    echo '  fi' >> /app/startup.sh && \
    echo '  echo "Redis еще загружается... (прошло $elapsed с из $timeout с)"' >> /app/startup.sh && \
    echo '  sleep 5' >> /app/startup.sh && \
    echo '  elapsed=$((elapsed + 5))' >> /app/startup.sh && \
    echo 'done' >> /app/startup.sh && \
    echo '' >> /app/startup.sh && \
    echo 'if [ $elapsed -ge $timeout ]; then' >> /app/startup.sh && \
    echo '  echo "❌ Redis не готов через $timeout секунд, но продолжаем..."' >> /app/startup.sh && \
    echo 'fi' >> /app/startup.sh && \
    echo '' >> /app/startup.sh && \
    echo 'echo "🚀 Запуск go-worker-1m..."' >> /app/startup.sh && \
    echo './main' >> /app/startup.sh

RUN chmod +x /app/startup.sh

CMD ["/app/startup.sh"]
