FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY telegram-worker/requirements.txt /app/requirements.txt

# Install runtime deps from requirements.txt
RUN pip install --upgrade pip && \
    pip install --default-timeout=100 -r requirements.txt

# Copy the common module (now accessible from root context)
COPY common/ ./common/

# App files from telegram-worker directory
COPY telegram-worker/main.py /app/main.py
COPY telegram-worker/multithreaded_worker.py /app/multithreaded_worker.py
COPY telegram-worker/channel_poller.py /app/channel_poller.py
COPY telegram-worker/improved_notifier.py /app/improved_notifier.py
COPY telegram-worker/notifier.py /app/notifier.py
COPY telegram-worker/app /app/app
COPY telegram-worker/notify_worker.py /app/notify_worker.py

# Session storage (mount as volume)
VOLUME ["/app/sessions"]

CMD ["python", "multithreaded_worker.py"]
