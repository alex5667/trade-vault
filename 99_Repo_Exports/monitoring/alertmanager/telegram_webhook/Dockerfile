FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application
COPY app.py /app/app.py

# Runtime defaults (can be overridden via compose environment)
ENV WEBHOOK_PORT=8081
ENV PYTHONUNBUFFERED=1

EXPOSE 8081

CMD ["python", "/app/app.py"]
