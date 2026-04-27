FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY signal-generator/requirements.txt .
RUN pip install --default-timeout=100 --no-cache-dir -r requirements.txt

# Copy application
COPY signal-generator/signal_generator.py .
COPY signal-generator/xauusd_signal_formatter.py .

# Run as non-root
RUN useradd -m -u 1000 scanner && chown -R scanner:scanner /app
USER scanner

CMD ["python", "-u", "signal_generator.py"]

