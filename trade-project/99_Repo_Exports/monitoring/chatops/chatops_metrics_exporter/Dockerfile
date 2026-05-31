FROM python:3.11-slim

WORKDIR /app

COPY monitoring/chatops/chatops_metrics_exporter/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY monitoring/chatops/chatops_metrics_exporter/exporter.py /app/exporter.py

ENV PYTHONUNBUFFERED=1
ENV CHATOPS_METRICS_EXPORTER_PORT=9816

CMD ["python", "/app/exporter.py"]
