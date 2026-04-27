FROM python:3.11-slim
WORKDIR /app
COPY regime-worker/requirements.txt .
RUN pip install --default-timeout=100 --no-cache-dir -r requirements.txt
COPY regime-worker /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
CMD ["python", "main.py"]
