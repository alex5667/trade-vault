FROM python:3.11-slim
WORKDIR /app
COPY regime-storage/requirements.txt .
RUN pip install --default-timeout=100 --no-cache-dir -r requirements.txt
COPY regime-storage/regime_storage.py /app/regime_storage.py
ENV PYTHONUNBUFFERED=1
CMD ["python", "regime_storage.py"]
