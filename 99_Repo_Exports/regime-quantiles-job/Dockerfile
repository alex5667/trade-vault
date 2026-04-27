FROM python:3.11-slim
WORKDIR /app
COPY regime-quantiles-job/requirements.txt .
RUN pip install --default-timeout=100 --no-cache-dir -r requirements.txt
COPY regime-quantiles-job/quantiles_job.py /app/quantiles_job.py
ENV PYTHONUNBUFFERED=1
CMD ["python", "quantiles_job.py"]

