FROM python:3.11-slim

WORKDIR /app

COPY monitoring/chatops/telegram_task_bot/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY monitoring/chatops/telegram_task_bot/bot.py /app/bot.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "/app/bot.py"]
