FROM python:3.11-slim

WORKDIR /app

# Устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код бота
COPY bot_tg_prorok.py .

# Создаем папку для данных
RUN mkdir -p /app/data

# Запускаем бота
CMD ["python", "bot_tg_prorok.py"]