# 1. Берем официальный легкий образ Python 3.10
FROM python:3.10-slim

# 2. Настройки для Python (не создавать .pyc файлы, выводить логи сразу)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Устанавливаем рабочую папку внутри контейнера
WORKDIR /app

# 4. Сначала копируем только список зависимостей (для кэширования)
COPY requirements.txt .

# 5. Устанавливаем библиотеки
RUN pip install --no-cache-dir -r requirements.txt

# 6. Копируем весь остальной код проекта
COPY . .

# 7. Создаем папку для данных (на всякий случай)
RUN mkdir -p /app/data

# 8. Команда запуска
# --host 0.0.0.0 обязателен для Docker
# ВНИМАНИЕ: Мы убрали --reload, чтобы сервис был стабильным
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8010"]