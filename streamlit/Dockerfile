FROM python:3.11-slim

WORKDIR /app

# Системные зависимости для psycopg2
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Python-зависимости
COPY streamlit/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Код приложения
COPY streamlit/app.py /app/app.py
COPY ml /app/ml

EXPOSE 8001

CMD ["streamlit", "run", "app.py", "--server.port=8001", "--server.address=0.0.0.0"]