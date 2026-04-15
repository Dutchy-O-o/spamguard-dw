FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential libfreetype6-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# DB ve model volume olarak mount edilir — imaj icine kopyalanmaz
ENV PYTHONUNBUFFERED=1 \
    FLASK_APP=webapp.app

EXPOSE 5000

CMD ["python", "webapp/app.py"]
