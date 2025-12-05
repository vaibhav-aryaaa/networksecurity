FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update -y && apt install awscli -y

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "app.py"]