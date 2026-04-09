FROM python:3.11-slim

LABEL maintainer="nginx302_proxy"
LABEL description="HTTP Reverse Proxy with 302 Redirect Support"

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY config.yaml.template ./config.yaml

RUN mkdir -p /app/log /app/cache

EXPOSE 8080

CMD ["python", "main.py", "-c", "config.yaml"]
