FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Shanghai

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN grep -v -E "scrapy|drissionpage|lxml|beautifulsoup4" requirements.txt > requirements_minimal.txt || true

RUN pip install --no-cache-dir -r requirements.txt || pip install --no-cache-dir \
    fastapi>=0.109.0 \
    uvicorn[standard]>=0.27.0 \
    sqlalchemy>=2.0.0 \
    aiomysql>=0.2.0 \
    asyncmy>=0.2.0 \
    aiohttp>=3.8.0 \
    requests>=2.31.0 \
    python-dateutil>=2.8.0 \
    pytz>=2023.3 \
    redis>=5.0.0 \
    python-dotenv>=1.0.0 \
    pydantic>=2.0.0

COPY . .

RUN mkdir -p /app/logs

RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8005

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8005/ || exit 1

CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8005"]
