FROM apache/airflow:2.10.2

USER root

# 1. 安裝系統依賴 (SQL Server 驅動 & 瀏覽器)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    gnupg2 \
    unixodbc-dev \
    chromium \
    chromium-driver \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# 2. 一次裝好所有 Python 套件
RUN pip install --no-cache-dir \
    selenium \
    pyodbc \
    google-generativeai \
    psutil \
    python-dotenv \
    requests