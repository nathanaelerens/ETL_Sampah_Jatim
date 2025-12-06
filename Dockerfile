FROM apache/airflow:3.1.3

USER root

# Install Chromium and ChromiumDriver (auto-matched by apt)
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install selenium
RUN pip install selenium openpyxl pandas
