FROM apache/airflow:2.7.1

USER root
# Cài đặt gcc để compile một số thư viện python nếu cần
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Copy file requirements và cài đặt
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt