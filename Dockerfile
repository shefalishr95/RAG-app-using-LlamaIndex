FROM apache/airflow:2.9.3-python3.11

COPY requirements.txt /requirements.txt

# Upgrade pip and install dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

USER airflow