FROM apache/airflow:2.8.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libaio1 \
        wget \
        unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# dbt logs 폴더 미리 생성 + 권한 부여
RUN mkdir -p /opt/airflow/dbt/apt_mart/logs \
    && chown -R airflow:root /opt/airflow/dbt \
    && chmod -R 775 /opt/airflow/dbt

USER airflow

RUN pip install --upgrade pip

RUN pip install --no-cache-dir \
    requests pandas oracledb python-dotenv python-dateutil pendulum pytz gspread gspread-dataframe \
    dbt-core==1.7.0 \
    dbt-oracle==1.7.0
