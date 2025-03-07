FROM bitnami/spark:latest

COPY spark-env.sh /opt/bitnami/spark/conf/

COPY app /app

WORKDIR /app

