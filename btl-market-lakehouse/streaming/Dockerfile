FROM apache/spark:3.5.1

USER root
WORKDIR /opt/app
ENV PYTHONPATH=/opt/app

COPY common /opt/app/common
COPY streaming /opt/app/streaming
COPY contracts /opt/app/contracts

USER 185
ENTRYPOINT ["/opt/spark/bin/spark-submit"]
CMD ["--master", "local[*]", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "/opt/app/streaming/job.py"]
