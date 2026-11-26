FROM spark:3.5.9-python3

USER root

COPY history-spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY history-entrypoint.sh /opt/app/history-entrypoint.sh

RUN chmod 755 /opt/app/history-entrypoint.sh \
    && mkdir -p /opt/spark-events \
    && chown -R spark:spark /opt/spark-events /opt/app

USER spark

ENV SPARK_HOME=/opt/spark \
    PATH="/opt/spark/bin:${PATH}"

EXPOSE 18080
ENTRYPOINT ["/opt/app/history-entrypoint.sh"]
