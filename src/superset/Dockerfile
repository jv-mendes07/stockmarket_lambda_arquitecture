FROM apache/superset:4.0.1

USER root

RUN apt-get update && \
    apt-get install -y supervisor && \
    mkdir -p /var/log/supervisor

# FIX de cryptography compatível com Superset
RUN pip install --upgrade "cryptography>=42.0.4,<43.0.0"

# Snowflake sem atualizar cryptography
RUN pip install snowflake-sqlalchemy==1.7.7
RUN pip install snowflake-connector-python==3.8.0
RUN pip install sortedcontainers


COPY entrypoint.sh /entrypoint.sh
COPY supervisord.conf /etc/supervisor/conf.d/superset.conf
COPY superset_config.py /app/pythonpath/superset_config.py

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

USER root
