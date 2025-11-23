FROM apache/spark:3.4.2

USER root

# Create spark user
RUN useradd -m -u 1001 sparkuser

# Prepare home
RUN mkdir -p /home/spark && \
    chown -R 1001:1001 /home/spark

ENV HOME=/home/spark

# Install python
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

COPY jars/*.jar /opt/spark/jars/
RUN chown -R 1001:1001 /opt/spark/jars
RUN chown -R 1001:1001 /opt/spark

# Switch to non-root user
USER sparkuser
