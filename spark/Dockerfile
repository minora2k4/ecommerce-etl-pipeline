FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

USER root

# Install necessary packages
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy local JARs instead of downloading
COPY jars/*.jar /opt/spark/jars/

# Create checkpoint directory with proper permissions
RUN mkdir -p /opt/spark/work-dir/checkpoint && \
    chmod -R 777 /opt/spark/work-dir

USER spark

WORKDIR /opt/spark/work-dir