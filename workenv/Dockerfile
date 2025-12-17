# Use Python 3.11 slim as base (much better than Python 3.8)
FROM python:3.11-slim-bookworm

# Switch to root for installations
USER root

# Install Java (required for PySpark) and other utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    nano \
    sudo \
    curl \
    procps \
    wget && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Create non-root user with sudo privileges
RUN useradd -ms /bin/bash developer && \
    echo "developer ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Install Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && \
    pip install -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# Install Starship prompt (official method from starship.rs)
RUN curl -sS https://starship.rs/install.sh | sh -s -- --yes && \
    echo 'eval "$(starship init bash)"' >> /home/developer/.bashrc && \
    chown developer:developer /home/developer/.bashrc

# Create workspace with proper permissions
RUN mkdir -p /app/workspace && \
    chmod 777 /app/workspace

# Configure Spark environment variables
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV SPARK_LOCAL_IP=0.0.0.0
ENV SPARK_MASTER_HOST=localhost
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Jupyter configuration
ENV JUPYTER_RUNTIME_DIR=/tmp/.jupyter/runtime
ENV JUPYTER_DATA_DIR=/tmp/.jupyter/data
ENV JUPYTER_ENABLE_LAB=yes

# Set working directory
WORKDIR /app/workspace


# Switch to non-root user
USER developer

# Expose ports for Spark UI and Jupyter
EXPOSE 4040 8888

# Start Jupyter Lab
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--ServerApp.token=''", "--ServerApp.password=''", "--ServerApp.allow_origin='*'", "--ServerApp.allow_remote_access=True"]
