# =============================================================================
# Dockerfile for Kafka Consumer
# =============================================================================
# 
# WHAT IS A DOCKERFILE?
#   A Dockerfile is a text recipe that tells Docker how to build an "image" —
#   a self-contained package with your code, its dependencies, and a minimal
#   operating system. Think of it like a blueprint for a virtual computer
#   that is purpose-built to run your consumer script.
#
# WHY WE USE DOCKER FOR THIS PROJECT:
#   1. Reproducibility: Anyone can run this consumer on any machine without
#      installing Python, confluent-kafka, pymongo, etc. manually.
#   2. Scaling: We can spin up multiple consumer instances with one command
#      (docker-compose up --scale consumer=3). Each instance joins the same
#      Kafka consumer group and processes different partitions.
#   3. Isolation: The consumer runs in its own sandbox. If it crashes, it
#      does not affect the producer, API, or your host machine.
#
# HOW TO BUILD AND RUN (see README for full instructions):
#   docker build -t logistics-consumer .
#   docker run --env-file .env logistics-consumer
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1: Base Image
# ---------------------------------------------------------------------------
# We use python:3.11-slim as the base. "slim" means it is a stripped-down
# version of Debian with just enough to run Python. This keeps our image
# small (~150MB vs ~900MB for the full image).
#
# TRADE-OFF: slim images lack some system libraries. If you need to compile
# C extensions (like confluent-kafka's librdkafka), you may need build tools.
# We handle this below.
FROM python:3.11-slim

# ---------------------------------------------------------------------------
# Stage 2: Install system-level dependencies
# ---------------------------------------------------------------------------
# confluent-kafka is a wrapper around librdkafka (a C library). On slim images,
# we need build tools to compile it. We install them, then clean up to keep
# the image small.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Stage 3: Set up working directory
# ---------------------------------------------------------------------------
# /app inside the container is where our code lives. This is NOT a directory
# on your laptop — it exists only inside the Docker container.
WORKDIR /app

# ---------------------------------------------------------------------------
# Stage 4: Install Python dependencies
# ---------------------------------------------------------------------------
# We copy requirements.txt FIRST (before the rest of the code) because Docker
# caches each step. If requirements.txt has not changed, Docker reuses the
# cached layer and skips pip install — making rebuilds much faster.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# Stage 5: Copy application code
# ---------------------------------------------------------------------------
# Now we copy everything else. If you change code but not requirements.txt,
# Docker only re-runs from this step onward (fast rebuild).
COPY config/ config/
COPY consumer/ consumer/
COPY validation/ validation/

# ---------------------------------------------------------------------------
# Stage 6: Define the default command
# ---------------------------------------------------------------------------
# When someone runs this container, it automatically starts the consumer.
# The --batch-size flag can be overridden at runtime:
#   docker run logistics-consumer python -m consumer.kafka_consumer --batch-size 200
CMD ["python", "-m", "consumer.kafka_consumer"]
