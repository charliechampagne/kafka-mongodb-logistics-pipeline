"""
config/config.py
=================
Centralized configuration module for the Kafka-MongoDB Logistics Pipeline.

WHY THIS EXISTS:
    Instead of scattering connection strings and credentials across multiple files,
    we centralize all configuration here. This follows the 12-factor app methodology
    where config is loaded from environment variables, making the app portable across
    dev, staging, and production environments without code changes.

ARCHITECTURAL DECISION:
    We use python-dotenv to load a .env file locally, but in Docker containers,
    environment variables are injected directly via docker-compose.yml or Kubernetes
    secrets. This dual approach means the same code works both locally and in containers.

SECURITY NOTE:
    No default values are provided for sensitive credentials. If a required environment
    variable is missing, the application raises an explicit error at startup rather than
    silently falling back to a hardcoded value. This prevents accidental credential
    exposure in version control.
"""
import os
from dotenv import load_dotenv

# Load .env file if it exists (local development)
# In Docker, env vars are injected directly — load_dotenv() is a no-op
load_dotenv()


def _require(var_name: str) -> str:
    """
    Fetch a required environment variable.
    Raises a clear error at startup if it is missing,
    rather than failing silently or using unsafe defaults.
    """
    value = os.getenv(var_name)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{var_name}' is not set. "
            f"Check your .env file or container environment configuration."
        )
    return value


# =============================================================================
# Kafka Configuration
# =============================================================================
KAFKA_CONFIG = {
    'bootstrap.servers': _require('KAFKA_BOOTSTRAP_SERVERS'),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': _require('KAFKA_SASL_USERNAME'),
    'sasl.password': _require('KAFKA_SASL_PASSWORD'),
}

KAFKA_TOPIC = _require('KAFKA_TOPIC')

# =============================================================================
# Schema Registry Configuration
# =============================================================================
SCHEMA_REGISTRY_CONFIG = {
    'url': _require('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': '{}:{}'.format(
        _require('SCHEMA_REGISTRY_API_KEY'),
        _require('SCHEMA_REGISTRY_API_SECRET')
    )
}

SCHEMA_SUBJECT = _require('SCHEMA_SUBJECT')

# =============================================================================
# MongoDB Configuration
# =============================================================================
MONGO_CONNECTION_STRING = _require('MONGO_CONNECTION_STRING')
MONGO_DATABASE         = _require('MONGO_DATABASE')
MONGO_COLLECTION       = _require('MONGO_COLLECTION')

# =============================================================================
# Consumer Configuration
# =============================================================================
# These are non-sensitive so safe defaults are acceptable here
CONSUMER_GROUP_ID  = os.getenv('KAFKA_CONSUMER_GROUP_ID', 'logistics-consumer-group')
AUTO_OFFSET_RESET  = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')

# =============================================================================
# API Configuration
# =============================================================================
# These are non-sensitive so safe defaults are acceptable here
API_HOST = os.getenv('API_HOST', '0.0.0.0')
API_PORT = int(os.getenv('API_PORT', 8000))