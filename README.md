# Kafka-MongoDB Logistics Data Pipeline

A production-grade data engineering pipeline that integrates **Confluent Kafka** with **MongoDB Atlas** using **Python**, featuring Avro serialization, data validation, a REST API, and Docker-based consumer scaling.

## Architecture Overview

```
┌──────────────────┐     ┌──────────────────────────┐     ┌──────────────────┐
│                  │     │    Confluent Kafka         │     │                  │
│   CSV Data       │────▶│  Topic: delivery_trip_truck│────▶│  Kafka Consumer  │
│   (Producer)     │     │  Schema Registry (Avro)   │     │  + Validation    │
│                  │     │                            │     │                  │
└──────────────────┘     └──────────────────────────┘     └────────┬─────────┘
                                                                    │
                                                          ┌────────┴─────────┐
                                                          │                  │
                                                   ┌──────┴───┐      ┌──────┴──────┐
                                                   │ MongoDB   │      │ Dead-Letter  │
                                                   │ Main      │      │ Collection   │
                                                   │ Collection│      │ (rejected)   │
                                                   └──────┬───┘      └─────────────┘
                                                          │
                                                   ┌──────┴──────┐
                                                   │  FastAPI     │
                                                   │  REST API    │
                                                   │  (port 8000) │
                                                   └─────────────┘
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Message Broker | Confluent Kafka (Cloud) | Event streaming and decoupling |
| Serialization | Avro + Schema Registry | Schema evolution and type safety |
| Data Store | MongoDB Atlas | Flexible document storage for logistics data |
| Language | Python 3.11 | Core application logic |
| API Framework | FastAPI | REST endpoints with auto-docs |
| Containerization | Docker + Docker Compose | Reproducible deployments and consumer scaling |
| Version Control | Git + GitHub | Code versioning and collaboration |

## Project Structure

```
kafka-mongodb-logistics-pipeline/
├── .env.example          # Template for environment variables
├── .env                  # Actual config (NOT in git)
├── .gitignore            # Git ignore rules
├── Dockerfile            # Consumer container definition
├── Dockerfile.api        # API container definition
├── docker-compose.yml    # Multi-container orchestration
├── requirements.txt      # Python dependencies
├── config/
│   ├── __init__.py
│   └── config.py         # Centralized configuration (12-factor)
├── producer/
│   ├── __init__.py
│   └── kafka_producer.py # Reads CSV, serializes to Avro, publishes to Kafka
├── consumer/
│   ├── __init__.py
│   └── kafka_consumer.py # Consumes from Kafka, validates, writes to MongoDB
├── validation/
│   ├── __init__.py
│   └── validators.py     # Business rule validation (15 rules documented)
├── api/
│   ├── __init__.py
│   └── app.py            # FastAPI with 14 endpoints
├── data/
│   └── .gitkeep          # CSV goes here (not tracked by git)
└── docs/
    └── architecture.md   # Additional architecture notes
```

## Quick Start

### Prerequisites
- Python 3.9+
- Docker Desktop (see Docker Installation Guide below)
- Access to Confluent Kafka Cloud (credentials in .env)
- Access to MongoDB Atlas (connection string in .env)

### 1. Clone and Setup
```bash
git clone https://github.com/YOUR_USERNAME/kafka-mongodb-logistics-pipeline.git
cd kafka-mongodb-logistics-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt

# Copy and configure environment variables
cp .env.example .env
# Edit .env with your credentials
```

### 2. Place Data File
```bash
# Copy your CSV to the data directory
cp /path/to/delivery_trip_truck_data.csv data/
```

### 3. Run the Producer
```bash
python -m producer.kafka_producer --csv-path data/delivery_trip_truck_data.csv --delay 0.1
```

### 4. Run the Consumer
```bash
# Single instance
python -m consumer.kafka_consumer --batch-size 100
```

### 5. Run the API
```bash
uvicorn api.app:app --host 0.0.0.0 --port 8000 --reload
# Visit http://localhost:8000/docs for Swagger UI
```

### 6. Run with Docker (Scaled)
```bash
# Build images
docker-compose build

# Start with 3 consumer instances
docker-compose up --scale consumer=3

# View logs
docker-compose logs -f consumer
```

---

## Docker Installation Guide (Step-by-Step)

### What is Docker?
Docker packages your application and all its dependencies into a "container" — a lightweight, standalone unit that runs the same way on any machine. Think of it as a shipping container for software.

### Install Docker Desktop

**Windows:**
1. Go to https://www.docker.com/products/docker-desktop/
2. Download "Docker Desktop for Windows"
3. Run the installer (requires admin rights)
4. During install, ensure "Use WSL 2 instead of Hyper-V" is checked
5. Restart your computer when prompted
6. Open Docker Desktop — you should see the Docker whale icon in your system tray
7. Open a terminal and verify: `docker --version`

**Mac:**
1. Go to https://www.docker.com/products/docker-desktop/
2. Download for your chip type (Intel or Apple Silicon)
3. Open the .dmg file and drag Docker to Applications
4. Launch Docker Desktop from Applications
5. Verify: `docker --version`

**Linux (Ubuntu/Debian):**
```bash
# Update package index
sudo apt-get update

# Install prerequisites
sudo apt-get install ca-certificates curl gnupg lsb-release

# Add Docker's official GPG key
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Set up the repository
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine and Docker Compose
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add your user to the docker group (avoids needing sudo)
sudo usermod -aG docker $USER
# Log out and log back in for this to take effect

# Verify installation
docker --version
docker compose version
```

### Docker Key Concepts
- **Image**: A blueprint/template (like a class in OOP)
- **Container**: A running instance of an image (like an object)
- **Dockerfile**: Recipe to build an image
- **docker-compose.yml**: Defines how multiple containers work together
- **Volume**: Persistent storage that survives container restarts
- **Port mapping**: Connecting container ports to your machine's ports

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | System health check |
| GET | `/trips` | List trips (paginated) |
| GET | `/trips/booking/{id}` | GPS trail for a booking |
| GET | `/trips/vehicle/{no}` | Trips by vehicle |
| GET | `/trips/customer/{id}` | Trips by customer |
| GET | `/trips/status/{G\|R}` | Filter by on-time status |
| GET | `/trips/search` | Multi-field search |
| GET | `/analytics/summary` | KPI summary |
| GET | `/analytics/by-gps-provider` | Stats per GPS provider |
| GET | `/analytics/by-customer` | Top customers by volume |
| GET | `/analytics/supplier-performance` | Supplier on-time rates |
| GET | `/analytics/top-routes` | Most common routes |
| GET | `/analytics/distance-distribution` | Distance bucket analysis |
| GET | `/data-quality/dead-letters` | Rejected records |

---

## Git Version Control Guide

See the project documentation (DOCX) for detailed Git workflow instructions including branching strategy, commit conventions, and PR workflows.

## License

This project is part of a data engineering learning portfolio.
