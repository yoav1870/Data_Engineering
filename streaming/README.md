# Nail Salon Streaming Pipeline

This directory contains the real-time streaming components for the Nail Salon data engineering pipeline.

## Architecture Overview

```
Customer Ratings (Real-time) → Kafka → Spark Streaming → Bronze Layer (Iceberg)
```

## Components

### 1. Kafka Infrastructure

- **Zookeeper**: Coordination service for Kafka
- **Kafka Broker**: Message broker for real-time data streaming
- **Topic**: `nail_salon_ratings` - stores customer rating events

### 2. Data Producer

- **File**: `producer/ratings_producer.py`
- **Purpose**: Simulates real-time customer ratings
- **Features**:
  - Generates realistic rating data
  - Sends JSON messages to Kafka topic
  - Configurable streaming duration and frequency
  - Customer ID-based partitioning

### 3. Data Consumer

- **File**: `consumer/ratings_consumer.py`
- **Purpose**: Consumes ratings from Kafka and writes to Bronze layer
- **Features**:
  - Spark Structured Streaming
  - JSON parsing with schema validation
  - Iceberg table writing with partitioning
  - Checkpoint management for fault tolerance

### 4. Storage

- **MinIO**: S3-compatible object storage
- **Buckets**:
  - `warehouse`: Iceberg tables and checkpoints
  - `raw-data`: Raw data files

## Data Schema

### Ratings Event Schema

```json
{
  "customer_id": 1,
  "branch_id": 1,
  "employee_id": 5,
  "treatment_id": 1,
  "rating_value": 4.5,
  "comment": "Excellent service!",
  "timestamp": "2025-01-15T10:30:00"
}
```

### Bronze Table Schema

```sql
CREATE TABLE bronze_ratings_streaming (
    customer_id INT,
    branch_id INT,
    employee_id INT,
    treatment_id INT,
    rating_value FLOAT,
    comment STRING,
    timestamp STRING,
    processed_timestamp TIMESTAMP,
    _kafka_topic STRING,
    _kafka_partition INT,
    _kafka_offset BIGINT
) USING iceberg
PARTITIONED BY (days(timestamp))
```

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.9+ (for local testing)

### 1. Start the Infrastructure

```bash
# Create external network
docker network create iceberg_net

# Start all services
docker-compose up -d
```

### 2. Verify Services

```bash
# Check if Kafka is running
docker logs kafka

# Check if MinIO is accessible
# Open http://localhost:9001 (admin/password)

# Check if producer is sending data
docker logs ratings-producer

# Check if consumer is processing data
docker logs ratings-consumer
```

### 3. Test the Pipeline

```bash
# Install dependencies
pip install kafka-python

# Run test script
python test_streaming.py
```

## Configuration

### Producer Configuration

- **Streaming Duration**: Default 10 minutes
- **Message Interval**: Default 3 seconds
- **Kafka Topic**: `nail_salon_ratings`
- **Bootstrap Servers**: `kafka:9092` (internal), `localhost:29092` (external)

### Consumer Configuration

- **Spark Application**: `Nail Salon Ratings Consumer`
- **Checkpoint Location**: `s3a://warehouse/checkpoints/ratings`
- **Starting Offset**: `latest` (only new messages)
- **Batch Processing**: Uses `foreachBatch` for Iceberg writes

### Kafka Configuration

- **Broker ID**: 1
- **Replication Factor**: 1 (single broker)
- **Ports**: 9092 (internal), 29092 (external)

## Monitoring

### Kafka Topics

```bash
# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe topic
docker exec kafka kafka-topics --describe --topic nail_salon_ratings --bootstrap-server localhost:9092
```

### MinIO Console

- **URL**: http://localhost:9001
- **Username**: admin
- **Password**: password

### Spark UI

- **URL**: http://localhost:4040 (when Spark job is running)

## Troubleshooting

### Common Issues

1. **Kafka Connection Failed**

   - Check if Zookeeper is running: `docker logs zookeeper`
   - Check if Kafka is running: `docker logs kafka`
   - Verify network: `docker network ls`

2. **MinIO Connection Failed**

   - Check MinIO logs: `docker logs minio`
   - Verify buckets exist: `docker logs minio-init`

3. **Spark Job Fails**
   - Check consumer logs: `docker logs ratings-consumer`
   - Verify Iceberg dependencies are loaded
   - Check MinIO connectivity from Spark container

### Logs

```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f ratings-producer
docker-compose logs -f ratings-consumer
```

## Next Steps

1. **Scale the Pipeline**: Add more Kafka brokers for high availability
2. **Add Monitoring**: Integrate with Prometheus/Grafana
3. **Data Quality**: Add validation rules in the consumer
4. **Error Handling**: Implement dead letter queues for failed messages
5. **Security**: Add authentication and encryption

## Files Structure

```
streaming/
├── docker-compose.yml          # Infrastructure orchestration
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── test_streaming.py           # Test script
├── producer/
│   ├── Dockerfile             # Producer container
│   └── ratings_producer.py    # Kafka producer
└── consumer/
    └── ratings_consumer.py    # Spark streaming consumer
```
