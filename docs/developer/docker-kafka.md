


# Kafka Docker Setup

This repository includes a Docker Compose configuration for running Apache Kafka locally.

## Prerequisites

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/) (usually included with Docker Desktop)

## Running Kafka

To start the Kafka environment:

```bash
docker-compose up -d
```

This will start:
- Kafka broker on port 9092
- Kafka controller on port 9093 (KRaft mode)

To stop the containers:

```bash
docker-compose down
```

## Verifying the Setup

Check if the containers are running:

```bash
docker-compose ps
```

## Testing Kafka

### Create a Topic

```bash
docker exec -it kafka kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### List Topics

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Produce Messages

```bash
docker exec -it kafka kafka-console-producer --topic test-topic --bootstrap-server localhost:9092
```

Type messages and press Enter. Press Ctrl+C to exit.

### Consume Messages

```bash
docker exec -it kafka kafka-console-consumer --topic test-topic --from-beginning --bootstrap-server localhost:9092
```

Press Ctrl+C to exit.

## Integration with Rust Application

To integrate Kafka with your Rust application, you can use libraries like:
- [rdkafka](https://crates.io/crates/rdkafka)
- [kafka-rust](https://crates.io/crates/kafka)

Example Cargo.toml addition:
```toml
[dependencies]
rdkafka = "0.25"
```

## Troubleshooting

- If you encounter connection issues, ensure that the ports 9092 and 9093 are not being used by other applications.
- Check container logs with `docker-compose logs kafka`.
