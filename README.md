# Example of Java <-> Rust communication via Kafka

## Requirements:

 * Docker-compose
 * Java 12 or later
 * Maven
 * Rust 1.51 or later

## How to run it

### Prepare Kafka

```
make run-kafka
```

Navigate to the Control Center web interface at http://localhost:9021, select one healthy cluster and create 2 topics:

1. "j2rs"
1. "rs2j"

Since it is development example, they should have replication factor 1

### Start Java worker
```
run-java-worker
```

### Start Rust worker
In separate console:

```
make run-rust-worker
```

