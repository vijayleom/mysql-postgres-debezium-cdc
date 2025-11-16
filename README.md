# Debezium MySQL CDC to Postgres

End-to-end change data capture (CDC) pipeline using Debezium to stream MySQL changes to Kafka, then consume and sink to Postgres.

## Architecture

```
MySQL (source) 
  ↓ binlog
Debezium MySQL Connector 
  ↓ Kafka topics
Custom Java Consumer 
  ↓ JDBC
Postgres (sink)
```

## Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| **zookeeper** | confluentinc/cp-zookeeper:7.6.1 | 2181 | Kafka metadata |
| **kafka** | confluentinc/cp-kafka:7.6.1 | 9092 | Message broker |
| **connect** | quay.io/debezium/connect:2.6 | 8083 | Debezium CDC runtime |
| **mysql** | mysql:8.0 | 3306 | Source database with binlog |
| **postgres** | postgres:16 | 5432 | Target sink database |
| **kafka-ui** | provectuslabs/kafka-ui:latest | 9095 | Kafka browser UI |

## Quick Start

### 1. Start Services

```bash
docker compose up -d
```

**Wait 30s for all services to initialize.** Verify:

```bash
docker compose ps
```

All services should show `Up` or `Up (healthy)`.

### 2. Register Debezium MySQL Connector

```bash
curl -X POST -H 'Content-Type: application/json' \
  --data @connectors/mysql-source.json \
  http://localhost:8083/connectors
```

**Check connector status:**

```bash
curl http://localhost:8083/connectors/mysql-connector/status | jq
```

Expect: `"state": "RUNNING"` for connector and task.

### 3. Verify Topics Created

```bash
docker exec kafka /usr/bin/kafka-topics \
  --bootstrap-server kafka:29092 --list | grep mysql.app
```

Expect:
- `mysql.app.customers`
- `mysql.app.orders`

### 4. View CDC Events

**From beginning (snapshot):**

```bash
docker exec -i kafka /usr/bin/kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic mysql.app.customers \
  --from-beginning \
  --max-messages 3
```

### 5. Test Live CDC

**Insert a row in MySQL:**

```bash
docker exec -i mysql mysql -uroot -proot -e \
  "INSERT INTO app.customers(first_name,last_name,email) 
   VALUES('Test','User','test@example.com');"
```

**See the change event:**

```bash
docker exec -i kafka /usr/bin/kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic mysql.app.customers \
  --timeout-ms 5000 \
  --max-messages 1
```

## Run Java Consumer (Standalone)

The consumer reads from Kafka and sinks to Postgres using UPSERT (ON CONFLICT).

### Build

```bash
cd consumer
mvn clean package
```

Output: `target/cdc-consumer-0.1.0.jar`

### Run

**From `consumer/` directory:**

```bash
java -jar target/cdc-consumer-0.1.0.jar
```

Logs show:
- `Config loaded from: classpath:config.properties`
- `Connected to Postgres.`
- `Subscribed to topics matching regex: ^[^.]+\.[^.]+\.(customers|orders)$`

**Verify sink in Postgres:**

```bash
docker exec -i cdc-postgres psql -U postgres -c \
  "SELECT id, first_name, last_name, email FROM customers ORDER BY id;"
```

## Configuration

### MySQL Connector

File: `connectors/mysql-source.json`

Key settings:
- `database.hostname`: `mysql`
- `database.user`: `dbz` (granted REPLICATION CLIENT/SLAVE)
- `snapshot.mode`: `initial` (snapshot existing rows on first start)
- `table.include.list`: `app.customers,app.orders`

### Consumer App

File: `consumer/src/main/resources/config.properties`

```properties
bootstrap.servers=localhost:9092
topics.regex=^[^.]+\.[^.]+\.(customers|orders)$
pg.url=jdbc:postgresql://localhost:5432/postgres
pg.user=postgres
pg.password=postgres
pk.customers=id
pk.orders=id
```

## Database Schemas

### MySQL (Source)

Schema: `app`

```sql
CREATE TABLE app.customers (
  id INT PRIMARY KEY AUTO_INCREMENT,
  first_name VARCHAR(255),
  last_name VARCHAR(255),
  email VARCHAR(255) UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Init script: `mysql/init/01_setup.sql` (runs on first start)

### Postgres (Sink)

Schema: `public`

```sql
CREATE TABLE public.customers (
  id BIGINT PRIMARY KEY,
  first_name TEXT,
  last_name TEXT,
  email TEXT,
  created_at VARCHAR,
  updated_at VARCHAR
);
```

Init script: `postgres/init/01_schema.sql` (runs on first start)

## Useful Commands

### Check MySQL Binlog Position

```bash
docker exec -i mysql mysql -uroot -proot -e "SHOW MASTER STATUS\G"
```

### List All Kafka Topics

```bash
docker exec kafka /usr/bin/kafka-topics \
  --bootstrap-server kafka:29092 --list
```

### Delete a Connector

```bash
curl -X DELETE http://localhost:8083/connectors/mysql-connector
```

### View Connector Logs

```bash
docker compose logs -f connect
```

### Reset Kafka Consumer Offset

```bash
docker exec kafka /usr/bin/kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --group cdc-sink \
  --reset-offsets --to-earliest \
  --topic mysql.app.customers \
  --execute
```

### Browse Kafka UI

Open: http://localhost:9095

- View topics, messages, consumer groups, connectors

## Troubleshooting

### Connector Not Registered

**Symptom:** No topics created.

**Fix:** Register the connector:

```bash
curl -X POST -H 'Content-Type: application/json' \
  --data @connectors/mysql-source.json \
  http://localhost:8083/connectors
```

### Consumer Shows No Logs

**Symptom:** `java -jar` runs but no INFO logs.

**Fix:** slf4j-simple needs INFO level (already set). Rebuild:

```bash
cd consumer && mvn clean package
```

### MySQL Not Capturing Changes

**Symptom:** Changes to MySQL not in Kafka.

**Check:**
1. Binlog enabled: `docker exec -i mysql mysql -uroot -proot -e "SHOW VARIABLES LIKE 'log_bin';"`
2. Connector running: `curl http://localhost:8083/connectors/mysql-connector/status`
3. Connector logs: `docker compose logs connect | grep -i error`

### Postgres Connection Failed

**Symptom:** Consumer error: "Connection refused" to Postgres.

**Fix:** Ensure Postgres is healthy:

```bash
docker compose ps postgres
docker exec cdc-postgres pg_isready -U postgres
```

## Clean Up

**Stop all services:**

```bash
docker compose down
```

**Remove volumes (destructive):**

```bash
docker compose down -v
```

## Tech Stack

- **Debezium 2.6** (MySQL Connector)
- **Kafka 3.7** (Confluent Platform 7.6.1)
- **MySQL 8.0** (with binlog ROW format, GTID)
- **Postgres 16**
- **Java 17** (consumer app with Kafka Clients 3.7, Jackson, JDBC)

## License

MIT
