package com.example.cdc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DebeziumToPostgresConsumer {
    private static final Logger log = LoggerFactory.getLogger(DebeziumToPostgresConsumer.class);
    private final Properties appProps = new Properties();
    private final ObjectMapper mapper = new ObjectMapper();
    private Connection pgConnection;
    private Map<String, String> tableMappings = new HashMap<>();
    private Map<String, List<String>> tablePrimaryKeys = new HashMap<>();

    public static void main(String[] args) throws Exception {
        DebeziumToPostgresConsumer app = new DebeziumToPostgresConsumer();
        app.loadConfig();
        app.connectPostgres();
        app.consumeLoop();
    }

    private void loadConfig() throws IOException {
        // Load from env-provided path first, then local file, then classpath
        String envPath = System.getenv().getOrDefault("CONSUMER_CONFIG", "");
        String configSource = "unknown";
        boolean loaded = false;
        if (!envPath.isBlank()) {
            try (FileInputStream fis = new FileInputStream(envPath)) {
                appProps.load(fis);
                loaded = true;
                log.info("Loaded config from {}", envPath);
                configSource = "env:CONSUMER_CONFIG=" + envPath;
            } catch (IOException e) {
                log.warn("Failed to load config from CONSUMER_CONFIG path {}: {}", envPath, e.getMessage());
            }
        }
        if (!loaded) {
            try (FileInputStream fis = new FileInputStream("config.properties")) {
                appProps.load(fis);
                loaded = true;
                log.info("Loaded config from working directory config.properties");
                configSource = "working-directory:config.properties";
            } catch (IOException e) {
                log.warn("No config.properties in working directory. Trying classpath.");
            }
        }
        if (!loaded) {
            try (var is = DebeziumToPostgresConsumer.class.getClassLoader().getResourceAsStream("config.properties")) {
                if (is != null) {
                    appProps.load(is);
                    loaded = true;
                    log.info("Loaded config from classpath");
                    configSource = "classpath:config.properties";
                }
            }
        }
        if (!loaded) {
            throw new IOException("Unable to load config.properties from CONSUMER_CONFIG, working directory, or classpath");
        }
        // Always print an explicit summary of where the config was loaded from
        log.info("Config loaded from: {}", configSource);
        // Load PK mappings
        appProps.forEach((k, v) -> {
            String key = k.toString();
            if (key.startsWith("pk.")) {
                String table = key.substring(3); // after pk.
                List<String> pkCols = Arrays.stream(v.toString().split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .toList();
                tablePrimaryKeys.put(table, pkCols);
            }
            if (key.startsWith("map.")) {
                String table = key.substring(4); // after map.
                tableMappings.put(table, v.toString().trim());
            }
        });
        log.info("Loaded PK mappings: {}", tablePrimaryKeys);
        log.info("Loaded table mappings: {}", tableMappings);
    }

    private void connectPostgres() throws SQLException {
        String url = appProps.getProperty("pg.url");
        String user = appProps.getProperty("pg.user");
        String password = appProps.getProperty("pg.password");
        pgConnection = DriverManager.getConnection(url, user, password);
        pgConnection.setAutoCommit(true);
        log.info("Connected to Postgres.");
    }

    private KafkaConsumer<String, String> buildConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appProps.getProperty("bootstrap.servers"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, appProps.getProperty("group.id", "cdc-sink"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Enable cooperative rebalancing
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        return new KafkaConsumer<>(props);
    }

    private void consumeLoop() throws SQLException, IOException {
        Pattern topicPattern = Pattern.compile(appProps.getProperty("topics.regex"));
        try (KafkaConsumer<String, String> consumer = buildConsumer()) {
            consumer.subscribe(topicPattern);
            log.info("Subscribed to topics matching regex: {}", topicPattern);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record.topic(), record.key(), record.value());
                }
            }
        }
    }

    private void processRecord(String topic, String key, String value) {
        try {
            if (value == null || value.isBlank()) {
                log.debug("Tombstone or empty message for key {} on topic {}", key, topic);
                return;
            }

            JsonNode root = mapper.readTree(value);
            // Debezium MySQL (2.x) typically has the envelope at the root (no 'payload')
            JsonNode envelope = root.has("payload") && root.get("payload").isObject() ? root.get("payload") : root;

            String op = envelope.path("op").asText();
            JsonNode after = envelope.get("after");
            JsonNode before = envelope.get("before");

            // Prefer table/db from the Debezium source block for MySQL
            JsonNode source = envelope.get("source");
            String db = (source != null && source.has("db")) ? source.get("db").asText() : null;
            String tableFromSource = (source != null && source.has("table")) ? source.get("table").asText() : null;

            String table = (tableFromSource != null && !tableFromSource.isBlank())
                    ? tableFromSource
                    : extractTableFromTopic(topic);

            // Resolve target table via mapping: try "db.table" first, then just "table"
            String dbTableKey = (db != null && tableFromSource != null) ? (db + "." + tableFromSource) : null;
            String targetTable = table.toLowerCase();
            if (dbTableKey != null && tableMappings.containsKey(dbTableKey)) {
                targetTable = tableMappings.get(dbTableKey);
            } else if (tableMappings.containsKey(table)) {
                targetTable = tableMappings.get(table);
            }

            // Resolve PKs: try "db.table" first, then just "table", default to "id"
            List<String> pkCols;
            if (dbTableKey != null && tablePrimaryKeys.containsKey(dbTableKey)) {
                pkCols = tablePrimaryKeys.get(dbTableKey);
            } else if (tablePrimaryKeys.containsKey(table)) {
                pkCols = tablePrimaryKeys.get(table);
            } else {
                pkCols = List.of("id");
            }

            switch (op) {
                case "c":
                case "r":
                case "u":
                    upsertRow(targetTable, after, pkCols);
                    break;
                case "d":
                    deleteRow(targetTable, before, pkCols);
                    break;
                default:
                    log.warn("Unknown op '{}' for topic {} key {}", op, topic, key);
            }
        } catch (Exception e) {
            log.error("Failed processing record from topic {} key {}", topic, key, e);
        }
    }
    
    private String extractTableFromTopic(String topic) {
        // Debezium default topic: <prefix>.<schema>.<table>
        String[] parts = topic.split("\\.");
        return parts[parts.length - 1];
    }

    private void upsertRow(String table, JsonNode after, List<String> pkCols) throws SQLException {
        if (after == null) {
            log.warn("Skip upsert; after is null for table {}", table);
            return;
        }
        Map<String, Object> columnValues = jsonNodeToColumnMap(after);

        // Build dynamic UPSERT using ON CONFLICT
        String columnsCsv = String.join(", ", columnValues.keySet());
        String placeholders = String.join(", ", Collections.nCopies(columnValues.size(), "?"));
        String updateSet = buildUpdateSet(columnValues.keySet(), pkCols);
        String pkCsvLower = String.join(", ", pkCols.stream().map(String::toLowerCase).toList());

        String sql = "INSERT INTO " + table + " (" + columnsCsv.toLowerCase() + ") VALUES (" + placeholders + ") " +
                "ON CONFLICT (" + pkCsvLower + ") DO UPDATE SET " + updateSet + ";";

        try (PreparedStatement ps = pgConnection.prepareStatement(sql)) {
            int idx = 1;
            for (String col : columnValues.keySet()) {
                ps.setObject(idx++, columnValues.get(col));
            }
            ps.executeUpdate();
        }
    }

    private String buildUpdateSet(Set<String> columns, List<String> pkCols) {
        List<String> assignments = new ArrayList<>();
        for (String col : columns) {
            if (pkCols.contains(col)) continue; // don't update PK
            assignments.add(col.toLowerCase() + " = EXCLUDED." + col.toLowerCase());
        }
        if (assignments.isEmpty()) {
            return pkCols.get(0).toLowerCase() + " = EXCLUDED." + pkCols.get(0).toLowerCase(); // trivial case
        }
        return String.join(", ", assignments);
    }

    private void deleteRow(String table, JsonNode before, List<String> pkCols) throws SQLException {
        if (before == null) {
            log.warn("Skip delete; before is null for table {}", table);
            return;
        }
        Map<String, Object> beforeMap = jsonNodeToColumnMap(before);
        List<String> whereClauses = new ArrayList<>();
        for (String pk : pkCols) {
            whereClauses.add(pk.toLowerCase() + " = ?");
        }
        String sql = "DELETE FROM " + table + " WHERE " + String.join(" AND ", whereClauses);
        try (PreparedStatement ps = pgConnection.prepareStatement(sql)) {
            int idx = 1;
            for (String pk : pkCols) {
                ps.setObject(idx++, beforeMap.get(pk));
            }
            int count = ps.executeUpdate();
            log.info("Deleted {} rows from {}", count, table);
        }
    }

    private Map<String, Object> jsonNodeToColumnMap(JsonNode node) {
        Map<String, Object> map = new LinkedHashMap<>();
        node.fieldNames().forEachRemaining(field -> {
            JsonNode valueNode = node.get(field);
            Object val = null;
            if (valueNode == null || valueNode.isNull()) {
                val = null;
            } else if (valueNode.isNumber()) {
                val = valueNode.numberValue();
            } else if (valueNode.isTextual()) {
                val = valueNode.asText();
            } else if (valueNode.isBoolean()) {
                val = valueNode.booleanValue();
            } else {
                val = valueNode.toString();
            }
            map.put(field, val);
        });
        return map;
    }
}