-- Initialize MySQL for Debezium CDC and create sample APP schema
-- This script runs once on first container start by the MySQL entrypoint

-- Create an application schema analogous to the Oracle APP schema
CREATE SCHEMA IF NOT EXISTS app;

-- Ensure the Debezium user has required privileges (user is created by env var)
-- REPLICATION CLIENT/SLAVE and global SELECT/SHOW DATABASES are required
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'dbz'@'%';
FLUSH PRIVILEGES;

-- Create sample tables
CREATE TABLE IF NOT EXISTS app.customers (
  id INT PRIMARY KEY AUTO_INCREMENT,
  first_name VARCHAR(255) NOT NULL,
  last_name VARCHAR(255) NOT NULL,
  email VARCHAR(255) UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS app.orders (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  purchaser INT NOT NULL,
  quantity INT NOT NULL,
  product VARCHAR(255) NOT NULL,
  CONSTRAINT fk_orders_customer FOREIGN KEY (purchaser) REFERENCES app.customers(id)
);

-- Seed sample data (similar to Oracle script)
INSERT INTO app.customers(first_name, last_name, email)
VALUES ('Sally', 'Thomas', 'sally.thomas@acme.com')
ON DUPLICATE KEY UPDATE email=VALUES(email);

INSERT INTO app.customers(first_name, last_name, email)
VALUES ('George', 'Bailey', 'gbailey@foobar.com')
ON DUPLICATE KEY UPDATE email=VALUES(email);

-- Optional: tune binlog retention or row image at runtime if needed (already set via command):
-- SET GLOBAL binlog_row_image = 'FULL';
