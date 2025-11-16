CREATE SCHEMA IF NOT EXISTS public;

-- Sink tables mirroring Oracle APP schema (column names in lower_snake_case)
CREATE TABLE IF NOT EXISTS public.customers (
  id           BIGINT PRIMARY KEY,
  first_name   TEXT,
  last_name    TEXT,
  email        TEXT,
  created_at   VARCHAR,
  updated_at   VARCHAR
);

CREATE TABLE IF NOT EXISTS public.orders (
  id            BIGINT PRIMARY KEY,
  customer_id   BIGINT NOT NULL,
  status        TEXT,
  amount        NUMERIC(12,2),
  created_at    VARCHAR,
  updated_at    VARCHAR
);
