#!/bin/sh
set -e

# Simple helper to generate a few CDC events in MySQL so Debezium emits them to Kafka.
# Run from repo root: docker compose exec mysql-primary bash /scripts/pump_mysql_changes.sh

mysql -u root -prootpw retail <<'SQL'
-- Product 201: insert -> update -> delete -> reinsert
INSERT INTO products (product_id, name, category, price, description, updated_at)
VALUES (201, 'Demo Widget', 'Tools', 20.0, 'insert v1', NOW())
ON DUPLICATE KEY UPDATE name=VALUES(name), category=VALUES(category), price=VALUES(price), description=VALUES(description), updated_at=VALUES(updated_at);

UPDATE products SET price = 22.5, description = 'update v2' WHERE product_id = 201;
DELETE FROM products WHERE product_id = 201;
INSERT INTO products (product_id, name, category, price, description, updated_at)
VALUES (201, 'Demo Widget', 'Tools', 23.5, 'reinstate v3', NOW())
ON DUPLICATE KEY UPDATE name=VALUES(name), category=VALUES(category), price=VALUES(price), description=VALUES(description), updated_at=VALUES(updated_at);

-- Product 202: insert -> update
INSERT INTO products (product_id, name, category, price, description, updated_at)
VALUES (202, 'Demo Gizmo', 'Electronics', 55.0, 'insert v1', NOW())
ON DUPLICATE KEY UPDATE name=VALUES(name), category=VALUES(category), price=VALUES(price), description=VALUES(description), updated_at=VALUES(updated_at);

UPDATE products SET price = 58.0, description = 'update v2' WHERE product_id = 202;
SQL

echo "CDC events pumped: 201 (insert -> update -> delete -> reinsert), 202 (insert -> update)"
