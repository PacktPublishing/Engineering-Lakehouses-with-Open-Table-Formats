CREATE DATABASE IF NOT EXISTS retail;
USE retail;

CREATE TABLE IF NOT EXISTS products (
  product_id INT PRIMARY KEY,
  name VARCHAR(255),
  category VARCHAR(255),
  price DOUBLE,
  description TEXT,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO products (product_id, name, category, price, description, updated_at)
VALUES
  (1, 'Widget', 'Tools', 10.0, 'seed', NOW()),
  (2, 'Gadget', 'Electronics', 99.0, 'seed', NOW())
ON DUPLICATE KEY UPDATE
  name=VALUES(name),
  category=VALUES(category),
  price=VALUES(price),
  description=VALUES(description),
  updated_at=VALUES(updated_at);
