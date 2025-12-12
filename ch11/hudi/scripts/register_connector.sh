#!/bin/sh
set -e

CONNECT_URL="http://connect:8083"

echo "Waiting for Kafka Connect to be ready at ${CONNECT_URL}..."
until curl -s "${CONNECT_URL}/connectors" >/dev/null 2>&1; do
  sleep 2
done

echo "Registering Debezium MySQL connector..."
curl -s -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  --data @/connectors/globalmart-mysql-connector.json \
  "${CONNECT_URL}/connectors" || true

echo "Current connectors:"
curl -s "${CONNECT_URL}/connectors" || true
echo ""
