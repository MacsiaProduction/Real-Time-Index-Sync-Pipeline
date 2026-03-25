#!/bin/bash
set -e

ES_URL="http://elasticsearch:9200"
INDEX_NAME="products"
MAPPING_FILE="/mapping/index-mapping.json"

echo "Waiting for Elasticsearch to be ready..."
until curl -s "${ES_URL}/_cluster/health" | grep -q '"status":"green"\|"status":"yellow"'; do
    sleep 2
done
echo "Elasticsearch is ready"

# Check if index already exists
if curl -s -o /dev/null -w "%{http_code}" "${ES_URL}/${INDEX_NAME}" | grep -q "200"; then
    echo "Index '${INDEX_NAME}' already exists, skipping creation"
else
    echo "Creating index '${INDEX_NAME}'..."
    curl -s -X PUT "${ES_URL}/${INDEX_NAME}" \
        -H "Content-Type: application/json" \
        -d @"${MAPPING_FILE}"
    echo ""
    echo "Index '${INDEX_NAME}' created successfully"
fi
