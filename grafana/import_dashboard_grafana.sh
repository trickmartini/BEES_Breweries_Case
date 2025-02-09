#!/bin/bash

GRAFANA_URL="http://localhost:3000"
GRAFANA_USER="admin"
GRAFANA_PASSWORD="admin"

DASHBOARD_JSON=$(cat ../grafana/dashboards/etl_monitoring.json)

curl -X POST "$GRAFANA_URL/api/dashboards/db" \
     -u "$GRAFANA_USER:$GRAFANA_PASSWORD" \
     -H "Content-Type: application/json" \
     -d "{ \"dashboard\": $DASHBOARD_JSON, \"overwrite\": true }"
