#!/bin/sh
# Registers connector-postgres.json against Kafka Connect's REST API.
# Run by the connector-init service in docker-compose.yml, mounted in
# rather than inlined as a compose `command:` string -- Compose interpolates
# any $VAR/$(...) found inside the YAML file itself before the container
# ever sees it, which silently ate every variable in an earlier inline
# version of this script (fixable with `$$` escaping, but a real script
# file is less of a trap to edit later).
set -eu

resp=$(curl -s -w '\n%{http_code}' -X PUT -H "Content-Type: application/json" \
  --data @/connector-postgres.json \
  http://connect:8083/connectors/postgres-source/config)

code=$(echo "$resp" | tail -1)
body=$(echo "$resp" | sed '$d')
echo "$body"

case "$code" in
  200|201) echo "connector registered ($code)" ;;
  *) echo "connector registration failed ($code)"; exit 1 ;;
esac
