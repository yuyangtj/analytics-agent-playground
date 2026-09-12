#!/bin/sh
# Registers every connector config under /connectors/*.json against Kafka
# Connect's REST API. Run by the connector-init service in
# docker-compose.yml, mounted in rather than inlined as a compose
# `command:` string -- Compose interpolates any $VAR/$(...) found inside
# the YAML file itself before the container ever sees it, which silently
# ate every variable in an earlier inline version of this script (fixable
# with `$$` escaping, but a real script file is less of a trap to edit
# later).
#
# Connector name = filename without .json (e.g. connectors/postgres-source.json
# registers as "postgres-source").
set -eu

status=0
for f in /connectors/*.json; do
  name=$(basename "$f" .json)
  resp=$(curl -s -w '\n%{http_code}' -X PUT -H "Content-Type: application/json" \
    --data @"$f" \
    "http://connect:8083/connectors/$name/config")

  code=$(echo "$resp" | tail -1)
  body=$(echo "$resp" | sed '$d')
  echo "=== $name ==="
  echo "$body"

  case "$code" in
    200|201) echo "$name registered ($code)" ;;
    *) echo "$name registration FAILED ($code)"; status=1 ;;
  esac
done

exit "$status"
