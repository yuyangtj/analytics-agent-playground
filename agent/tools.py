"""The run_sql tool: the agent's only way to interact with the business database."""

import decimal
import datetime

import duckdb

MAX_ROWS = 200

RUN_SQL_TOOL = {
    "name": "run_sql",
    "description": (
        "Execute a read-only SQL query against the business database (DuckDB dialect) "
        "and return the results. Use this to explore the schema (e.g. SELECT * FROM "
        "information_schema.tables/columns) and to answer the user's question."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "A SQL query to execute."},
        },
        "required": ["query"],
    },
}


def _jsonable(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value


def run_sql(con: duckdb.DuckDBPyConnection, query: str) -> dict:
    """Executes `query` against `con` (expected to be read-only) and returns a JSON-safe result dict."""
    try:
        cursor = con.execute(query)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
    except Exception as e:
        return {"error": str(e)}

    truncated = len(rows) > MAX_ROWS
    rows = rows[:MAX_ROWS]
    records = [
        {col: _jsonable(val) for col, val in zip(columns, row)}
        for row in rows
    ]

    result = {"columns": columns, "rows": records, "row_count": len(records)}
    if truncated:
        result["truncated"] = True
        result["note"] = f"Result truncated to first {MAX_ROWS} rows."
    return result
