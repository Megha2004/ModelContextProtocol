#!/usr/bin/env python3
"""
Working PostgreSQL MCP Server for Claude
"""

import json
import sys
import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "dbname",
    "user": "postgres",
    "password": "password"
}


def log(msg):
    print(f"[PG-MCP] {msg}", file=sys.stderr, flush=True)


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def send(resp):
    """All responses must be valid JSON-RPC 2.0"""
    resp.setdefault("jsonrpc", "2.0")
    print(json.dumps(resp), flush=True)


def format_text(text):
    """MCP content format helper"""
    return {"type": "text", "text": str(text)}


# ------------------------ DB FUNCTIONS ----------------------------

def query_db(query):
    if not query.strip().upper().startswith("SELECT"):
        return "❌ Error: Only SELECT allowed."

    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()
        conn.close()

        if not rows:
            return "No results"

        return json.dumps(rows, indent=2, default=str)

    except Exception as e:
        return f"❌ DB Error: {e}"


def list_tables():
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema='public'
                ORDER BY table_name
            """)
            rows = cur.fetchall()
        conn.close()

        if not rows:
            return "No tables"

        return "\n".join(f"- {row[0]}" for row in rows)

    except Exception as e:
        return f"❌ DB Error: {e}"


def describe_table(name):
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns
                WHERE table_name=%s
                ORDER BY ordinal_position
            """, (name,))
            rows = cur.fetchall()
        conn.close()

        if not rows:
            return f"Table `{name}` not found."

        return json.dumps(rows, indent=2)

    except Exception as e:
        return f"❌ DB Error: {e}"


# ------------------------- MCP HANDLING ----------------------------

def handle(msg):
    method = msg.get("method")
    msg_id = msg.get("id")
    params = msg.get("params", {})

    log(f"Received → {method}")

    # ---- Initialization ----
    if method == "initialize":
        return {
            "id": msg_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {},
                    "resources": {},
                    "prompts": {}
                },
                "serverInfo": {
                    "name": "postgres-mcp",
                    "version": "1.0.0"
                }
            }
        }

    # ---- List tools ----
    if method == "tools/list":
        return {
            "id": msg_id,
            "result": {
                "tools": [
                    {
                        "name": "query_database",
                        "description": "Execute SELECT query",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "query": {"type": "string"}
                            },
                            "required": ["query"]
                        }
                    },
                    {
                        "name": "list_tables",
                        "description": "List PostgreSQL tables",
                        "inputSchema": {"type": "object"}
                    },
                    {
                        "name": "describe_table",
                        "description": "Describe a table",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "table_name": {"type": "string"}
                            },
                            "required": ["table_name"]
                        }
                    }
                ]
            }
        }

    # ---- Tool Call ----
    if method == "tools/call":
        tool = params.get("name")
        args = params.get("arguments", {})

        if tool == "query_database":
            out = query_db(args.get("query", ""))

        elif tool == "list_tables":
            out = list_tables()

        elif tool == "describe_table":
            out = describe_table(args.get("table_name", ""))

        else:
            out = f"Unknown tool: {tool}"

        return {
            "id": msg_id,
            "result": {
                "content": [format_text(out)]
            }
        }

    # ---- Notifications (No reply) ----
    if method == "notifications/initialized":
        return None

    # ---- Unknown method ----
    return {
        "id": msg_id,
        "error": {"code": -32601, "message": f"Unknown method: {method}"}
    }


# ----------------------------- MAIN --------------------------------

def main():
    log("PostgreSQL MCP Server Running")

    while True:
        line = sys.stdin.readline()
        if not line:
            break

        line = line.strip()
        if not line:
            continue

        try:
            msg = json.loads(line)
            response = handle(msg)

            if response:
                send(response)

        except Exception as e:
            send({
                "id": msg.get("id", None),
                "error": {"code": -32000, "message": str(e)}
            })


if __name__ == "__main__":
    main()
