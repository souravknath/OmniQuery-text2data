"""
QueryExecutor.py
Reads the LLM-generated query plan from llm_output.json, executes each query
in the order specified by 'execution_order', substitutes placeholder values
from earlier results into dependent queries, and writes all results (labelled
by DB name) to QueryOutput.json.
"""

import os
import re
import json
import ast
import sys
from datetime import date, datetime

import pymongo
import pyodbc
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# Force UTF-8 output on Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# ---------------------------------------------------------------------------
# JSON serialiser
# ---------------------------------------------------------------------------
class _Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------
def _pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def _sql_server_conn():
    raw = os.getenv("SQLSERVER_CONNECTION_STRING", "").strip('"')
    return pyodbc.connect(raw)


def _mongo_db():
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    db  = os.getenv("MONGO_DB", "CustomerDB")
    return pymongo.MongoClient(uri)[db]


# ---------------------------------------------------------------------------
# Placeholder resolution
# Patterns handled (case-insensitive for IN keyword):
# ---------------------------------------------------------------------------
_RE_IN   = re.compile(r'\bIN\s*\(\s*\{?\{([\w.]+)\.(\w+)\}\}?\s*\)', re.IGNORECASE)
_RE_EQ   = re.compile(r'(=\s*ANY\s*\(|=\s*)\{?\{([\w.]+)\.(\w+)\}\}?(\s*\))?', re.IGNORECASE)
_RE_BARE = re.compile(r'\{?\{([\w.]+)\.(\w+)\}\}?')
# Strip "AND <expr> IN ({placeholder})" or "AND <expr> = {placeholder}" when upstream is empty
_RE_AND_IN  = re.compile(r'\s+AND\s+[\w."]+\s+IN\s*\(\s*\{?\{[\w.]+\.\w+\}\}?\s*\)', re.IGNORECASE)
_RE_AND_EQ  = re.compile(r'\s+AND\s+[\w."]+\s*=\s*(?:ANY\s*\()?\s*\{?\{[\w.]+\.\w+\}\}?\s*\)?', re.IGNORECASE)


def _resolve_placeholders(query: str, results_so_far: dict, is_mongo: bool = False) -> str:
    def _values_for(db_key, field):
        rows = results_so_far.get(db_key, [])
        values = []
        for row in rows:
            if field in row:
                values.append(row[field])
            else:
                for k, v in row.items():
                    if k.lower() == field.lower():
                        values.append(v)
                        break
        return values

    def _fmt(values):
        if is_mongo:
            return values  # Return raw values for JSON serialization
        return [f"'{v}'" if isinstance(v, str) else str(v) for v in values]

    def _is_empty_upstream(placeholder_text: str) -> bool:
        """Return True if the placeholder refers to a known-empty upstream result."""
        m = re.search(r'\{([\w.]+)\.(\w+)\}', placeholder_text)
        if not m:
            return False
        db_parts, field = m.group(1), m.group(2)
        db_key = db_parts.split('.')[0]
        if db_key not in results_so_far:
            return False
        return len(_values_for(db_key, field)) == 0

    # Pre-pass: strip entire AND clauses whose placeholder resolves to empty
    def _strip_empty_and(m):
        if _is_empty_upstream(m.group(0)):
            print(f"        [WARN] Empty upstream result detected -> removing AND clause: {m.group(0).strip()}")
            return ""   # drop the whole AND condition
        return m.group(0)

    query = _RE_AND_IN.sub(_strip_empty_and, query)
    query = _RE_AND_EQ.sub(_strip_empty_and, query)

    # Pass 1: IN ({DB.Field}) – remaining (non-AND) occurrences
    def _sub_in(m):
        db_parts, field = m.group(1), m.group(2)
        db_key = db_parts.split('.')[0]
        if db_key not in results_so_far:
            return m.group(0)
        values = _values_for(db_key, field)
        if not values:
            return "IN (SELECT NULL WHERE 1=0)"
        return f"IN ({', '.join(_fmt(values))})"

    query = _RE_IN.sub(_sub_in, query)

    # Pass 2: = {DB.Field}  or  = ANY({DB.Field})
    def _sub_eq(m):
        db_parts = m.group(2)
        field  = m.group(3)
        db_key = db_parts.split('.')[0]
        if db_key not in results_so_far:
            return m.group(0)
        values = _values_for(db_key, field)
        if not values:
            return "= NULL"
        formatted = _fmt(values)
        if len(values) == 1:
            return f"= {formatted[0]}"
        return f"IN ({', '.join(formatted)})"

    query = _RE_EQ.sub(_sub_eq, query)

    # Pass 3: bare {DB.Field}
    def _sub_bare(m):
        db_parts, field = m.group(1), m.group(2)
        db_key = db_parts.split('.')[0]
        if db_key not in results_so_far:
            return m.group(0)
        values = _values_for(db_key, field)
        if not values:
            return "NULL"
        if is_mongo:
            return ", ".join([json.dumps(v) for v in values])
        return ", ".join(_fmt(values))

    return _RE_BARE.sub(_sub_bare, query)



# ---------------------------------------------------------------------------
# Executors per DB type
# ---------------------------------------------------------------------------
def _run_postgres(query: str) -> list:
    conn = _pg_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _run_sql_server(query: str) -> list:
    conn = _sql_server_conn()
    try:
        cur = conn.cursor()
        cur.execute(query)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _run_mongo(query_str: str) -> list:
    mdb = _mongo_db()

    try:
        query_obj = json.loads(query_str)
    except json.JSONDecodeError:
        try:
            query_obj = ast.literal_eval(query_str)
        except Exception:
            query_obj = None

    if isinstance(query_obj, dict) and "collection" in query_obj:
        collection = mdb[query_obj["collection"]]
        pipeline   = query_obj.get("pipeline", [])
        rows       = list(collection.aggregate(pipeline))
    elif isinstance(query_obj, list):
        rows = []
        for col_name in mdb.list_collection_names():
            rows = list(mdb[col_name].aggregate(query_obj))
            if rows:
                break
    else:
        rows = []
        for col_name in mdb.list_collection_names():
            tmp = list(mdb[col_name].find(query_obj or {}, {"_id": 0}))
            if tmp:
                rows = tmp
                break

    # Remap _id if it came from a $group stage
    pipeline_ref = query_obj.get("pipeline", []) if isinstance(query_obj, dict) else (query_obj if isinstance(query_obj, list) else [])
    group_field = None
    for stage in pipeline_ref:
        if "$group" in stage:
            gid = stage["$group"].get("_id", "")
            if isinstance(gid, str) and gid.startswith("$"):
                group_field = gid[1:]
            break

    clean = []
    for row in rows:
        cleaned = {}
        for k, v in row.items():
            val = str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
            if k == "_id":
                if group_field:
                    cleaned[group_field] = val
                cleaned["_id"] = val
            else:
                cleaned[k] = val
        clean.append(cleaned)
    return clean


def _detect_db_type(db_name: str) -> str:
    name_l = db_name.lower()
    if "mongo" in name_l:
        return "mongo"
    if "inventory" in name_l or "sql_inventory" in name_l or "mssql" in name_l:
        return "sqlserver"
    return "postgres"


# ---------------------------------------------------------------------------
# Pretty-print a result table to the console
# ---------------------------------------------------------------------------
def _print_table(db_name: str, rows: list, max_rows: int = 50):
    if not rows:
        print(f"        [INFO] {db_name}: No rows returned.\n")
        return

    cols = list(rows[0].keys())
    col_widths = {
        c: max(len(str(c)), max(len(str(r.get(c, ""))) for r in rows))
        for c in cols
    }
    sep    = "+" + "+".join("-" * (col_widths[c] + 2) for c in cols) + "+"
    header = "|" + "|".join(f" {c:<{col_widths[c]}} " for c in cols) + "|"

    print(f"\n        Results for: {db_name}  ({len(rows)} row(s))")
    print("        " + sep)
    print("        " + header)
    print("        " + sep)
    for row in rows[:max_rows]:
        line = "|" + "|".join(f" {str(row.get(c, '')):<{col_widths[c]}} " for c in cols) + "|"
        print("        " + line)
    print("        " + sep)
    if len(rows) > max_rows:
        print(f"        ... and {len(rows) - max_rows} more rows (see QueryOutput.json for full results)")
    print()


# ---------------------------------------------------------------------------
# Main executor
# ---------------------------------------------------------------------------
def execute_plan(plan_file: str = "llm_output.json",
                 output_file: str = "QueryOutput.json") -> None:
    with open(plan_file, "r") as f:
        plan = json.load(f)

    execution_order = plan.get("execution_order", [d["name"] for d in plan["databases"]])
    db_queries      = {d["name"]: d["query"] for d in plan["databases"]}

    results_so_far: dict = {}
    output: dict = {}

    print("\n" + "=" * 60)
    print("  QueryExecutor - running queries in LLM-specified order")
    print("=" * 60 + "\n")

    for db_name in execution_order:
        if db_name not in db_queries:
            print(f"[SKIP]  '{db_name}' is in execution_order but has no query defined.")
            continue

        raw_query     = db_queries[db_name]
        db_type = _detect_db_type(db_name)
        resolved_query = _resolve_placeholders(raw_query, results_so_far, is_mongo=(db_type == "mongo"))
        print(f"[RUN]   {db_name}  ({db_type})")
        print(f"        Query : {resolved_query[:300]}{'...' if len(resolved_query) > 300 else ''}")

        try:
            if db_type == "mongo":
                rows = _run_mongo(resolved_query)
            elif db_type == "sqlserver":
                rows = _run_sql_server(resolved_query)
            else:
                rows = _run_postgres(resolved_query)
            print(f"        Result: {len(rows)} row(s)")
        except Exception as exc:
            print(f"        ERROR : {exc}\n")
            rows = []

        results_so_far[db_name] = rows

        output[db_name] = {
            "database":       db_name,
            "db_type":        db_type,
            "resolved_query": resolved_query,
            "row_count":      len(rows),
            "results":        rows
        }

        # Print result table to screen
        _print_table(db_name, rows)

    # Write combined output
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=4, cls=_Encoder)

    print("=" * 60)
    print(f"  All results saved to: {output_file}")
    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    execute_plan(plan_file="llm_output.json", output_file="QueryOutput.json")
