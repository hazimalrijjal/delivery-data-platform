"""Print a fingerprint (row count + hash of all values) of every model, to prove a refactor changed no numbers.

    python scripts/checksum_marts.py [warehouse/delivery.duckdb]
"""
import hashlib
import sys

import duckdb

db = sys.argv[1] if len(sys.argv) > 1 else "warehouse/delivery.duckdb"
con = duckdb.connect(db, read_only=True)
tables = con.execute("select table_schema, table_name from information_schema.tables "
                     "where table_schema in ('staging','marts') order by 1,2").fetchall()
for schema, name in tables:
    cols = [r[0] for r in con.execute(f"select column_name from information_schema.columns "
                                      f"where table_schema='{schema}' and table_name='{name}' order by ordinal_position").fetchall()]
    rows = con.execute(f"select * from {schema}.{name} order by {', '.join(str(i + 1) for i in range(len(cols)))}").fetchall()
    h = hashlib.md5(repr(rows).encode()).hexdigest()[:12]
    print(f"{schema}.{name:24} rows={len(rows):>7}  cols={len(cols):>2}  hash={h}")
