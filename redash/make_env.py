"""Create redash/.env with random secrets (run once): python redash/make_env.py"""
import secrets
from pathlib import Path

target = Path(__file__).with_name(".env")
if target.exists():
    raise SystemExit(f"{target} already exists - delete it first if you really want new secrets.")

db_password = secrets.token_hex(16)
lines = [
    f"POSTGRES_PASSWORD={db_password}",
    f"REDASH_DATABASE_URL=postgresql://postgres:{db_password}@postgres/postgres",
    "REDASH_REDIS_URL=redis://redis:6379/0",
    f"REDASH_COOKIE_SECRET={secrets.token_hex(32)}",
    f"REDASH_SECRET_KEY={secrets.token_hex(32)}",
    "REDASH_LOG_LEVEL=INFO",
]
target.write_text("\n".join(lines) + "\n")
print("Wrote", target)
