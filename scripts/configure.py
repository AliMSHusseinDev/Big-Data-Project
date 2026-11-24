"""Generate a local-only SQL login password once, before Compose reads .env."""

import secrets
from pathlib import Path

if __name__ == "__main__":
    target = Path(__file__).resolve().parents[1] / ".env"
    if not target.exists():
        with target.open("x", encoding="ascii") as stream:
            stream.write("MSSQL_SA_PASSWORD=K9a!" + secrets.token_hex(20) + "\n")
        target.chmod(0o600)
