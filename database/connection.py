import os
from contextlib import contextmanager
import pyodbc


@contextmanager
def connect(database="RestaurantDB", autocommit=False):
    # Braced ODBC values escape a closing brace by doubling it.
    password = os.environ["MSSQL_SA_PASSWORD"].replace("}", "}}")
    db = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=sql,1433;"
        f"DATABASE={database};UID=sa;PWD={{{password}}};"
        "Encrypt=yes;TrustServerCertificate=yes",
        timeout=6,
        autocommit=autocommit,
    )
    try:
        yield db
        if not autocommit:
            db.commit()
    finally:
        db.close()
