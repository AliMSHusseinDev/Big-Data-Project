from pathlib import Path
from database.connection import connect

if __name__ == "__main__":
    batches = Path(__file__).with_name("database.sql").read_text().split("\nGO")
    with connect("master", autocommit=True) as db:
        for batch in batches:
            if batch.strip():
                db.execute(batch.strip())
        db.execute("SELECT id FROM RestaurantDB.dbo.SimulationControl WHERE id=1").fetchone()
    print("RestaurantDB initialized")
