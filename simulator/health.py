from database.connection import connect

if __name__ == "__main__":
    with connect() as db:
        row = db.execute("""SELECT id FROM SimulationControl WHERE worker_seen >
                          DATEADD(second,-30,SYSUTCDATETIME())""").fetchone()
        raise SystemExit(0 if row else 1)
