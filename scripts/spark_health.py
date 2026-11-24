from pathlib import Path
import time
from urllib.request import urlopen

if __name__ == "__main__":
    live = Path("/tmp/kitchen-stream-live")
    with urlopen("http://localhost:4040/api/v1/applications", timeout=3) as response:
        ok = response.status == 200 and live.exists() and time.time() - live.stat().st_mtime < 30
    raise SystemExit(0 if ok else 1)
