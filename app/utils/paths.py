from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
RAW = DATA / "raw"
PROCESSED = DATA / "processed"

def ensure_dirs():
    PROCESSED.mkdir(parents=True, exist_ok=True)
