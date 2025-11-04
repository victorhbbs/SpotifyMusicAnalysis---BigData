import pandas as pd
from pathlib import Path
from app.utils.io_safe import read_csv_safe

def main(csv_path: str):
    df = read_csv_safe(csv_path, nrows=2000)
    print(">>> Colunas encontradas:")
    print(list(df.columns))
    print("\n>>> 5 linhas (amostra):")
    print(df.head(5).to_string(index=False))

    candidates = {
        "artist": [c for c in df.columns if str(c).lower() in ("artist","artists","artist_name") or "artist" in str(c).lower()],
        "track":  [c for c in df.columns if str(c).lower() in ("track","track_name","song","title") or "track" in str(c).lower()],
        "year":   [c for c in df.columns if "year" in str(c).lower()],
        "date":   [c for c in df.columns if "date" in str(c).lower()],
        "streams":[c for c in df.columns if any(k in str(c).lower() for k in ["streams","stream","plays","play_count","listens","popularity","count"])]
    }
    print("\n>>> Candidatos por tipo:")
    for k,v in candidates.items():
        print(f"- {k}: {v}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Uso: python -m app.jobs.inspect_tracks <data/raw/tracks.csv>")
        raise SystemExit(1)
    main(sys.argv[1])
