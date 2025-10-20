import pandas as pd
from pathlib import Path

RAW = Path("data/raw")

def load_raw(csv_name: str) -> pd.DataFrame:
    path = RAW / csv_name
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    seps = [None, ",", ";", "\t"]
    last_err = None
    for enc in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(path, encoding=enc, sep=sep, engine="python")
                print(f"[OK] Lido com encoding={enc} sep={'inferido' if sep is None else repr(sep)}")
                return df
            except Exception as e:
                last_err = e
                continue
    raise RuntimeError(f"Falha ao ler {path}: {last_err}")

if __name__ == "__main__":
    df = load_raw("tracks.csv")
    print(df.head())
    print(df.columns.tolist())
    print(df.info())
