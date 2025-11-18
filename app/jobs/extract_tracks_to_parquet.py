import sys, json, re, ast
import pandas as pd
from pathlib import Path
from app.utils.paths import RAW, PROCESSED, ensure_dirs
from app.utils.io_safe import read_csv_safe, write_parquet_safe
from app.utils.log import log
from app.utils.generations import add_generation_col

# Colunas mínimas que queremos garantir no dataset final
REQUIRED_COLS = ["artist", "track_name", "year", "streams"]

def _parse_artists_first(value):
    if pd.isna(value):
        return None
    s = str(value).strip()

    # tenta interpretar como lista literal
    try:
        val = ast.literal_eval(s)
        if isinstance(val, list) and len(val) > 0:
            return str(val[0]).strip()
        if isinstance(val, str):
            return val.strip()
    except Exception:
        pass

    # tenta extrair o primeiro artista de strings comuns
    s = s.strip("[]")
    if "," in s:
        s = s.split(",")[0]
    return s.strip(" '\"")

# extrai o ano de uma string de data de lançamento
def _extract_year_from_release_date(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.extract(r"(19\d{2}|20\d{2})", expand=False)
    return pd.to_numeric(s, errors="coerce").astype("Int64")

# normaliza colunas do DataFrame para o formato desejado
def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # mapeia nomes de colunas para minúsculas e sem espaços
    cols = {c.lower().strip(): c for c in df.columns}
    
    # processa coluna de artista
    if "artists" in cols and "artist" not in df.columns:
        df["artist"] = df[cols["artists"]].apply(_parse_artists_first)
    elif "artist" in cols:
        df["artist"] = df[cols["artist"]]
    else:
        df["artist"] = None

    # processa coluna de nome da faixa
    if "name" in cols:
        df["track_name"] = df[cols["name"]]
    elif "track" in cols or "track_name" in cols or "title" in cols:
        for k in ("track_name", "track", "title"):
            if k in cols:
                df["track_name"] = df[cols[k]]
                break
    else:
        df["track_name"] = None

    # processa coluna de ano
    if "release_date" in cols:
        df["year"] = _extract_year_from_release_date(df[cols["release_date"]])
    else:
        year_col = next((c for c in df.columns if "year" in str(c).lower()), None)
        if year_col:
            df["year"] = pd.to_numeric(df[year_col], errors="coerce").astype("Int64")
        else:
            df["year"] = pd.Series([None] * len(df), dtype="Int64")

    # processa coluna de streams/popularidade
    if "popularity" in cols:
        streams = pd.to_numeric(df[cols["popularity"]], errors="coerce").fillna(0)
        if streams.sum() == 0:
            streams = pd.Series([1] * len(df))
    else:
        streams = pd.Series([1] * len(df))
    df["streams"] = streams.astype("int64", errors="ignore")

    # adiciona coluna de plataforma fixa
    df["platform"] = "spotify"

    # Normaliza artista e track_name para strings legíveis
    df["artist"] = df["artist"].astype("string").fillna("Unknown Artist").str.strip()
    df["track_name"] = df["track_name"].astype("string").fillna("Unknown Track").str.strip()

    # --- FILTRO DE INTERVALO DE ANOS ---
    before = len(df)
    filtered = df[df["year"].between(1921, 2020, inclusive="both")]
    if len(filtered) == 0 and df["year"].notna().any():
        # Se nada caiu no range mas há anos válidos, mantém todos com year não-nulo
        log("Aviso: nenhum registro caiu no range 1921–2020; mantendo todos com YEAR válido.")
        df2 = df[df["year"].notna()].copy()
    else:
        removed = before - len(filtered)
        log(f"Removidas {removed} linhas fora de 1921–2020 ou sem 'year' válido")
        df2 = filtered.copy()

    # --- ADICIONA COLUNA DE GERAÇÃO / DÉCADA ---
    df2 = add_generation_col(df2, year_col="year", out_col="generation", mode="decade")
    df2["generation"] = df2["generation"].fillna("unknown")

    # Garante que todas as REQUIRED_COLS existam
    for col in REQUIRED_COLS:
        if col not in df2.columns:
            df2[col] = None

    return df2

def main(in_csv: str, out_dir: str):
    ensure_dirs()
    df = read_csv_safe(in_csv)
    df = _normalize_columns(df)

    out_parquet = Path(out_dir) / "tracks.parquet"
    write_parquet_safe(df, out_parquet)
    log(f"[TRACKS ETL] ok: linhas={len(df)} | cols={list(df.columns)}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python -m app.jobs.extract_tracks_to_parquet <data/raw/tracks.csv> <data/processed/>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
