import pandas as pd
import numpy as np
from pathlib import Path
from ast import literal_eval
from data_ingestion import load_raw

OUT_INTERIM = Path("data/interim")
OUT_INTERIM.mkdir(parents=True, exist_ok=True)

def _parse_list(cell):
    if isinstance(cell, list):
        return cell
    if pd.isna(cell):
        return []
    s = str(cell).strip()
    try:
        val = literal_eval(s)
        if isinstance(val, list):
            return val
        return [str(val)]
    except Exception:
        
        return [x.strip() for x in s.split(",") if x.strip()]

def basic_clean_pt(df_tracks: pd.DataFrame, df_artists: pd.DataFrame | None = None) -> pd.DataFrame:
    df = df_tracks.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    rename_pt = {
        "track_name": "música",
        "artists": "artista",            
        "artist_name": "artista",
        "album_name": "álbum",
        "popularity": "popularidade",
        "release_date": "data_lanc",
        "duration_ms": "duração_ms",
        "danceability": "dançabilidade",
        "energy": "energia",
        "loudness": "volume",
        "speechiness": "fala",
        "acousticness": "acústica",
        "instrumentalness": "instrumental",
        "liveness": "presença_ao_vivo",
        "valence": "valência",
        "tempo": "ritmo_bpm",
        "mode": "modo",
        "key": "tom",
        "time_signature": "compasso",

        "id_artists": "ids_artistas",
    }
    for k in list(rename_pt.keys()):
        if k not in df.columns and k.replace("_", "") in df.columns:
            rename_pt[k.replace("_", "")] = rename_pt[k]
    df = df.rename(columns={c: rename_pt[c] for c in df.columns if c in rename_pt})

    if "data_lanc" in df.columns:
        df["data_lanc"] = pd.to_datetime(df["data_lanc"], errors="coerce")
        df["ano"] = df["data_lanc"].dt.year

    if "artista" in df.columns:
        artistas_list = df["artista"].apply(_parse_list)
        df["artista_principal"] = artistas_list.apply(lambda xs: xs[0] if xs else np.nan)
    else:
        df["artista_principal"] = np.nan

    genero_por_artista = None
    if df_artists is not None and not df_artists.empty:
        da = df_artists.copy()
        da.columns = [c.strip().lower().replace(" ", "_") for c in da.columns]

        nome_col = "name" if "name" in da.columns else ("artist_name" if "artist_name" in da.columns else None)
        if not nome_col and "artists" in da.columns:
            nome_col = "artists"

        genero_col = "genres" if "genres" in da.columns else ("artist_genres" if "artist_genres" in da.columns else None)

        if nome_col and genero_col:
            da = da[[nome_col, genero_col]].dropna()
            da["__generos_list"] = da[genero_col].apply(_parse_list)

            da["gênero"] = da["__generos_list"].apply(lambda xs: xs[0] if xs else np.nan)
            
            genero_por_artista = (da
                                  .dropna(subset=["gênero"])
                                  .groupby(nome_col)["gênero"]
                                  .agg(lambda s: s.value_counts().index[0])
                                  .reset_index()
                                  .rename(columns={nome_col: "artista_principal"}))

    if genero_por_artista is not None:
        df = df.merge(genero_por_artista, on="artista_principal", how="left")

    if "gênero" not in df.columns:
        df["gênero"] = np.where(df.get("gênero").isna() if "gênero" in df.columns else True, "desconhecido", df.get("gênero", "desconhecido"))

    for col in ["música", "artista_principal", "álbum", "gênero"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    num_cols = [
        "ano","popularidade","dançabilidade","energia","valência","ritmo_bpm",
        "volume","fala","acústica","instrumental","presença_ao_vivo","duração_ms"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "ano" in df.columns:
        df = df[df["ano"].between(1960, 2020)]

    subset = [c for c in ["música","artista_principal","ano"] if c in df.columns]
    if subset:
        df = df.drop_duplicates(subset=subset)

    if "ritmo_bpm" in df.columns:
        df["ritmo_bpm"] = df["ritmo_bpm"].clip(40, 220)
    if "duração_ms" in df.columns:
        df["duração_ms"] = df["duração_ms"].clip(30_000, 600_000)

    if "ano" in df.columns:
        bins   = [1959, 1979, 1999, 2009, 2020]
        labels = ["Geração X (1960–1979)", "Millennials (1980–1999)", "Geração Z inicial (2000–2009)", "Nova Geração (2010–2020)"]
        df["geração"] = pd.cut(df["ano"], bins=bins, labels=labels)

    ordem = [c for c in [
        "música","artista_principal","álbum","gênero","ano","geração","popularidade",
        "dançabilidade","energia","valência","ritmo_bpm","volume","fala",
        "acústica","instrumental","presença_ao_vivo","duração_ms","tom","modo","compasso"
    ] if c in df.columns]
    df = df[ordem]

    return df

if __name__ == "__main__":
    raw_tracks = load_raw("tracks.csv")

    try:
        raw_artists = load_raw("artists.csv")
    except Exception:
        raw_artists = pd.DataFrame()

    clean = basic_clean_pt(raw_tracks, raw_artists)
    out = OUT_INTERIM / "clean_pt.csv"
    clean.to_csv(out, index=False)
    print(f"[OK] Arquivo salvo: {out} | linhas: {len(clean):,}")
