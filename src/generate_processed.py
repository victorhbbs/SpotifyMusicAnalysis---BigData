import pandas as pd
import numpy as np
from pathlib import Path
import sys

INTERIM = Path("data/interim")
PROCESSED = Path("data/processed")
PROCESSED.mkdir(parents=True, exist_ok=True)

candidatos = ["clean_pt.csv", "clean.csv"]
for nome in candidatos:
    src = INTERIM / nome
    if src.exists():
        df = pd.read_csv(src)
        break
else:
    sys.exit("ERRO: não achei data/interim/clean_pt.csv nem clean.csv. Rode antes: python src\\data_cleaning.py")

main_out = PROCESSED / "processed_pt.csv"
df.to_csv(main_out, index=False)

feat_cols = [c for c in ["dançabilidade","energia","valência","ritmo_bpm","acústica","instrumental","presença_ao_vivo"] if c in df.columns]

if "ano" in df.columns:
    by_year = df.groupby("ano")[feat_cols + (["popularidade"] if "popularidade" in df.columns else [])] \
                .mean(numeric_only=True).reset_index()
    by_year.to_csv(PROCESSED / "metricas_por_ano.csv", index=False)

if "geração" in df.columns:
    by_gen = df.groupby("geração")[feat_cols + (["popularidade"] if "popularidade" in df.columns else [])] \
               .mean(numeric_only=True).reset_index()
    by_gen.to_csv(PROCESSED / "metricas_por_geracao.csv", index=False)

if {"geração", "artista_principal", "popularidade"}.issubset(df.columns):
    top_art = (df.groupby(["geração","artista_principal"])["popularidade"]
                 .mean(numeric_only=True).reset_index()
                 .rename(columns={"artista_principal":"artista"}))

    top_art = (top_art.sort_values(["geração","popularidade"], ascending=[True, False])
                     .groupby("geração").head(15))
    top_art.to_csv(PROCESSED / "top_artistas_por_geracao.csv", index=False)
else:
    pd.DataFrame(columns=["geração","artista","popularidade"]).to_csv(
        PROCESSED / "top_artistas_por_geracao.csv", index=False
    )

print("[OK] Processados salvos em data/processed/")
