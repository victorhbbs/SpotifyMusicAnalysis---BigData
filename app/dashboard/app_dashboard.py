import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import sys

PROCESSED = Path(__file__).resolve().parents[2] / "data" / "processed"

def _order_decades(values):
    def to_int(v):
        try:
            return int(str(v).replace("s",""))
        except Exception:
            return 9999
    return sorted(values, key=to_int)

def _clean_numeric_series(s: pd.Series) -> pd.Series:
    if s.dtype.kind in ("i","u","f"):
        return s
    s = s.astype(str).str.replace(r"[^\d]", "", regex=True)
    return pd.to_numeric(s, errors="coerce").fillna(0)

def plot_generation_platform():
    path = PROCESSED / "generation_stats.csv"
    if not path.exists():
        raise SystemExit(f"Arquivo não encontrado: {path}")
    df = pd.read_csv(path)

    expected = {"generation","platform","total_streams"}
    missing = expected - set(df.columns)
    if missing:
        raise SystemExit(f"{path.name} faltando colunas: {missing}. Colunas encontradas: {list(df.columns)}")

    df["total_streams"] = _clean_numeric_series(df["total_streams"])

    decades = _order_decades(df["generation"].unique().tolist())
    df = df[df["generation"].isin(decades)]

    pivot = df.pivot_table(index="generation", columns="platform",
                           values="total_streams", aggfunc="sum").fillna(0)

    pivot = pivot.reindex(decades)

    pivot = pivot.apply(_clean_numeric_series)

    if pivot.shape[0] == 0 or pivot.shape[1] == 0:
        print("[dashboard] generation_stats.csv não gerou dados suficientes para plotar.")
        print("[dashboard] Amostra do CSV:")
        print(df.head(10).to_string(index=False))
        return

    if (pivot.sum().sum() == 0):
        print("[dashboard] Aviso: total_streams somou 0 após limpeza. Verifique separadores no CSV.")

    ax = pivot.plot(figsize=(11,6))
    ax.set_title("Streams por Década × Plataforma (1920s–2020s)")
    ax.set_xlabel("Década")
    ax.set_ylabel("Streams totais")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_top_artists_by_generation(top_n=10):
    path = PROCESSED / "top_artists_by_gen.csv"
    if not path.exists():
        raise SystemExit(f"Arquivo não encontrado: {path}")
    df = pd.read_csv(path)

    expected = {"generation","artist","total_streams"}
    missing = expected - set(df.columns)
    if missing:
        raise SystemExit(f"{path.name} faltando colunas: {missing}. Colunas: {list(df.columns)}")

    df["total_streams"] = _clean_numeric_series(df["total_streams"])

    decades = _order_decades(df["generation"].unique().tolist())
    any_plotted = False
    for gen in decades:
        sub = (df[df["generation"] == gen]
               .sort_values("total_streams", ascending=False)
               .head(top_n))
        if sub.empty:
            continue
        any_plotted = True
        plt.figure(figsize=(10,6))
        plt.barh(sub["artist"], sub["total_streams"])
        plt.title(f"Top {top_n} Artistas — {gen}")
        plt.xlabel("Streams totais")
        plt.ylabel("Artista")
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plt.show()

    if not any_plotted:
        print("[dashboard] Nenhuma década possui dados para top artistas. Veja amostra:")
        print(df.head(10).to_string(index=False))

if __name__ == "__main__":
    plot_generation_platform()
    plot_top_artists_by_generation()
