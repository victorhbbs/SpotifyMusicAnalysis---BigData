import pandas as pd
from pathlib import Path
from app.utils.paths import PROCESSED_DATA_DIR, REPORTS_DIR

# converte um ano em geração/década
def year_to_generation(year: int) -> str:
    """
    Converte um ano (ex: 1995) em uma 'geração/década' em string (ex: '1990s').
    """
    try:
        y = int(year)
    except (TypeError, ValueError):
        return "Unknown"

    if y < 1920:
        return "Before 1920"

    decade = (y // 10) * 10
    return f"{decade}s"

# converte um gênero bruto em um macro-gênero padronizado
def map_macro_genre(raw_genre: str) -> str:
    if not isinstance(raw_genre, str):
        return "Other"

    g = raw_genre.lower()

    if "pop" in g:
        return "Pop"
    if "rock" in g:
        return "Rock"
    if "hip hop" in g or "rap" in g or "trap" in g:
        return "Rap"
    if "funk" in g:
        return "Funk"
    if ("electro" in g or "house" in g or "techno" in g or
        "edm" in g or "dance" in g):
        return "Electronic"

    return "Other"

# função principal do job
def main():
    # lê o dataset processado com clusters
    file = PROCESSED_DATA_DIR / "spotify_with_clusters.csv"
    if not file.exists():
        raise SystemExit(f"Arquivo não encontrado: {file}")

    print(f"Lendo: {file}")
    df = pd.read_csv(file)

    # verifica se as colunas necessárias existem
    required = {"genre", "year"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Colunas ausentes no CSV: {missing}")

    # prepara dados para cálculo de participação por geração
    df["year"] = df["year"].astype(int)
    df["generation"] = df["year"].apply(year_to_generation)

    df["macro_genre"] = df["genre"].apply(map_macro_genre)

    counts = (
        df.groupby(["generation", "macro_genre"])
          .size()
          .reset_index(name="count")
    )

    # calcula porcentagens
    counts["total"] = counts.groupby("generation")["count"].transform("sum")
    counts["pct"] = (counts["count"] / counts["total"]) * 100

    # cria tabela pivot com gerações como índice e macro-gêneros como colunas
    pivot = (
        counts
        .pivot(index="generation", columns="macro_genre", values="pct")
        .fillna(0)
        .sort_index()
    )

    # garante que a pasta de saída existe
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORTS_DIR / "genre_share_by_generation.csv"
    pivot.to_csv(out, float_format="%.2f")

    print(f"Gerado: {out}")


if __name__ == "__main__":
    main()
