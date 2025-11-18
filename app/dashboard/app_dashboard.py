import pandas as pd
import matplotlib.pyplot as plt
from app.utils.paths import REPORTS_DIR, FIGURES_DIR
from pathlib import Path
import sys

# Define caminhos para as pastas do projeto (data/processed)
PROCESSED = Path(__file__).resolve().parents[2] / "data" / "processed"
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Garante que a pasta para salvar figuras existe
FIGURES_DIR = PROCESSED_DATA_DIR / "figures"
FIGURES_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# FUNÇÃO: Carrega o CSV de participação de gêneros por geração
# =========================
def load_genre_share():
    csv_path = PROCESSED_DATA_DIR / "genre_share_by_generation.csv"
    df = pd.read_csv(csv_path)
    return df.sort_values("generation")  # Mantém as gerações em ordem


# =========================
# FUNÇÃO: Ordena décadas corretamente (1920s, 1930s, ...)
# =========================
def _order_decades(values):
    def to_int(v):
        try:
            # remove o "s" final → "1990s" vira "1990", converte para inteiro
            return int(str(v).replace("s",""))
        except Exception:
            return 9999   # fallback para itens inválidos
    return sorted(values, key=to_int)


# =========================
# FUNÇÃO: Limpa colunas numéricas antes de plotar
# =========================
def _clean_numeric_series(s: pd.Series) -> pd.Series:
    # Se já for numérico, retorna direto
    if s.dtype.kind in ("i","u","f"):
        return s
    # Remove qualquer caractere não numérico (ex: vírgulas, letras)
    s = s.astype(str).str.replace(r"[^\d]", "", regex=True)
    return pd.to_numeric(s, errors="coerce").fillna(0)


# =========================
# GRÁFICO 1: Streams por Década × Plataforma
# =========================
def plot_generation_platform():
    path = PROCESSED / "generation_stats.csv"
    if not path.exists():
        raise SystemExit(f"Arquivo não encontrado: {path}")
    df = pd.read_csv(path)

    # Verifica se as colunas necessárias existem
    expected = {"generation","platform","total_streams"}
    missing = expected - set(df.columns)
    if missing:
        raise SystemExit(f"{path.name} faltando colunas: {missing}. Colunas encontradas: {list(df.columns)}")

    # Limpa coluna numeric
    df["total_streams"] = _clean_numeric_series(df["total_streams"])

    # Ordena décadas e mantém apenas décadas válidas
    decades = _order_decades(df["generation"].unique().tolist())
    df = df[df["generation"].isin(decades)]

    # Pivot table → linhas = geração; colunas = plataformas; valores = streams
    pivot = df.pivot_table(
        index="generation",
        columns="platform",
        values="total_streams",
        aggfunc="sum"
    ).fillna(0)

    # Reordena a tabela pela ordem das décadas
    pivot = pivot.reindex(decades)
    pivot = pivot.apply(_clean_numeric_series)

    # Caso o dataframe final esteja vazio
    if pivot.shape[0] == 0 or pivot.shape[1] == 0:
        print("[dashboard] generation_stats.csv não gerou dados suficientes para plotar.")
        print("[dashboard] Amostra do CSV:")
        print(df.head(10).to_string(index=False))
        return

    # Aviso caso o total seja 0 (potencial erro no CSV)
    if (pivot.sum().sum() == 0):
        print("[dashboard] Aviso: total_streams somou 0 após limpeza. Verifique separadores no CSV.")

    # Plot do gráfico
    ax = pivot.plot(figsize=(11,6))
    ax.set_title("Streams por Década × Plataforma (1920s–2020s)")
    ax.set_xlabel("Década")
    ax.set_ylabel("Streams totais")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# =========================
# GRÁFICO 2: Top Artistas por Década
# =========================
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

    # Gera um gráfico por década
    for gen in decades:
        sub = (df[df["generation"] == gen]
               .sort_values("total_streams", ascending=False)
               .head(top_n))
        if sub.empty:
            continue

        any_plotted = True
        plt.figure(figsize=(10,6))
        plt.barh(sub["artist"], sub["total_streams"])  # gráfico horizontal
        plt.title(f"Top {top_n} Artistas — {gen}")
        plt.xlabel("Streams totais")
        plt.ylabel("Artista")
        plt.gca().invert_yaxis()  # artista mais popular no topo
        plt.tight_layout()
        plt.show()

    if not any_plotted:
        print("[dashboard] Nenhuma década possui dados para top artistas. Veja amostra:")
        print(df.head(10).to_string(index=False))


# =========================
# GRÁFICO 3: Composição de Gêneros por Geração (stacked)
# =========================
def plot_genre_share_stacked():
    df = load_genre_share()
    df_plot = df.set_index("generation")

    ax = df_plot.plot(kind="bar", stacked=True, figsize=(12, 6))
    ax.set_ylabel("Participação na geração (%)")
    ax.set_xlabel("Geração / Década")
    ax.set_title("Composição de gêneros por geração")
    plt.tight_layout()

    # Salva o gráfico como PNG
    out_png = FIGURES_DIR / "genre_share_stacked.png"
    plt.savefig(out_png, dpi=120)
    plt.close()
    print(f"Gráfico salvo em: {out_png}")


# =========================
# GRÁFICO 4: Tendência de um gênero específico ao longo das gerações
# =========================
def plot_genre_trend(genre: str):
    df = load_genre_share()
    if genre not in df.columns:
        print(f"Gênero {genre} não encontrado nas colunas do CSV.")
        return

    ax = df.plot(
        x="generation",
        y=genre,
        kind="line",
        marker="o",
        figsize=(10, 4),
    )
    ax.set_ylabel("Participação na geração (%)")
    ax.set_xlabel("Geração / Década")
    ax.set_title(f"Evolução do gênero {genre} por geração")
    plt.xticks(rotation=45)
    plt.tight_layout()

    # salva o gráfico em PNG
    out_png = FIGURES_DIR / f"genre_trend_{genre.lower()}.png"
    plt.savefig(out_png, dpi=120)
    plt.close()
    print(f"Gráfico salvo: {out_png}")


# =========================
# PONTO DE ENTRADA
# =========================
if __name__ == "__main__":
    # Executa todos os gráficos principais
    plot_generation_platform()
    plot_top_artists_by_generation()
    plot_genre_share_stacked()

    # Gera gráficos individuais de tendência por gênero macro
    for g in ["Pop", "Rock", "Rap", "Funk", "Electronic", "Other"]:
        plot_genre_trend(g)
