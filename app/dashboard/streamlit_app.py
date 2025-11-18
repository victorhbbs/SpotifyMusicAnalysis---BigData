import streamlit as st
import pandas as pd
from app.utils.paths import PROCESSED_DATA_DIR

# ========== LOADERS COM CACHE ==========

@st.cache_data
def load_genre_share():
    # Lê o CSV com a participação de gêneros por geração
    path = PROCESSED_DATA_DIR / "genre_share_by_generation.csv"
    df = pd.read_csv(path)
    # Garante que as gerações venham ordenadas
    return df.sort_values("generation")


@st.cache_data
def load_tracks_with_clusters():
    # Lê o CSV com as faixas já enriquecidas (por exemplo, com clusters)
    path = PROCESSED_DATA_DIR / "spotify_with_clusters.csv"
    return pd.read_csv(path)


@st.cache_data
def load_generation_stats():
    # Tenta diferentes nomes de arquivo para as métricas por geração
    candidates = [
        PROCESSED_DATA_DIR / "generation_stats.csv",
        PROCESSED_DATA_DIR / "metricas_por_geracao.csv",
    ]
    for c in candidates:
        if c.exists():
            df = pd.read_csv(c)
            return df
    # Se não encontrar nenhum, retorna None
    return None


@st.cache_data
def load_top_artists_by_gen():
    # Tenta diferentes nomes para o arquivo de top artistas por geração
    candidates = [
        PROCESSED_DATA_DIR / "top_artists_by_gen.csv",
        PROCESSED_DATA_DIR / "top_artistas_por_geracao.csv",
        PROCESSED_DATA_DIR / "artist_trends.csv",
    ]
    for c in candidates:
        if c.exists():
            df = pd.read_csv(c)
            return df
    return None


# ========== MACRO-GÊNERO ==========

def map_macro_genre(raw_genre: str) -> str:
    # Converte o gênero original em um "macro-gênero" padronizado
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
    # Se não cair em nenhuma categoria, classifica como "Other"
    return "Other"


# ========== APP PRINCIPAL ==========

def main():
    # Título principal da aplicação
    st.title("Big Data Spotify – Dashboard")

    # Cria três abas principais no dashboard
    tab_genres, tab_metrics, tab_artists = st.tabs(
        ["Gêneros x Gerações", "Métricas por geração", "Top artistas por geração"]
    )

    # ---- ABA 1: GÊNEROS X GERAÇÕES ----
    with tab_genres:
        st.header("Composição de gêneros por geração")

        # Carrega a tabela de participação de gêneros por geração
        st.markdown("#### Evolução dos gêneros selecionados (por ano)")

        # Carrega o dataset detalhado com faixas
        df_tracks = load_tracks_with_clusters()
        df_tracks["year"] = df_tracks["year"].astype(int)
        df_tracks["macro_genre"] = df_tracks["genre"].apply(map_macro_genre)

        # Agrupa por ano + macro_gênero e calcula percentual por ano
        year_counts = (
            df_tracks
            .groupby(["year", "macro_genre"])
            .size()
            .reset_index(name="count")
        )

        year_counts["total"] = year_counts.groupby("year")["count"].transform("sum")
        year_counts["pct"] = (year_counts["count"] / year_counts["total"]) * 100

        # Tabela pivot: linhas = ano, colunas = macro_gêneros, valores = %
        pivot_year = (
            year_counts
            .pivot(index="year", columns="macro_genre", values="pct")
            .fillna(0)
            .sort_index()
        )

        pivot_year.index = pivot_year.index.astype(str)

        selected = st.multiselect(
            "Escolha os gêneros:",
            options=[c for c in pivot_year.columns],
            default=[g for g in ["Pop", "Rock", "Rap", "Electronic", "Other"] if g in pivot_year.columns],
        )

        if selected:
            chart_data = pivot_year[selected]
            st.line_chart(chart_data)

        st.divider()
        st.markdown("#### Detalhe por ano para um macro-gênero")

        # Carrega faixas com clusters para detalhar um macro-gênero
        df_tracks = load_tracks_with_clusters()
        df_tracks["year"] = df_tracks["year"].astype(int)
        # Cria coluna de macro-gênero a partir do gênero original
        df_tracks["macro_genre"] = df_tracks["genre"].apply(map_macro_genre)

        # Lista de macro-gêneros disponíveis
        genres_available = sorted(df_tracks["macro_genre"].unique().tolist())
        genre_choice = st.selectbox("Selecione um macro-gênero", genres_available)

        # Filtra apenas as faixas daquele macro-gênero
        sub = df_tracks[df_tracks["macro_genre"] == genre_choice]
        if sub.empty:
            st.info(f"Nenhuma faixa encontrada para o gênero '{genre_choice}'.")
        else:
            # Conta quantas faixas por ano naquele macro-gênero
            year_counts = (
                sub.groupby("year")
                   .size()
                   .reset_index(name="n_tracks")
                   .sort_values("year")
                   .set_index("year")
            )

            # Gráfico de barras com o número de faixas por ano
            st.bar_chart(year_counts["n_tracks"], height=250)

            # Tabela com as faixas mais populares daquele macro-gênero
            st.markdown("Faixas mais populares nesse gênero:")
            st.dataframe(
                sub[["track_name", "artist", "year", "genre", "pop"]]
                .sort_values("pop", ascending=False)
                .head(30),
                use_container_width=True,
            )

    # ---- ABA 2: MÉTRICAS POR GERAÇÃO ----
    with tab_metrics:
        st.header("Métricas por geração")

        df_stats = load_generation_stats()
        if df_stats is None:
            st.warning("Nenhum arquivo de métricas por geração encontrado em data/processed.")
        else:
            st.markdown("Tabela de métricas por geração:")
            st.dataframe(df_stats, use_container_width=True)

            # Tenta descobrir qual coluna representa a geração
            gen_col = None
            for cand in ["generation", "geracao", "decade"]:
                if cand in df_stats.columns:
                    gen_col = cand
                    break

            if gen_col is None:
                st.info("Não foi possível identificar a coluna de geração (ex: 'generation').")
            else:
                # Todas as colunas numéricas que podem ser exibidas como métricas
                metric_cols = [c for c in df_stats.columns if c != gen_col]

                st.markdown("#### Selecionar métricas para ver evolução por geração")
                selected_metrics = st.multiselect(
                    "Métricas:",
                    options=metric_cols,
                    default=metric_cols[: min(3, len(metric_cols))],
                )

                if selected_metrics:
                    # Ordena por geração e plota evolução das métricas selecionadas
                    chart_df = df_stats.sort_values(gen_col).set_index(gen_col)[selected_metrics]
                    st.line_chart(chart_df)

    # ---- ABA 3: TOP ARTISTAS POR GERAÇÃO ----
    with tab_artists:
        st.header("Top artistas por geração")

        df_top = load_top_artists_by_gen()
        if df_top is None:
            st.warning("Nenhum arquivo de 'top artistas por geração' encontrado em data/processed.")
        else:
            st.markdown("Tabela bruta de top artistas por geração:")
            st.dataframe(df_top.head(200), use_container_width=True)

            # Detecta automaticamente as colunas de geração, artista e score
            gen_col = None
            artist_col = None
            score_col = None

            # coluna de geração
            for cand in ["generation", "geracao", "decade"]:
                if cand in df_top.columns:
                    gen_col = cand
                    break

            # coluna de artista
            for cand in ["artist", "artists", "nome_artista"]:
                if cand in df_top.columns:
                    artist_col = cand
                    break

            # coluna de score/popularidade
            for cand in ["mean_popularity", "popularity", "score", "media_pop"]:
                if cand in df_top.columns:
                    score_col = cand
                    break

            if not all([gen_col, artist_col, score_col]):
                st.info(
                    "Não foi possível identificar automaticamente as colunas de geração, artista e score.\n"
                    f"Colunas disponíveis: {df_top.columns.tolist()}"
                )
            else:
                # Usuário escolhe a geração que quer analisar
                generations = sorted(df_top[gen_col].unique().tolist())
                gen_choice = st.selectbox("Geração / década", generations)

                max_n = 30
                top_n = st.slider("Quantos artistas exibir", min_value=5, max_value=max_n, value=10, step=1)

                # Filtra artistas daquela geração e pega os top N pelo score
                sub = (
                    df_top[df_top[gen_col] == gen_choice]
                    .sort_values(score_col, ascending=False)
                    .head(top_n)
                )

                if sub.empty:
                    st.info("Nenhum dado para essa geração.")
                else:
                    st.markdown(f"#### Top {len(sub)} artistas em {gen_choice}")
                    # Gráfico de barras com score por artista
                    chart_df = sub[[artist_col, score_col]].set_index(artist_col)
                    st.bar_chart(chart_df)

                    st.markdown("Tabela detalhada:")
                    st.dataframe(sub, use_container_width=True)


if __name__ == "__main__":
    # Permite rodar o dashboard com "python streamlit_app.py" (via `streamlit run` ou similar)
    main()
