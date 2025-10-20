import pandas as pd
import plotly.express as px
import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Evolução das Preferências Musicais", layout="wide")
st.title("Evolução das Preferências Musicais por Gerações (Spotify)")

@st.cache_data
def load_main():
    return pd.read_csv("data/processed/processed_pt.csv")

@st.cache_data
def load_by_year():
    p = Path("data/processed/metricas_por_ano.csv")
    return pd.read_csv(p) if p.exists() else None

@st.cache_data
def load_by_gen():
    p = Path("data/processed/metricas_por_geracao.csv")
    return pd.read_csv(p) if p.exists() else None

@st.cache_data
def load_top_art():
    p = Path("data/processed/top_artistas_por_geracao.csv")
    return pd.read_csv(p) if p.exists() else None

df = load_main()
by_year = load_by_year()
by_gen  = load_by_gen()
top_art = load_top_art()

anos = (int(df["ano"].min()), int(df["ano"].max()))
faixa_anos = st.sidebar.slider("Faixa de anos", anos[0], anos[1], (anos[0], anos[1]))
generos = sorted(df["gênero"].dropna().unique()) if "gênero" in df.columns else []
genero_sel = st.sidebar.multiselect("Gêneros (opcional)", generos)
geracoes = sorted(df["geração"].dropna().unique()) if "geração" in df.columns else []
geracao_sel = st.sidebar.multiselect("Gerações (opcional)", geracoes)

q = df[df["ano"].between(*faixa_anos)]
if genero_sel:
    q = q[q["gênero"].isin(genero_sel)]
if geracao_sel and "geração" in q.columns:
    q = q[q["geração"].isin(geracao_sel)]

st.subheader("📈 Tendências anuais das características")
feat_cols = [c for c in ["dançabilidade","energia","valência","ritmo_bpm"] if c in q.columns]
if feat_cols:
    evol = q.groupby("ano")[feat_cols].mean(numeric_only=True).reset_index()
    st.plotly_chart(px.line(evol, x="ano", y=feat_cols, title="Médias por ano"), use_container_width=True)
else:
    st.info("Não há colunas de áudio suficientes para esta visualização.")

st.subheader("👥 Comparativo por geração (médias)")
if "geração" in q.columns and feat_cols:
    comp = q.groupby("geração")[feat_cols + ["popularidade"]].mean(numeric_only=True).reset_index()
    st.plotly_chart(px.bar(comp, x="geração", y=feat_cols, barmode="group",
                           title="Características médias por geração"), use_container_width=True)
else:
    st.info("Gerações não disponíveis no filtro atual.")

st.subheader("🎭 Emoção vs. intensidade (Valência x Energia)")
if set(["valência","energia"]).issubset(q.columns):
    amostra = q.sample(min(len(q), 30000), random_state=42)
    st.plotly_chart(px.scatter(amostra, x="valência", y="energia",
                               color=("geração" if "geração" in amostra.columns else None),
                               opacity=0.4, title="Dispersão (amostra)"),
                    use_container_width=True)

st.subheader("🏆 Top artistas por geração (popularidade média)")
if top_art is not None:
    if geracao_sel:
        t = top_art[top_art["geração"].isin(geracao_sel)]
    else:
        t = top_art.copy()
    st.dataframe(t.reset_index(drop=True), use_container_width=True)
else:
    st.info("Tabela de top artistas não disponível.")

st.subheader("🎵 Amostra de músicas (após filtros)")
cols = [c for c in ["música","artista","ano","gênero","dançabilidade","energia","valência","ritmo_bpm","popularidade"] if c in q.columns]
st.dataframe(q.sort_values(["ano","popularidade"], ascending=[False, False])[cols].head(300), use_container_width=True)

st.caption("Projeto Acadêmico de Big Data — Evolução das Preferências Musicais (Spotify)")
