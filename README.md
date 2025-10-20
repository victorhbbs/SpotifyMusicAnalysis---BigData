# 🎵 Big Data — Evolução das Preferências Musicais (Spotify)

Pipeline **ETL → BI** em Python para analisar a evolução de características musicais por **gerações** usando dados públicos obtidos do Kaggle e visualização interativa no **Streamlit**.

> **Importante:** Este repositório **não** redistribui dados de terceiros. Você baixará os dados diretamente do Kaggle (autor original) e os colocará localmente em `data/raw/`.

---

## ✨ O que tem aqui
- **ETL reproducível**: ingestão robusta, limpeza/tradução de colunas para PT-BR, enriquecimento de gênero (via `artists.csv`), agregações por **ano** e **geração**.
- **Dashboard interativo** (Streamlit + Plotly): tendências anuais, comparativos por geração, dispersão *valência × energia*, top artistas por geração e amostra de faixas filtradas.
- **Código organizado** em `src/` + artefatos em `data/interim/` e `data/processed/`.

---

## 📁 Estrutura do projeto
```
.
├── app/
│   └── streamlit_app.py
├── src/
│   ├── data_ingestion.py
│   ├── data_cleaning.py
│   └── generate_processed.py
├── data/
│   ├── raw/        # (coloque aqui tracks.csv, artists.csv)  ← NÃO versionado
│   ├── interim/    # (clean_pt.csv)                          ← NÃO versionado
│   └── processed/  # (processed_pt.csv + tabelas)            ← NÃO versionado
├── .gitignore
└── README.md
```

> Dica: você pode criar arquivos vazios `.gitkeep` dentro de `data/raw`, `data/interim` e `data/processed` para a estrutura de pastas aparecer no GitHub sem subir dados.

---

## 📚 Fonte de dados (Kaggle)
**Dataset:** *Spotify Dataset 1921–2020, 600k+ tracks* — por **Yama Erenay**  
**Página:** https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks

Este dataset foi construído a partir da **API oficial do Spotify** (via Spotipy) e inclui:
- `tracks.csv` — faixas com *audio features* (danceability, energy, valence, tempo, etc.)
- `artists.csv` — metadados de artistas com lista de `genres`
- `dict_artists.json` — dicionário auxiliar de artistas (opcional)

> **Licença/termos**: verifique os termos na página do Kaggle. Não redistribua os arquivos neste repositório.

---

## ⬇️ Como baixar os dados (duas formas)

### Opção A — Download manual (mais simples)
1. Crie uma conta no **Kaggle** e aceite os termos do dataset.
2. Na página do dataset, clique em **Download**.
3. Extraia o `.zip` baixado.
4. Coloque os arquivos **`tracks.csv`** (obrigatório) e **`artists.csv`** (recomendado) em:
   ```
   data/raw/
   ```

### Opção B — Via **Kaggle API** (automatizável)
1. Instale a CLI do Kaggle e configure seu `~/.kaggle/kaggle.json` (token de API).
2. No terminal (raiz do projeto):
   ```bash
   kaggle datasets download -d yamaerenay/spotify-dataset-19212020-600k-tracks -p data/raw
   unzip data/raw/spotify-dataset-19212020-600k-tracks.zip -d data/raw
   ```
3. Confirme se `data/raw/tracks.csv` e `data/raw/artists.csv` existem.

> Se não puder usar a CLI, fique com a **Opção A** (manual).

---

## ▶️ Como executar localmente

1) **Crie o ambiente** e instale dependências (exemplo Windows):
```bash
python -m venv .venv
# PowerShell
.venv\Scripts\Activate.ps1
# ou CMD
.venv\Scripts\activate.bat

pip install pandas numpy plotly streamlit scikit-learn scipy python-dotenv pyjanitor
```

2) **Gere o dataset limpo (PT-BR)**:
```bash
python src/data_cleaning.py
```
Saída esperada: `data/interim/clean_pt.csv`

3) **Crie os artefatos analíticos**:
```bash
python src/generate_processed.py
```
Saídas esperadas (em `data/processed/`):
```
processed_pt.csv
metricas_por_ano.csv
metricas_por_geracao.csv
top_artistas_por_geracao.csv
```

4) **Abra o dashboard**:
```bash
streamlit run app/streamlit_app.py
```
Acesse o navegador em `http://localhost:8501`.

---

## 🧪 O que o código faz (resumo técnico)
- `data_ingestion.py`: leitura **tolerante** de CSV (vários `encoding` e `sep`).
- `data_cleaning.py`: padroniza e traduz colunas, cria `ano` e `geração`, define `artista_principal` e **enriquece `gênero`** via *join* com `artists.csv`. Remove duplicatas e trata outliers simples.
- `generate_processed.py`: produz **tabelas analíticas** por **ano/geração** e **Top artistas**.
- `streamlit_app.py`: UI interativa com filtros e gráficos (Plotly).

---

## 🏷️ Citação / Atribuição
Se você publicar resultados, cite o autor do dataset:

> Erenay, Yama. *Spotify Dataset 1921–2020, 600k+ tracks.* Kaggle. Disponível em: https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks

E, se desejar, inclua uma nota metodológica:

> As análises utilizam dados extraídos da API oficial do Spotify (via Spotipy) e podem refletir limitações do catálogo digital (viés temporal/cobertura).

---

## 🔒 Aviso sobre dados de terceiros
Este repositório **não** inclui cópias dos dados originais. Para usar, faça o download diretamente do **Kaggle** e coloque os arquivos em `data/raw/`. Respeite a licença/termos do dataset.
