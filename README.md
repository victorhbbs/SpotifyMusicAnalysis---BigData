# Big Data Spotify Project

Este projeto analisa a evolução das preferências musicais entre diferentes gerações, utilizando dados históricos de 1921 a 2020.

## Estrutura

- **app/jobs/** – scripts de processamento (ETL e MapReduce)
- **app/dashboard/** – geração de gráficos com Matplotlib
- **app/utils/** – funções auxiliares (logs, paths, etc.)
- **data/raw/** – dados brutos (não versionados)
- **data/processed/** – saídas processadas (intermediárias e finais)

## Pipeline

1. **ETL** – leitura e padronização do dataset (`extract_tracks_to_parquet.py`)
2. **MapReduce** – agregações por década e artista (`build_processed.py`)
3. **Dashboard** – visualização local dos resultados (`app_dashboard.py`)

## Como executar

Clone o repositório e crie um ambiente virtual:

```bash
git clone https://github.com/SEU_USUARIO/bigdata-spotify.git
cd bigdata-spotify
python -m venv .venv
.\.venv\Scripts\activate   # (Windows)

Instale as dependências:

pip install -r requirements.txt

Adicione o dataset em data/raw/tracks.csv e execute:

python -m app.jobs.extract_tracks_to_parquet "data/raw/tracks.csv" "data/processed"
python -m app.jobs.build_processed
python -m app.dashboard.app_dashboard