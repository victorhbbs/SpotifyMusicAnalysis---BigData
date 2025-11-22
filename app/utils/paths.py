from pathlib import Path

# descobre o diretório-base do projeto.
BASE_DIR = Path(__file__).resolve().parents[2]

# pasta raiz onde ficam os dados
DATA_DIR = BASE_DIR / "data"

# padrão de pipiline de dados; respectivamente raiz, intermediário e processado
RAW_DATA_DIR      = DATA_DIR / "raw"
INTERIM_DATA_DIR  = DATA_DIR / "interim"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# pasta onde estão salvo os relatórios gerados
REPORTS_DIR = PROCESSED_DATA_DIR

# pasta onde ficam as figuras
FIGURES_DIR = REPORTS_DIR / "figures"

# ==== caminhos específicos para arquivos usados no pipeline ====

# csv de dados brutos de faixas, parquet de faixas processadas e csv trabalhados com clusters posteriormente

TRACKS_RAW_CSV              = RAW_DATA_DIR / "tracks.csv"
TRACKS_PARQUET              = PROCESSED_DATA_DIR / "tracks_parquet"
SPOTIFY_WITH_CLUSTERS_CSV   = PROCESSED_DATA_DIR / "spotify_with_clusters.csv"

RAW = RAW_DATA_DIR
INTERIM = INTERIM_DATA_DIR
PROCESSED = PROCESSED_DATA_DIR

# garante que os diretórios existem
for d in [RAW_DATA_DIR, INTERIM_DATA_DIR, PROCESSED_DATA_DIR, REPORTS_DIR, FIGURES_DIR]:
    d.mkdir(parents=True, exist_ok=True)
