from pathlib import Path

# Raiz do projeto (dois níveis acima de app/utils/paths.py)
BASE_DIR = Path(__file__).resolve().parents[2]

# Pasta principal de dados
DATA_DIR = BASE_DIR / "data"

# Pastas de estágios do pipeline
RAW_DATA_DIR       = DATA_DIR / "raw"
INTERIM_DATA_DIR   = DATA_DIR / "interim"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Pastas de relatórios/figuras
REPORTS_DIR = PROCESSED_DATA_DIR 
FIGURES_DIR = REPORTS_DIR / "figures"

#  ALIASES para compatibilidade com o restante do código
# (vários arquivos importam RAW, INTERIM, PROCESSED)
RAW       = RAW_DATA_DIR
INTERIM   = INTERIM_DATA_DIR
PROCESSED = PROCESSED_DATA_DIR

# Arquivos usados no pipeline
TRACKS_RAW_CSV            = RAW_DATA_DIR / "tracks.csv"
TRACKS_PARQUET            = PROCESSED_DATA_DIR / "tracks.parquet"
SPOTIFY_WITH_CLUSTERS_CSV = PROCESSED_DATA_DIR / "spotify_with_clusters.csv"

# Garante que todas as pastas existem
def ensure_dirs():
    for d in [RAW_DATA_DIR, INTERIM_DATA_DIR, PROCESSED_DATA_DIR, REPORTS_DIR, FIGURES_DIR]:
        d.mkdir(parents=True, exist_ok=True)