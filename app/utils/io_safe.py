import pandas as pd
from pathlib import Path
from .log import log

def read_csv_safe(path, **kwargs) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {p}")

    tried = []
    for sep in [",", ";", "\t"]:
        for enc in ["utf-8", "latin-1"]:
            try:
                df = pd.read_csv(p, sep=sep, encoding=enc, **kwargs)
                if df.shape[1] >= 2: 
                    log(f"CSV lido com sep='{sep}', encoding='{enc}', linhas={len(df)}, cols={len(df.columns)}")
                    return df
            except Exception as e:
                tried.append((sep, enc, str(e)[:80]))
    raise ValueError(f"Falha ao ler CSV. Tentativas: {tried[:3]} ...")

def write_parquet_safe(df, path: str):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(p, index=False)
    except Exception as e:
        
        csv_fallback = p.with_suffix(".csv")
        log(f"Falha ao escrever Parquet ({e}). Fazendo fallback para CSV: {csv_fallback.name}")
        df.to_csv(csv_fallback, index=False)
    else:
        log(f"Parquet gravado: {p}")
