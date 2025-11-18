import pandas as pd

def _to_int_or_none(v):
    try:
        return int(v)
    except Exception:
        return None

# atribui a geração baseada na década do ano
def assign_generation_decade(row, year_col="year"):
    # obtém o ano como inteiro
    y = _to_int_or_none(row.get(year_col))
    if y is None:
        return "unknown"

    # calcula a década
    decade = (y // 10) * 10

    # limita a década entre 1920 e 2020
    if decade < 1920:
        decade = 1920
    if decade > 2020:
        decade = 2020
    return f"{decade}s"

def add_generation_col(df: pd.DataFrame, year_col="year", out_col="generation", mode="decade"):
    # Verifica se a coluna de ano existe; se não existir, tenta nomes alternativos
    if year_col not in df.columns:
        for alt in ["release_year", "year_released", "Year"]:
            if alt in df.columns:
                year_col = alt
                break

    # adiciona a coluna de geração conforme o modo especificado
    if mode == "decade":
        df[out_col] = df.apply(assign_generation_decade, axis=1, year_col=year_col)
    else:
    # se o modo não for reconhecido, preenche com "unknown"
        df[out_col] = "unknown"
    return df
