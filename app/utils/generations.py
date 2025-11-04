import pandas as pd

def _to_int_or_none(v):
    try:
        return int(v)
    except Exception:
        return None

def assign_generation_decade(row, year_col="year"):
    y = _to_int_or_none(row.get(year_col))
    if y is None:
        return "unknown"

    decade = (y // 10) * 10

    if decade < 1920:
        decade = 1920
    if decade > 2020:
        decade = 2020
    return f"{decade}s"

def add_generation_col(df: pd.DataFrame, year_col="year", out_col="generation", mode="decade"):
    if year_col not in df.columns:
        for alt in ["release_year", "year_released", "Year"]:
            if alt in df.columns:
                year_col = alt
                break

    if mode == "decade":
        df[out_col] = df.apply(assign_generation_decade, axis=1, year_col=year_col)
    else:
        
        df[out_col] = "unknown"
    return df
