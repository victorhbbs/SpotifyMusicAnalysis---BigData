import json
import subprocess
import sys
from pathlib import Path
import pandas as pd

from app.utils.paths import PROCESSED # pasta data/processed
from app.utils.log import log # função de loggin simples

def _read_tracks_dataframe() -> pd.DataFrame:
    """Lê data/processed/tracks.parquet (preferência) ou tracks.csv (fallback)."""
    parquet = PROCESSED / "tracks.parquet"
    csv = PROCESSED / "tracks.csv"
    if parquet.exists():
        log(f"Lendo {parquet.name}")
        return pd.read_parquet(parquet)
    if csv.exists():
        log(f"Lendo {csv.name}")
        return pd.read_csv(csv)
    raise SystemExit("Nenhum 'tracks.parquet' ou 'tracks.csv' encontrado em data/processed/.")

def parquet_to_compact_csv(df: pd.DataFrame, cols, out_csv: Path):
    # verifica se todas as colunas existem
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas ausentes em tracks: {missing}")
    slim = df[cols].copy()

    # normaliza cada coluna: strings e numéricos
    for c in slim.columns:
        if str(slim[c].dtype) in ("object", "string"):
            # garante tipo string e preenche nulo como "unknown"
            slim[c] = slim[c].fillna("unknown").astype(str)
        else:
            # converte para numérico, preenche nulo com 0
            slim[c] = pd.to_numeric(slim[c], errors="coerce").fillna(0).astype("int64")

    # garante que a pasta de saída existe
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    # grava CSV compacto
    slim.to_csv(out_csv, index=False)
    log(f"CSV compacto gerado: {out_csv.name} (linhas={len(slim)})")

def run_mrjob(script_relpath: str, input_csv: Path, output_jsonl: Path):
    """Roda mrjob em modo local/inline SEM Hadoop, escreve JSONL único."""

    # prepara pasta temporária para saída do mrjob
    outdir = output_jsonl.with_suffix("")
    if outdir.exists():
        for p in outdir.glob("*"):
            try:
                p.unlink()
            except Exception:
                pass
        try:
            outdir.rmdir()
        except Exception:
            pass

    # garante que a pasta existe
    outdir.mkdir(parents=True, exist_ok=True)

    # monta o comando para rodar o mrjob em modo inline
    cmd = [
        sys.executable, script_relpath, str(input_csv),
        "--runner=inline",
        "--no-conf",
        "--output-dir", str(outdir)
    ]
    log("Executando: " + " ".join(cmd))

    # roda o comando
    subprocess.check_call(cmd)

    # concatena todos os arquivos part-* em um JSONL único
    parts = sorted(outdir.glob("part-*"))
    with open(output_jsonl, "w", encoding="utf-8") as f_out:
        for part in parts:
            with open(part, "r", encoding="utf-8") as f_in:
                for line in f_in:
                    if line.strip():
                        f_out.write(line)
    log(f"Saída MR: {output_jsonl.name}")

def jsonl_to_csv(jsonl_path: Path, out_csv: Path, columns: list):
    # lê JSONL em DataFrame, ordena e grava CSV
    rows = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            rows.append(json.loads(line))
    df = pd.DataFrame(rows)

    # se houver coluna total_streams, ordena do maior pro menor
    if "total_streams" in df.columns:
        df = df.sort_values("total_streams", ascending=False)

    # se uma ordem específica de colunas foi pedida, reordena
    if columns:
        df = df.reindex(columns=columns)
    df.to_csv(out_csv, index=False)
    log(f"CSV final: {out_csv.name} (linhas={len(df)})")

def main():
    # 1) Lê o dataset processado principal
    df = _read_tracks_dataframe()

    # ========== PIPELINE GERAÇÃO × PLATAFORMA ==========

    # 2) Gera CSV compacto para MRJob de geração x plataforma
    genplat_csv = PROCESSED / "mr_input_genplat.csv"
    parquet_to_compact_csv(df, cols=["generation", "platform", "streams"], out_csv=genplat_csv)

    # 3) Roda o MapReduce de estatísticas por geração/plataforma
    genplat_jsonl = PROCESSED / "genplat.jsonl"
    run_mrjob("app/jobs/mapreduce_generation_stats.py", genplat_csv, genplat_jsonl)

    # 4) Converte saída JSONL em CSV final para o dashboard
    generation_stats_csv = PROCESSED / "generation_stats.csv"
    jsonl_to_csv(
        genplat_jsonl,
        generation_stats_csv,
        columns=["generation", "platform", "total_streams"]
    )

    # ========== PIPELINE GERAÇÃO × ARTISTA ==========

    # 5) Gera CSV compacto para MRJob de geração x artista
    genartist_csv = PROCESSED / "mr_input_genartist.csv"
    parquet_to_compact_csv(df, cols=["generation", "artist", "streams"], out_csv=genartist_csv)

    # 6) Roda o MapReduce de estatísticas por geração/artista
    genartist_jsonl = PROCESSED / "genartist.jsonl"
    run_mrjob("app/jobs/mapreduce_artist_trends.py", genartist_csv, genartist_jsonl)

    # 7) Converte saída JSONL em CSV de tendências por artista
    artist_trends_csv = PROCESSED / "artist_trends.csv"
    jsonl_to_csv(
        genartist_jsonl,
        artist_trends_csv,
        columns=["generation", "artist", "total_streams"]
    )

    # 8) Deriva um CSV com apenas os TOP 10 artistas por geração
    top_by_gen = (
        pd.read_csv(artist_trends_csv)
          .groupby("generation", group_keys=False)
          .head(10)
    )
    top_by_gen.to_csv(PROCESSED / "top_artists_by_gen.csv", index=False)
    log("Build finalizado com sucesso.")

if __name__ == "__main__":
    main()
