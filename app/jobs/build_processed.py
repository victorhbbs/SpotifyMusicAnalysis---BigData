import json
import subprocess
import sys
from pathlib import Path
import pandas as pd

from app.utils.paths import PROCESSED
from app.utils.log import log

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
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas ausentes em tracks: {missing}")
    slim = df[cols].copy()

    for c in slim.columns:
        if str(slim[c].dtype) in ("object", "string"):
            slim[c] = slim[c].fillna("unknown").astype(str)
        else:
            slim[c] = pd.to_numeric(slim[c], errors="coerce").fillna(0).astype("int64")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    slim.to_csv(out_csv, index=False)
    log(f"CSV compacto gerado: {out_csv.name} (linhas={len(slim)})")

def run_mrjob(script_relpath: str, input_csv: Path, output_jsonl: Path):
    """Roda mrjob em modo local/inline SEM Hadoop, escreve JSONL único."""
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

    outdir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, script_relpath, str(input_csv),
        "--runner=inline",
        "--no-conf",
        "--output-dir", str(outdir)
    ]
    log("Executando: " + " ".join(cmd))

    subprocess.check_call(cmd)

    parts = sorted(outdir.glob("part-*"))
    with open(output_jsonl, "w", encoding="utf-8") as f_out:
        for part in parts:
            with open(part, "r", encoding="utf-8") as f_in:
                for line in f_in:
                    if line.strip():
                        f_out.write(line)
    log(f"Saída MR: {output_jsonl.name}")

def jsonl_to_csv(jsonl_path: Path, out_csv: Path, columns: list):
    rows = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            rows.append(json.loads(line))
    df = pd.DataFrame(rows)
    if "total_streams" in df.columns:
        df = df.sort_values("total_streams", ascending=False)
    if columns:
        df = df.reindex(columns=columns)
    df.to_csv(out_csv, index=False)
    log(f"CSV final: {out_csv.name} (linhas={len(df)})")

def main():
    df = _read_tracks_dataframe()

    genplat_csv = PROCESSED / "mr_input_genplat.csv"
    parquet_to_compact_csv(df, cols=["generation", "platform", "streams"], out_csv=genplat_csv)

    genplat_jsonl = PROCESSED / "genplat.jsonl"
    run_mrjob("app/jobs/mapreduce_generation_stats.py", genplat_csv, genplat_jsonl)

    generation_stats_csv = PROCESSED / "generation_stats.csv"
    jsonl_to_csv(genplat_jsonl, generation_stats_csv,
                 columns=["generation", "platform", "total_streams"])

    genartist_csv = PROCESSED / "mr_input_genartist.csv"
    parquet_to_compact_csv(df, cols=["generation", "artist", "streams"], out_csv=genartist_csv)

    genartist_jsonl = PROCESSED / "genartist.jsonl"
    run_mrjob("app/jobs/mapreduce_artist_trends.py", genartist_csv, genartist_jsonl)

    artist_trends_csv = PROCESSED / "artist_trends.csv"
    jsonl_to_csv(genartist_jsonl, artist_trends_csv,
                 columns=["generation", "artist", "total_streams"])

    top_by_gen = (
        pd.read_csv(artist_trends_csv)
          .groupby("generation", group_keys=False)
          .head(10)
    )
    top_by_gen.to_csv(PROCESSED / "top_artists_by_gen.csv", index=False)
    log("Build finalizado com sucesso.")

if __name__ == "__main__":
    main()
