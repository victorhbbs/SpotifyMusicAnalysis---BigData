import sys, os, glob
from pyspark.sql import functions as F
from app.utils.io import spark

def _expand_inputs(pattern: str):
    if any(ch in pattern for ch in ["*", "?", "["]):
        files = glob.glob(pattern)
    elif os.path.isdir(pattern):
        files = [os.path.join(pattern, f) for f in os.listdir(pattern) if f.lower().endswith(".csv")]
    else:
        files = [pattern]
    if not files:
        raise FileNotFoundError(f"Nenhum CSV para: {pattern}")
    return [f.replace("\\", "/") for f in files]

def main(input_csv: str, out_parquet: str):
    sp = spark("ETL_Artists")

    files = _expand_inputs(input_csv)
    df = (sp.read.option("header", True).option("inferSchema", True).csv(files))

    clean = F.regexp_replace(F.col("genres").cast("string"), r"[\[\]'\"]", "")
    df = df.withColumn("genres_clean", clean)

    df = df.withColumn("genre_raw", F.explode(F.split(F.col("genres_clean"), r",")))

    df = df.withColumn("genre", F.trim(F.col("genre_raw")))

    artists = (df.withColumnRenamed("id", "artist_id")
                 .withColumnRenamed("name", "artist_name")
                 .select("artist_id", "artist_name", "followers", "popularity", "genre"))

    out = (artists.na.drop(subset=["artist_id"])
                   .filter(F.col("genre").isNotNull() & (F.col("genre") != "")))
    kept = out.count()
    print(f"[ARTISTS ETL] linhas após explode/limpeza: {kept}")

    (out.repartition(8, "genre")
        .write.mode("overwrite")
        .partitionBy("genre")
        .parquet(out_parquet))

    print(f"[ARTISTS ETL] escrito em: {out_parquet}")
    sp.stop()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
