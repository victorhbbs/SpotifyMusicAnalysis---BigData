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

def _extract_year_from(colname: str):
    return F.regexp_extract(F.col(colname).cast("string"), r"(\d{4})", 1).cast("int")

def _split_artist_ids(colname: str):
    s = F.regexp_replace(F.col(colname).cast("string"), r"[\[\]'\" ]", "")
    return F.split(s, r",")

def main(input_csv: str, out_parquet: str):
    sp = spark("ETL_Tracks")

    files = _expand_inputs(input_csv)
    df = (sp.read.option("header", True).option("inferSchema", True).csv(files))

    df = df.withColumnRenamed("name", "track_name")

    df = df.withColumn("year", _extract_year_from("release_date"))

    df = df.withColumn("artist_ids", _split_artist_ids("id_artists"))
    df = df.withColumn("artist_id", F.explode(F.col("artist_ids")))
    df = df.drop("artist_ids")

    df = (df.withColumn("track_name", F.trim("track_name"))
            .withColumn("artist_id", F.trim("artist_id"))
            .filter(F.col("artist_id").isNotNull() & (F.col("artist_id") != "")))

    total = df.count()
    with_year = df.filter(F.col("year").isNotNull()).count()
    print(f"[TRACKS ETL] linhas pós-explode: {total} | com year: {with_year}")

    if with_year == 0:
        print("[TRACKS ETL] Atenção: 'year' não encontrado; gravando sem particionamento.")
        df.write.mode("overwrite").parquet(out_parquet)
    else:
        (df.filter(F.col("year").isNotNull())
            .repartition(8, "year")
            .write.mode("overwrite")
            .partitionBy("year")
            .parquet(out_parquet))

    print(f"[TRACKS ETL] escrito em: {out_parquet}")
    sp.stop()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
