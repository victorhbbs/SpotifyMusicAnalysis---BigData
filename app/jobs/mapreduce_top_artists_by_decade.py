import sys
from pyspark.sql import functions as F
from app.utils.io import spark

def decade(y: int) -> int:
    return (int(y) // 10) * 10

def main(parquet_path: str, output_path: str):
    sp = spark("MapReduceTopArtists")
    df = sp.read.parquet(parquet_path)

    rdd = (
        df.select("artist", "year")
          .where(F.col("artist").isNotNull() & F.col("year").isNotNull())
          .rdd
          .map(lambda r: ((r["artist"], decade(r["year"])), 1))
          .reduceByKey(lambda a, b: a + b)
    )

    out = (rdd
           .map(lambda kv: (kv[0][0], kv[0][1], kv[1]))
           .toDF(["artist", "decade", "track_count"])
           .orderBy(F.col("decade").asc(), F.col("track_count").desc()))

    (out.coalesce(1)
        .write.mode("overwrite")
        .json(output_path))

    sp.stop()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
