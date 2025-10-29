import sys
from pyspark.sql import functions as F
from app.utils.io import spark

def decade(col):
    return (F.col(col) // 10) * 10

def main(tracks_parquet: str, artists_parquet: str, out_parquet: str):
    sp = spark("Join_Tracks_Artists")

    tracks = sp.read.parquet(tracks_parquet)
    artists = sp.read.parquet(artists_parquet)

    fact = (tracks.join(F.broadcast(artists.select("artist_id","artist_name","genre")),
                        on="artist_id", how="left")
                 .withColumn("decade", decade("year"))
                 .na.drop(subset=["decade"]))

    trends = (fact.groupBy("decade","genre")
                  .agg(
                      F.count("*").alias("tracks"),
                      F.avg("popularity").alias("avg_popularity")
                  ))

    trends.write.mode("overwrite").parquet(out_parquet)
    print(f"[JOIN] OK -> {out_parquet}")
    sp.stop()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
