import sys
from pyspark.sql import SparkSession

def main(top_decade_path: str, yearly_pop_path: str, out_dir: str):
    spark = (SparkSession.builder
             .appName("ExportForDashboard")
             .master("local[*]")
             .getOrCreate())

    top = spark.read.json(top_decade_path)
    (top.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"{out_dir}/top_artists_decade_csv"))

    yearly = spark.read.parquet(yearly_pop_path)
    (yearly.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"{out_dir}/yearly_popularity_csv"))

    spark.stop()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
