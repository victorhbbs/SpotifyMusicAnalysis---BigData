from pyspark.sql import SparkSession

def spark(app_name="BigDataSpotifyLocal"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.port", "4040")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
