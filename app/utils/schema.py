from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

tracks_schema = StructType([
    StructField("track_id", StringType(), True),
    StructField("track_name", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("album", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("genre", StringType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("popularity", IntegerType(), True),
    StructField("streams", DoubleType(), True),
    StructField("country", StringType(), True)
])