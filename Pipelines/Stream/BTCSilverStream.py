from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

uri=''

spark = SparkSession.builder \
    .appName("BTCSilverStream") \
    .config(
        "spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .config(
        "spark.mongodb.read.connection.uri",uri) \
    .config(
        "spark.mongodb.write.connection.uri",uri) \
    .getOrCreate()

df=spark.readStream\
    .format("mongodb")\
    .option("database", "bronze_db") \
    .option("collection", "btc_bronze_batch") \
    .load()

bucketed = df.groupBy(
    F.window("datetime", "5 minutes"),
    "ticker"
).agg(
    F.last("close").alias("close_5min")
)

bucketed = bucketed.select(
    "ticker",
    F.col("window.end").alias("datetime"),
    "close_5min"
)

w = Window.partitionBy("ticker").orderBy("datetime")

silver = bucketed.withColumn(
    "prev_close",
    F.lag("close_5min").over(w)
).withColumn(
    "PrctChange",
    (F.col("close_5min") - F.col("prev_close")) / F.col("prev_close") * 100
).withColumn(
    "Direction",
    F.when(F.col("PrctChange") > 0, "Up")
     .when(F.col("PrctChange") < 0, "Down")
     .otherwise("No Change")
)

final_df = silver.select(
    "ticker",
    "datetime",
    F.col("close_5min").alias("close"),
    "high",
    "low",
    "open",
    'volume',
    "PrctChange",
    "Direction"
)

query = final_df.writeStream \
    .format("mongodb") \
    .option("database", "silver_db") \
    .option("collection", "btc_silver_stream") \
    .outputMode("append") \
    .option("checkpointLocation", "/chk/btcsilver") \
    .start()

query.awaitTermination()
