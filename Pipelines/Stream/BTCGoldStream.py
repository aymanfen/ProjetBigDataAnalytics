from pyspark.sql import SparkSession

# MongoDB URI
uri = ""

# Spark session
spark = SparkSession.builder \
    .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .config("spark.mongodb.read.connection.uri", uri) \
    .config("spark.mongodb.write.connection.uri", uri) \
    .appName("BTCGoldStream") \
    .getOrCreate()

silver_stream = spark.readStream \
    .format("mongodb") \
    .option("database", "silver_db") \
    .option("collection", "btc_silver_stream") \
    .load()

gold_facts = silver_stream.select(
    "ticker",
    "datetime",
    'open',
    "close",
    'high',
    'low',
    'volume',
    "PrctChange",
    "Direction"
)

query = gold_facts.writeStream \
    .format("mongodb") \
    .option("database", "gold_db") \
    .option("collection", "FinancialFactStream") \
    .outputMode("append") \
    .option("checkpointLocation", "/chk/btcgold") \
    .start()

query.awaitTermination()

