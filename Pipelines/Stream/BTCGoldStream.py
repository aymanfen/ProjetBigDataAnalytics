from pyspark.sql import SparkSession

uri="mongodb+srv://aymanfenkouch_db_user:pVXppiIq5O6WWOI8@stockmarketcluster.vnoccrr.mongodb.net/?appName=StockMarketCluster"


spark = SparkSession.builder \
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

