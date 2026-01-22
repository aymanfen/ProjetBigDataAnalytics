from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

uri=''


spark = SparkSession.builder \
    .appName("BTCSilverBatch") \
    .config(
        "spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .config(
        "spark.mongodb.read.connection.uri",uri) \
    .config(
        "spark.mongodb.write.connection.uri",uri) \
    .getOrCreate()

df=spark.read\
    .format("mongodb")\
    .option("database", "bronze_db") \
    .option("collection", "btc_bronze_batch") \
    .load()



#prct change and change direction

windowspec=Window.orderBy("datetime")

df=df.withColumn("PrctChange",(F.col("close") - F.lag("close").over(windowspec)) / F.lag("close").over(windowspec) * 100)

df = df.withColumn("Direction", F.when(F.col("PrctChange") > 0, "Up")
                               .when(F.col("PrctChange") < 0, "Down")
                               .otherwise("No Change"))

# SMA over n periods ( hours )

df = df.withColumn("SMA20", F.avg("close").over(Window.orderBy("datetime").rowsBetween(-(20-1), 0)))
df = df.withColumn("SMA50", F.avg("close").over(Window.orderBy("datetime").rowsBetween(-(50-1), 0)))
df = df.withColumn("SMA200", F.avg("close").over(Window.orderBy("datetime").rowsBetween(-(200-1), 0)))


# RSI
n = 14

# Gain and Loss
df = df.withColumn("change", F.col("close") - F.lag("close").over(windowspec))
df = df.withColumn("gain", F.when(F.col("change") > 0, F.col("change")).otherwise(0))
df = df.withColumn("loss", F.when(F.col("change") < 0, -F.col("change")).otherwise(0))

# Average Gain / Loss
avg_gain = F.avg("gain").over(Window.orderBy("datetime").rowsBetween(-(n-1), 0))
avg_loss = F.avg("loss").over(Window.orderBy("datetime").rowsBetween(-(n-1), 0))

df = df.withColumn("RS", avg_gain / avg_loss)
df = df.withColumn("RSI", 100 - 100 / (1 + F.col("RS")))


# ATR
# Suppose df has high, low, close columns
df = df.withColumn("prev_close", F.lag("close").over(windowspec))
df = df.withColumn("TR", F.greatest(
    F.col("high") - F.col("low"),
    F.abs(F.col("high") - F.col("prev_close")),
    F.abs(F.col("low") - F.col("prev_close"))
))

# ATR using moving average over n periods
n = 14
df = df.withColumn("ATR", F.avg("TR").over(Window.orderBy("datetime").rowsBetween(-(n-1), 0)))


# MACD

# Step 1: Define rolling windows for short and long MA
short_window = 12  # e.g., 12 hours
long_window = 26   # e.g., 26 hours

window_short = Window.orderBy("datetime").rowsBetween(-(short_window-1), 0)
window_long = Window.orderBy("datetime").rowsBetween(-(long_window-1), 0)

# Step 2: Compute short-term and long-term SMA
df = df.withColumn("SMA_short", F.avg("close").over(window_short))
df = df.withColumn("SMA_long", F.avg("close").over(window_long))

# Step 3: Compute MACD using SMA
df = df.withColumn("MACD", F.col("SMA_short") - F.col("SMA_long"))

cols_to_remove = ["interval", "scraped_at", "change", "gain",'loss','RS','prev_close','TR','SMA_short','SMA_long']

df = df.drop(*cols_to_remove)

df.write\
    .format("mongodb")\
    .option("database",'silver_db')\
    .option("collection",'btc_silver_batch')\
    .mode("overwrite")\
    .save()


