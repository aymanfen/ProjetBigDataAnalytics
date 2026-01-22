from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,to_date, year, month, dayofmonth,dayofweek, weekofyear, quarter, date_format, hour, minute, second


uri="mongodb+srv://aymanfenkouch_db_user:pVXppiIq5O6WWOI8@stockmarketcluster.vnoccrr.mongodb.net/?appName=StockMarketCluster"


spark = SparkSession.builder \
    .appName("BTCGoldBatch") \
    .config(
        "spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .config(
        "spark.mongodb.read.connection.uri",uri) \
    .config(
        "spark.mongodb.write.connection.uri",uri) \
    .getOrCreate()


df=spark.read\
    .format("mongodb")\
    .option("database", "silver_db") \
    .option("collection", "btc_silver_batch") \
    .load()


df.printSchema()
df.show()


TickerDim=df.select(col('ticker')).dropDuplicates()


TimeDim=df.select(col('datetime')).dropDuplicates()\
            .withColumn("year", year("datetime"))\
                .withColumn("month", month("datetime"))\
                .withColumn("day", dayofmonth("datetime"))\
                .withColumn("week", weekofyear("datetime"))\
                .withColumn("quarter", quarter("datetime"))\
                .withColumn("hour", hour("datetime"))\
                .withColumn("minute", minute("datetime"))\
                .withColumn("second", second("datetime"))



df.write\
    .format("mongodb")\
    .option("database",'gold_db')\
    .option("collection",'FinancialFact')\
    .mode("overwrite")\
    .save()


TimeDim.write\
    .format("mongodb")\
    .option("database",'gold_db')\
    .option("collection",'TimeDim')\
    .mode("overwrite")\
    .save()

TickerDim.write\
    .format("mongodb")\
    .option("database",'gold_db')\
    .option("collection",'TickerDim')\
    .mode("overwrite")\
    .save()


