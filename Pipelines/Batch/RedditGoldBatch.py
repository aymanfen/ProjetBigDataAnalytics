from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit

uri=''

spark = SparkSession.builder \
    .appName("RedditGoldBatch") \
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
    .option("collection", "reddit_silver_batch") \
    .load()


df.printSchema()
df.show()

AuthorDim=df.select(col('author'),
                    col('author_account_age_days'),
                    col('author_comment_karma'),
                    col('author_created_datetime'),
                    col('author_fullname'),
                    col('author_has_verified_email'),
                    col('author_is_gold'),
                    col('author_is_mod'),
                    col('author_link_karma'),
                    col('author_premium'),
                    col('author_total_karma'),
                    col('author_verified')).dropDuplicates()

PlatformDim=df.select(col('subreddit')).withColumn("Platform",lit("Reddit")).dropDuplicates()

removecols=['author','author_account_age_days','author_comment_karma','author_created_datetime','author_created_ts','author_has_verified_email','author_is_gold',
            'author_is_mod','author_link_karma','author_premium','author_total_karma','author_verified']

FactSentiment=df.drop(*removecols)

FactSentiment.write\
    .format("mongodb")\
    .option("database",'gold_db')\
    .option("collection",'SentimentFact')\
    .mode("overwrite")\
    .save()

PlatformDim.write\
    .format("mongodb")\
    .option("database",'gold_db')\
    .option("collection",'PlatformDim')\
    .mode("overwrite")\
    .save()

AuthorDim.write\
    .format("mongodb")\
    .option("database",'gold_db')\
    .option("collection",'AuthorDim')\
    .mode("overwrite")\
    .save()


