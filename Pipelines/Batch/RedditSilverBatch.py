from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,log, to_timestamp, from_utc_timestamp, from_unixtime
from pyspark.sql.types import FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob

uri="mongodb+srv://aymanfenkouch_db_user:pVXppiIq5O6WWOI8@stockmarketcluster.vnoccrr.mongodb.net/?appName=StockMarketCluster"

spark = SparkSession.builder \
    .appName("RedditSilverBatch") \
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
    .option("collection", "reddit_bronze_batch") \
    .load()


df.printSchema()
df.show()

# VADER UDF
analyzer = SentimentIntensityAnalyzer()

def vader_sentiment(text):
    if text:
        return float(analyzer.polarity_scores(text)['compound'])
    else:
        return 0.0

vader_udf = udf(vader_sentiment, FloatType())

# TextBlob UDF
def textblob_sentiment(text):
    if text:
        return float(TextBlob(text).sentiment.polarity)
    else:
        return 0.0

textblob_udf = udf(textblob_sentiment, FloatType())


df = df.withColumn("VaderScore", vader_udf(col("selftext"))) \
       .withColumn("TextblobScore", textblob_udf(col("selftext")))


# shift features to avoid log(0)
df = df.withColumn("log_post_score", log(1 + col("score"))) \
       .withColumn("log_num_comments", log(1 + col("num_comments"))) \
       .withColumn("log_author_karma", log(1 + col("author_total_karma"))) \
       .withColumn("log_author_comment_karma", log(1 + col("author_comment_karma")))

# define social index
df = df.withColumn(
    "SocialIndex",
    col("log_post_score") +
    col("log_num_comments") +
    col("log_author_karma") +
    col("log_author_comment_karma") +
    col("upvote_ratio")   # weight here if you want: col("upvote_ratio")*2
)

# optional: combine with sentiment
df = df.withColumn("VaderFullScore", col("SocialIndex") * col("VaderScore"))
df = df.withColumn("TextblobFullScore", col("SocialIndex") * col("TextblobScore"))

df = df.withColumn("datetime", to_timestamp(col("created_datetime"), "yyyy-MM-dd'T'HH:mm:ss"))


df = df.withColumn(
    "author_created_ts",from_unixtime(col("author_created_utc")).cast("timestamp")
).withColumn(
    "author_created_datetime",from_utc_timestamp(col("author_created_ts"), "Africa/Casablanca")
)

cols_to_remove = ["archived",'author_created_utc','author_created_ts','author_flair','created_utc','distinguished_type','edited','edited_datetime',
                  'flair','flair_css','gilded','id','is_distinguished','is_gallery','is_self','is_video','locked','media_type','over_18','permalink','selftext','spoiler',
                  'total_awards','url','created_datetime','log_post_score','log_num_comments','log_author_karma','log_author_comment_karma']

df = df.drop(*cols_to_remove)

df.write\
    .format("mongodb")\
    .option("database",'silver_db')\
    .option("collection",'reddit_silver_batch')\
    .mode("overwrite")\
    .save()


