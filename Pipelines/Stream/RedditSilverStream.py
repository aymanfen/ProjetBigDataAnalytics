from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.functions import pandas_udf
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob

uri = ""

# Spark session
spark = SparkSession.builder \
    .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .config("spark.mongodb.read.connection.uri", uri) \
    .config("spark.mongodb.write.connection.uri", uri) \
    .appName("RedditSilverStream") \
    .getOrCreate()

df = spark.readStream \
    .format("mongodb") \
    .option("database", "bronze_db") \
    .option("collection", "reddit_bronze_stream") \
    .load()

df = df.withColumn(
    "datetime",
    F.to_timestamp("created_datetime", "yyyy-MM-dd'T'HH:mm:ss")
)

df = df.withWatermark("datetime", "15 minutes")

analyzer = SentimentIntensityAnalyzer()

@pandas_udf(FloatType())
def vader_sentiment(text: pd.Series) -> pd.Series:
    return text.fillna("").apply(
        lambda t: analyzer.polarity_scores(t)['compound']
    )

@pandas_udf(FloatType())
def textblob_sentiment(text: pd.Series) -> pd.Series:
    return text.fillna("").apply(
        lambda t: TextBlob(t).sentiment.polarity
    )

df = df.withColumn("VaderScore", vader_sentiment(F.col("selftext"))) \
       .withColumn("TextblobScore", textblob_sentiment(F.col("selftext")))

# log features
df = df.withColumn("log_post_score", F.log1p(F.col("score"))) \
       .withColumn("log_num_comments", F.log1p(F.col("num_comments"))) \
       .withColumn("log_author_karma", F.log1p(F.col("author_total_karma"))) \
       .withColumn("log_author_comment_karma", F.log1p(F.col("author_comment_karma")))

# social index
df = df.withColumn(
    "SocialIndex",
    F.col("log_post_score") +
    F.col("log_num_comments") +
    F.col("log_author_karma") +
    F.col("log_author_comment_karma") +
    F.col("upvote_ratio")
)

# weighted sentiment
df = df.withColumn("VaderFullScore", F.col("SocialIndex") * F.col("VaderScore")) \
       .withColumn("TextblobFullScore", F.col("SocialIndex") * F.col("TextblobScore"))

cols_to_remove = [
    "archived", "author_created_utc", "author_created_ts",
    "author_flair", "created_utc", "distinguished_type",
    "edited", "edited_datetime", "flair", "flair_css",
    "gilded", "id", "is_distinguished", "is_gallery",
    "is_self", "is_video", "locked", "media_type",
    "over_18", "permalink", "selftext", "spoiler",
    "total_awards", "url", "created_datetime",
    "log_post_score", "log_num_comments",
    "log_author_karma", "log_author_comment_karma"
]

df = df.drop(*cols_to_remove)

query = df.writeStream \
    .format("mongodb") \
    .option("database", "silver_db") \
    .option("collection", "reddit_silver_stream") \
    .outputMode("append") \
    .option("checkpointLocation", "/chk/redditsilver") \
    .start()

query.awaitTermination()
