from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("RedditGoldStream") \
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
    ) \
    .getOrCreate()

silver_stream = spark.readStream \
    .format("mongodb") \
    .option("database", "silver_db") \
    .option("collection", "reddit_silver_stream") \
    .load()

removecols = [
    'author',
    'author_account_age_days',
    'author_comment_karma',
    'author_created_datetime',
    'author_created_ts',
    'author_has_verified_email',
    'author_is_gold',
    'author_is_mod',
    'author_link_karma',
    'author_premium',
    'author_total_karma',
    'author_verified'
]

fact_stream = silver_stream.drop(*removecols)

fact_stream = fact_stream.select(
    "subreddit",
    "datetime",
    "VaderScore",
    "TextblobScore",
    "SocialIndex",
    "VaderFullScore",
    "TextblobFullScore",
    "score",
    "num_comments",
    "upvote_ratio"
)

query = fact_stream.writeStream \
    .format("mongodb") \
    .option("database", "gold_db") \
    .option("collection", "SentimentFactStream") \
    .outputMode("append") \
    .option("checkpointLocation", "/chk/reddit_sentiment_gold") \
    .start()

query.awaitTermination()
