from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from pathlib import Path
from os.path import join

BASE_FOLDER = join(
    str(Path("~/Documents").expanduser()),
    "twitter_data_eng/twitter_datalake/",
    "{stage}/twitter"
)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("twitter_insight_tweet").getOrCreate()

    tweet = spark.read.json(BASE_FOLDER.format(stage="silver"))

    user_tweets = tweet.where("author_id = TWITTER_USER_ID").select(
        "author_id", "conversation_id")

    tweet = tweet.alias("tweet").join(user_tweets.alias("user_tweet"), [
        user_tweets.author_id != tweet.author_id,
        user_tweets.conversation_id != tweet.conversation_id
    ], "left"
    ).withColumn(
        "user_conversation", F.when(
            F.col("user_tweets.conversation_id").isNotNull(), 1).otherwise(0)
    ).withColumn(
        "reply_user", F.when(
            F.col("tweet.in_reply_to_user_id") == "TWITTER_USER_ID", 1
        ).otherwise(0)).groupBy(
            F.to_date("created_at").alias("created_date")
    ).agg(
        F.countDistinct("id").alias("n_tweet"),
        F.countDistinct("tweet.conversation_id").alias("n_conversation"),
        F.sum("user_conversation").alias("user_conversation"),
        F.sum("reply_user").alias("reply_user")
    ).withColumn("weekday", F.to_date("created_date", "E"))

    tweet.coalesce(1).write.json(BASE_FOLDER.format(stage="gold"))
