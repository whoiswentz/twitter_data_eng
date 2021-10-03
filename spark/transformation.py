from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

import argparse
from os.path import join


def get_tweets_data(df: DataFrame) -> DataFrame:
    return df.select(
        F.explode("data").alias("tweets")
    ).select(
        "tweets.author_id",
        "tweets.conversation_id",
        "tweets.public_metrics.*",
        "tweets.text",
        "tweets.in_reply_to_user_id",
        "tweets.created_at"
    )


def get_users_data(df: DataFrame) -> DataFrame:
    return df.select(
        F.explode("includes.users").alias("users")
    ).select("users.*")


def export_json(df: DataFrame, destination: str) -> None:
    df.coalesce(1).write.mode("overwrite").json(destination)


def twitter_transform(spark: SparkSession, source: str, destination: str, process_date: str) -> None:
    df = spark.read.json(source)

    tweets_df = get_tweets_data(df)
    users_df = get_users_data(df)

    table_destination = join(
        destination, "{table_name}", f"process_date={process_date}")

    export_json(tweets_df, table_destination.format(table_name="tweet"))
    export_json(users_df, table_destination.format(table_name="user"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Twitter Tranformation")
    parser.add_argument("--source", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--process-date", required=True)
    args = parser.parse_args()

    print(args)

    spark = SparkSession.builder.appName(
        "twitter_transformation").getOrCreate()

    twitter_transform(spark, args.source,
                      args.destination, args.process_date)
