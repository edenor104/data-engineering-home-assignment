import os
import boto3
import tempfile
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lag, avg, stddev, to_date
from dotenv import load_dotenv


def build_spark():
    return (
        SparkSession.builder
        .appName("Stocks Data Analysis (temp CSV upload)")
        .master("local[*]")
        .getOrCreate()
    )


def load_data(spark, path="stocks_data.csv"):
    df = (
        spark.read.csv(path, header=True, inferSchema=True)
        .withColumn("Date", to_date("Date"))
        .withColumn("Close", col("Close").cast("double"))
        .withColumn("Volume", col("Volume").cast("long"))
        .withColumn("Ticker", col("Ticker"))
    )
    return df


def clean_data(df):
    initial = df.count()
    df_cleaned = df.dropna(subset=["Date", "Close", "Ticker"]).dropDuplicates(["Date", "Ticker"])
    print(f"[CLEAN_DATA] Removed {initial - df_cleaned.count()} rows.")
    return df_cleaned


def compute_avg_daily_return(df):
    w = Window.partitionBy("Ticker").orderBy("Date")
    df_ret = df.withColumn(
        "daily_return",
        100 * (col("Close") - lag("Close").over(w)) / lag("Close").over(w)
    )
    avg_daily_return = (
        df_ret.groupBy("Date")
        .agg(avg("daily_return").alias("average_return"))
        .orderBy("Date")
    )
    return avg_daily_return


def compute_highest_worth_stock(df):
    highest_worth_stock = (
        df.withColumn("worth", col("Close") * col("Volume"))
        .groupBy("Ticker")
        .agg(avg("worth").alias("average_worth"))
        .orderBy(col("average_worth").desc())
    )
    return highest_worth_stock


def compute_most_volatile_stock(df):
    w = Window.partitionBy("Ticker").orderBy("Date")
    df_ret = df.withColumn(
        "daily_return",
        100 * (col("Close") - lag("Close").over(w)) / lag("Close").over(w)
    )
    most_volatile_stock = (
        df_ret.groupBy("Ticker")
        .agg(stddev("daily_return").alias("standard_deviation"))
        .orderBy(col("standard_deviation").desc())
    )
    return most_volatile_stock


def compute_top_three_30_day_returns(df):
    w = Window.partitionBy("Ticker").orderBy("Date")
    df_30 = df.withColumn("close_30_days_ago", lag("Close", 30).over(w))
    df_30 = df_30.withColumn(
        "return_30_day",
        100 * (col("Close") - col("close_30_days_ago")) / col("close_30_days_ago")
    )
    top_three_30_day_returns = (
        df_30.orderBy(col("return_30_day").desc())
        .select("Ticker", "Date", "return_30_day")
        .limit(3)
    )
    return top_three_30_day_returns


def save_and_upload_as_csv(df_spark, bucket, s3_key):
    """
    Convert Spark DF -> Pandas -> temporary CSV -> upload to S3.
    Auto-deletes temp file after upload.
    """
    pdf = df_spark.toPandas()

    tmp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    try:
        pdf.to_csv(tmp_file.name, index=False)
        tmp_file.close()

        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "eu-central-1"),
        )
        s3.upload_file(tmp_file.name, bucket, s3_key)
        print(f"[UPLOAD] Uploaded to s3://{bucket}/{s3_key}")
    finally:
        os.remove(tmp_file.name)  # Clean up temp file


def main():
    load_dotenv()
    bucket = "data-engineer-assignment-eden"

    spark = build_spark()
    df = load_data(spark, "stocks_data.csv")
    df = clean_data(df)

    avg_daily_return = compute_avg_daily_return(df)
    save_and_upload_as_csv(avg_daily_return, bucket, "outputs/avg_daily_return.csv")

    highest_worth_stock = compute_highest_worth_stock(df)
    save_and_upload_as_csv(highest_worth_stock, bucket, "outputs/highest_worth_stock.csv")

    most_volatile_stock = compute_most_volatile_stock(df)
    save_and_upload_as_csv(most_volatile_stock, bucket, "outputs/most_volatile_stock.csv")

    top_three_30_day_returns = compute_top_three_30_day_returns(df)
    save_and_upload_as_csv(top_three_30_day_returns, bucket, "outputs/top_3_30day_return.csv")

    spark.stop()

if __name__ == "__main__":
    main()
