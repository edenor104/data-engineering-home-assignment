from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lag, avg, stddev, to_date


def build_spark():
    """
    Initializes and returns the SparkSession in AWS Glue environment.
    """
    spark = SparkSession.builder.appName("Stock Analysis Glue Job").getOrCreate()
    return spark


def load_data(spark, path):
    """
    Loads stock data from S3 into a Spark DataFrame.

    :param spark: SparkSession object.
    :param path: S3 path to the CSV file (e.g., s3://bucket/key.csv).
    :return: Spark DataFrame containing parsed stock data.
    """
    df = (
        spark.read.option("header", True).option("inferSchema", True).csv(path)
            .withColumn("date", to_date("date"))  # ensure correct type
    )
    return df


def clean_data(df):
    """
    Cleans raw stock data:
    - Drops nulls in essential columns.
    - Removes duplicates based on (date, ticker).
    - Casts numeric columns for consistency.

    :param df: Raw DataFrame.
    :return: Cleaned DataFrame.
    """
    df_cleaned = (
        df.dropna(subset=["date", "close", "ticker"])
            .dropDuplicates(["date", "ticker"])
            .withColumn("close", col("close").cast("double"))
            .withColumn("open", col("open").cast("double"))
            .withColumn("high", col("high").cast("double"))
            .withColumn("low", col("low").cast("double"))
            .withColumn("volume", col("volume").cast("long"))
    )
    return df_cleaned


def clean_data(df):
    """
    Performs basic data cleaning on the stock DataFrame.
    Drops rows with nulls in critical columns and removes duplicates
    based on the combination of 'Date' and 'Ticker'. (change should be of one company per day)
    Make sure that the numerical values are formatted correctly

    :param df: Spark DataFrame containing the raw stock data.
    :return: Cleaned Spark DataFrame.
    """
    initial_count = df.count()

    df_cleaned = (
        df.dropna(subset=["date", "close", "ticker"])
            .dropDuplicates(["date", "ticker"])
            .withColumn("close", col("close").cast("double"))
            .withColumn("open", col("open").cast("double"))
            .withColumn("high", col("high").cast("double"))
            .withColumn("low", col("low").cast("double"))
            .withColumn("volume", col("volume").cast("long"))
    )

    cleaned_count = df_cleaned.count()
    print(f"[CLEAN_DATA] Removed {initial_count - cleaned_count} rows (nulls or duplicates).")

    return df_cleaned


def compute_avg_daily_return(df):
    """
    Computes the average daily return across all tickers.

    Steps:
    1. Retrieves the closing value from the previous day (per ticker).
    2. Calculates the absolute difference from the current close.
    3. Divides by the previous closing value to compute the return rate.
    4. Multiplies by 100 to convert to percentage.

    :param df: Spark DataFrame containing stock data with 'date', 'ticker', and 'close' columns.
    :return: Spark DataFrame with columns ['date', 'average_return'].
    """
    w = Window.partitionBy("ticker").orderBy("date")
    df_ret = df.withColumn(
        "daily_return",
        100 * (col("close") - lag("close").over(w)) / lag("close").over(w)
    )
    avg_daily_return = (
        df_ret.groupBy("date")
            .agg(avg("daily_return").alias("average_return"))
            .orderBy("date")
    )
    return avg_daily_return


def compute_highest_worth_stock(df, top_n=None):
    """
    Computes the average worth per ticker based on the product of close price and volume.

    The worth is calculated as:
        worth = close * volume
    Then averaged per ticker.

    :param df: Spark DataFrame containing at least 'ticker', 'close', and 'volume' columns.
    :param top_n: Optional. Number of top tickers to return based on average worth. If None, returns all.
    :return: Spark DataFrame with columns ['ticker', 'average_worth'], ordered descending by worth.
    """
    highest_worth_stock = (
        df.withColumn("worth", col("close") * col("volume"))
            .groupBy("ticker")
            .agg(avg("worth").alias("average_worth"))
            .orderBy(col("average_worth").desc())
    )

    if top_n is not None:
        highest_worth_stock = highest_worth_stock.limit(top_n)

    return highest_worth_stock


def compute_most_volatile_stock(df, periods_per_year=252, top_n=None):
    """
    Computes the annualized standard deviation of daily returns for each ticker.

    Daily return is calculated as:
        (current_close - previous_close) / previous_close * 100

    The standard deviation of daily returns is then annualized using:
        annualized_volatility = stddev(daily_return) * sqrt(periods_per_year)

    :param df: Spark DataFrame containing 'ticker', 'date', and 'close' columns.
    :param periods_per_year: Number of trading periods in a year (default: 252 for daily returns).
    :param top_n: Optional. Number of top tickers to return based on volatility. If None, returns all.
    :return: Spark DataFrame with columns ['ticker', 'annualized_volatility'], ordered by descending volatility.
    """
    w = Window.partitionBy("ticker").orderBy("date")

    df_ret = df.withColumn(
        "daily_return",
        100 * (col("close") - lag("close").over(w)) / lag("close").over(w)
    )
    annualization_factor = periods_per_year ** 0.5
    most_volatile_stock = (
        df_ret.groupBy("ticker")
            .agg((stddev("daily_return") * annualization_factor).alias("annualized_volatility"))
            .orderBy(col("annualized_volatility").desc())
    )

    if top_n is not None:
        most_volatile_stock = most_volatile_stock.limit(top_n)

    return most_volatile_stock


def compute_top_n_day_returns(df, days=30, top_n=3, include_price_diff=False):
    """
    Computes N-day returns for each ticker and returns the top N entries by return.

    N-day return is calculated as:
        (close_today - close_N_days_ago) / close_N_days_ago * 100

    Optionally, it can also include the actual closing prices used in the calculation:
        - close_today
        - close_N_days_ago

    :param df: Spark DataFrame containing 'ticker', 'date', and 'close' columns.
    :param days: Number of days to look back for computing return (default: 30).
    :param top_n: Number of top entries to return (default: 3).
    :param include_price_diff: Whether to include raw closing prices used in return calculation.
    :return: Spark DataFrame with columns:
             ['ticker', 'date', 'return_N_day'] +
             optionally ['close_today', 'close_N_days_ago'] if include_price_diff=True.
    """
    w = Window.partitionBy("ticker").orderBy("date")

    lag_col = f"close_{days}_days_ago"
    return_col = f"return_{days}_day"

    # Lag the close price
    df_n = df.withColumn(lag_col, lag("close", days).over(w))

    # Calculate N-day return
    df_n = df_n.withColumn(
        return_col,
        100 * (col("close") - col(lag_col)) / col(lag_col)
    )

    # Filter out rows with null returns
    df_n = df_n.filter(col(return_col).isNotNull())

    # Build selected columns
    selected_cols = ["ticker", "date", return_col]
    if include_price_diff:
        df_n = df_n.withColumn("close_today", col("close"))
        df_n = df_n.withColumn("close_n_days_ago", col(lag_col))
        selected_cols += ["close_today", "close_n_days_ago"]

    # Select and return top N by return
    top_n_day_returns = (
        df_n.orderBy(col(return_col).desc())
            .select(*selected_cols)
            .limit(top_n)
    )

    return top_n_day_returns


def write_to_s3(df, output_path):
    """
    Writes a distributed Spark DataFrame to S3 in CSV format with headers.

    This approach is scalable and keeps distributed output (one file per partition).

    :param df: Spark DataFrame.
    :param output_path: S3 URI, e.g., "s3://bucket-name/folder/"
    """
    df.write.mode("overwrite").option("header", True).parquet(output_path)


def main():
    spark = build_spark()

    # S3 inputs and outputs
    input_path = "s3://data-engineer-assignment-eden/stocks_data.csv"
    output_prefix = "s3://data-engineer-assignment-eden/outputs"

    # Analysis parameters
    days = 30
    top_n = 3
    include_price_diff = False

    df = load_data(spark, input_path)
    cleaned_df = clean_data(df)

    avg_daily_return_df = compute_avg_daily_return(cleaned_df)
    write_to_s3(avg_daily_return_df, f"{output_prefix}/avg_daily_return")

    highest_worth_df = compute_highest_worth_stock(cleaned_df, top_n=top_n)
    write_to_s3(highest_worth_df, f"{output_prefix}/highest_worth_stock")

    most_volatile_df = compute_most_volatile_stock(cleaned_df, top_n=top_n)
    write_to_s3(most_volatile_df, f"{output_prefix}/most_volatile_stock")

    top_n_day_returns_df = compute_top_n_day_returns(
        cleaned_df, days=days, top_n=top_n, include_price_diff=include_price_diff
    )
    write_to_s3(top_n_day_returns_df, f"{output_prefix}/top_3_30day_returns")


if __name__ == "__main__":
    main()
