from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, quarter, avg, count, 
    round, lag, stddev, to_date, date_format, 
    when, coalesce, lit, sequence, explode
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, TimestampType, IntegerType
)
import json
import os
import sys
import traceback
import pandas as pd
import numpy as np

def prepare_spark_session():
    """
    Initialize Spark session with appropriate configurations
    """
    return (SparkSession.builder
        .appName("Yelp Business Rating Trend Analysis")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .getOrCreate())

def load_and_preprocess_data(spark, input_path):
    """
    Load and preprocess Yelp business reviews data
    """
    # Define schema to ensure correct parsing
    schema = StructType([
        StructField("business_ID", StringType(), False),
        StructField("business_name", StringType(), False),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("stars", DoubleType(), False),
        StructField("review_text", StringType(), True),
        StructField("review_date", TimestampType(), False)
    ])
    
    # Read TSV with defined schema
    df = (spark.read
          .format("csv")
          .option("sep", "\t")
          .option("header", "false")
          .schema(schema)
          .load(input_path)
          .withColumn("year", year(col("review_date")).cast("integer"))
          .withColumn("month", month(col("review_date")).cast("integer"))
          .withColumn("quarter", quarter(col("review_date")))
          .withColumn("formatted_review_date", date_format(col("review_date"), "yyyy-MM-dd HH:mm:ss"))
          .na.drop(subset=["business_ID", "stars", "review_date"])
    )
    
    return df

def generate_complete_time_periods(spark, df):
    """
    Generate a complete set of time periods for each business
    """
    # Get min and max years from the data
    min_year = df.agg({"year": "min"}).collect()[0][0]
    max_year = df.agg({"year": "max"}).collect()[0][0]
    
    # Get unique businesses
    businesses = df.select("business_ID", "business_name", "city", "state").distinct()
    
    # Generate all possible year-month combinations
    all_periods = spark.createDataFrame(
        [(y, m) for y in range(min_year, max_year + 1) 
         for m in range(1, 13)],
        ["year", "month"]
    )
    
    # Cross join businesses with all periods
    complete_periods = businesses.crossJoin(all_periods)
    
    return complete_periods

def analyze_business_trends(df, output_dir):
    """
    Perform comprehensive business-specific time-based analysis
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Generate complete time periods
    spark = df.sparkSession
    complete_periods = generate_complete_time_periods(spark, df)
    businesses = df.select("business_ID", "business_name", "city", "state").distinct()

    # Business-level Monthly Trends with Complete Periods
    monthly_base = (df
        .groupBy(
            col("business_ID"),
            col("business_name"),
            col("city"),
            col("state"),
            col("year"),
            col("month")
        )
        .agg(
            round(avg("stars"), 2).alias("monthly_avg_rating"),
            coalesce(round(stddev("stars"), 2), lit(0.0)).alias("monthly_rating_std"),
            count("*").alias("review_volume")
        )
    )

    # Left join with complete periods to include all months
    business_monthly_trends = (complete_periods
        .join(monthly_base, 
              (complete_periods.business_ID == monthly_base.business_ID) & 
              (complete_periods.year == monthly_base.year) & 
              (complete_periods.month == monthly_base.month), 
              "left")
        .select(
            complete_periods.business_ID, 
            complete_periods.business_name,
            complete_periods.city,
            complete_periods.state,
            complete_periods.year,
            complete_periods.month,
            coalesce(monthly_base.monthly_avg_rating, lit(None)).alias("monthly_avg_rating"),
            coalesce(monthly_base.monthly_rating_std, lit(None)).alias("monthly_rating_std"),
            coalesce(monthly_base.review_volume, lit(0)).alias("review_volume")
        )
        .orderBy("business_ID", "year", "month")
    )

    # Quarterly Trends with Complete Periods
    quarterly_base = (df
        .groupBy(
            col("business_ID"),
            col("business_name"),
            col("city"),
            col("state"),
            col("year"),
            col("quarter")
        )
        .agg(
            round(avg("stars"), 2).alias("avg_quarterly_rating"),
            coalesce(round(stddev("stars"), 2), lit(0.0)).alias("quarterly_rating_std"),
            count("*").alias("quarterly_review_volume")
        )
    )

    # Generate complete quarterly periods
    complete_quarterly_periods = spark.createDataFrame(
        [(y, q) for y in range(df.agg({"year": "min"}).collect()[0][0], 
                                df.agg({"year": "max"}).collect()[0][0] + 1) 
         for q in range(1, 5)],
        ["year", "quarter"]
    )

    # Left join with complete quarterly periods
    business_quarterly_trends = (complete_quarterly_periods
        .crossJoin(businesses)
        .join(quarterly_base, 
              (complete_quarterly_periods.year == quarterly_base.year) & 
              (complete_quarterly_periods.quarter == quarterly_base.quarter) & 
              (businesses.business_ID == quarterly_base.business_ID), 
              "left")
        .select(
            businesses.business_ID, 
            businesses.business_name,
            businesses.city,
            businesses.state,
            complete_quarterly_periods.year,
            complete_quarterly_periods.quarter,
            coalesce(quarterly_base.avg_quarterly_rating, lit(None)).alias("avg_quarterly_rating"),
            coalesce(quarterly_base.quarterly_rating_std, lit(None)).alias("quarterly_rating_std"),
            coalesce(quarterly_base.quarterly_review_volume, lit(0)).alias("quarterly_review_volume")
        )
        .orderBy("business_ID", "year", "quarter")
    )

    # Convert to Pandas and save reports
    monthly_report = business_monthly_trends.toPandas()
    quarterly_report = business_quarterly_trends.toPandas()

    # Save JSON reports
    monthly_output_path = os.path.join(output_dir, 'business_monthly_trends.json')
    quarterly_output_path = os.path.join(output_dir, 'business_quarterly_trends.json')

    monthly_report.to_json(monthly_output_path, orient='records', indent=2)
    quarterly_report.to_json(quarterly_output_path, orient='records', indent=2)

    print("Business trend analysis reports saved to: {}".format(output_dir))
    
    return {
        "monthly_trends": monthly_report.to_dict(orient="records"),
        "quarterly_trends": quarterly_report.to_dict(orient="records")
    }

def main(input_path, output_dir):
    """
    Main execution function
    """
    # Initialize Spark session
    spark = prepare_spark_session()
    
    try:
        # Load and preprocess data
        reviews_df = load_and_preprocess_data(spark, input_path)
        
        # Generate business-specific trend analysis report
        analyze_business_trends(reviews_df, output_dir)
    
    finally:
        spark.stop()

# Example usage
if __name__ == "__main__":
    main('../dataset/small-r-00000', 'output')