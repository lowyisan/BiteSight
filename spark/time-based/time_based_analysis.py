# Contributor(s): Low Yi San

import os
import sys
import json
import traceback
import re
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, quarter, avg, count, round, stddev,
    date_format, coalesce, lit, collect_list, lag, when, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, DoubleType, TimestampType
)
from pyspark.sql.window import Window

# Topic modeling imports
import gensim
import gensim.corpora as corpora
import nltk
from nltk.corpus import stopwords

# Download NLTK stopwords if not already downloaded.
nltk.download('stopwords')
stop_words = set(stopwords.words('english'))


def prepare_spark_session():
    """Initialize Spark session with appropriate configurations."""
    return (SparkSession.builder
            .appName("Yelp Business Rating Trend Analysis with Spikes & Dips")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "4g")
            .getOrCreate())

def create_yelp_schema():
    """Create schema for Yelp dataset."""
    return StructType([
        StructField("business_ID", StringType(), True),
        StructField("business_name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal", StringType(), True),
        StructField("lat", FloatType(), True),
        StructField("lon", FloatType(), True),
        StructField("categories", StringType(), True),
        StructField("opening_hours", StringType(), True),
        StructField("stars", DoubleType(), True),
        StructField("review_text", StringType(), True),
        StructField("datetime", StringType(), True)
    ])

def validate_hdfs_path(spark, path):
    """Validate that an HDFS path exists."""
    if path.startswith("hdfs://"):
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        if not fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
            raise FileNotFoundError(f"Path not found: {path}")

def ensure_output_dir(spark, output_dir):
    """Ensure output directory exists (works for both local and HDFS)."""
    if output_dir.startswith("hdfs://"):
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        fs.mkdirs(spark._jvm.org.apache.hadoop.fs.Path(output_dir))
    else:
        os.makedirs(output_dir, exist_ok=True)

def load_and_preprocess_data(spark, input_path):
    """Load and preprocess data with path validation."""
    validate_hdfs_path(spark, input_path)
    
    schema = create_yelp_schema()
    df = (spark.read
          .format("csv")
          .option("sep", "\t")
          .option("header", "false")
          .schema(schema)
          .load(input_path)
          .withColumn("review_timestamp", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss"))
          .withColumn("year", year(col("review_timestamp")).cast("integer"))
          .withColumn("month", month(col("review_timestamp")).cast("integer"))
          .withColumn("quarter", quarter(col("review_timestamp")))
          .withColumn("formatted_review_date", date_format(col("review_timestamp"), "yyyy-MM-dd HH:mm:ss"))
          .na.drop(subset=["business_ID", "stars", "datetime"]))
    return df

def compute_monthly_trends(df):
    """Compute monthly trends."""
    return (df.groupBy("business_ID", "business_name", "address", "city", "state", "postal", "year", "month")
            .agg(round(avg("stars"), 2).alias("monthly_avg_rating"),
                   coalesce(round(stddev("stars"), 2), lit(0.0)).alias("monthly_rating_std"),
                   count("*").alias("review_volume")))

def analyze_business_trends(spark, df, output_dir):
    """Analyze and save business trends."""
    ensure_output_dir(spark, output_dir)
    
    # Quarterly analysis
    quarterly_trends = (df.groupBy("business_ID", "business_name", "city", "state", "postal", "year", "quarter")
                        .agg(round(avg("stars"), 2).alias("avg_quarterly_rating"),
                               count("*").alias("total_reviews"),
                               coalesce(round(stddev("stars"), 2), lit(0.0)).alias("rating_volatility")))
    
    # Monthly analysis
    monthly_trends = (df.groupBy("business_ID", "business_name", "address", "city", "state", "postal", "year", "month")
                      .agg(round(avg("stars"), 2).alias("monthly_avg_rating"),
                             coalesce(round(stddev("stars"), 2), lit(0.0)).alias("monthly_rating_std"),
                             count("*").alias("review_volume")))
    
    # Save results
    if output_dir.startswith("hdfs://"):
        # For HDFS output
        quarterly_trends.write.mode("overwrite").json(os.path.join(output_dir, "quarterly_trends"))
        monthly_trends.write.mode("overwrite").json(os.path.join(output_dir, "monthly_trends"))
    else:
        # For local output
        quarterly_trends.toPandas().to_json(os.path.join(output_dir, "business_quarterly_trends.json"), orient="records", indent=2)
        monthly_trends.toPandas().to_json(os.path.join(output_dir, "business_monthly_trends.json"), orient="records", indent=2)
    
    print(f"Business trend analysis saved to: {output_dir}")

def detect_spikes_and_dips(monthly_base, threshold=1.0):
    """Detect significant rating changes."""
    w = Window.partitionBy("business_ID").orderBy("year", "month")
    return (monthly_base
            .withColumn("prev_avg_rating", lag("monthly_avg_rating").over(w))
            .withColumn("rating_diff", col("monthly_avg_rating") - col("prev_avg_rating"))
            .withColumn("spike_or_dip",
                       when(col("rating_diff") > threshold, "spike")
                       .when(col("rating_diff") < -threshold, "dip")
                       .otherwise(None))
            .filter(col("spike_or_dip").isNotNull()))

def preprocess_reviews(review_texts):
    """Preprocess review text for topic modeling."""
    return [[token for token in 
             re.sub(r'[^a-z\s]', '', text.lower()).split() 
             if token not in stop_words and len(token) > 2] 
            for text in review_texts]

def run_topic_modeling(review_texts, num_topics=3):
    """Perform LDA topic modeling."""
    texts = preprocess_reviews(review_texts)
    if not texts or all(not text for text in texts):
        return []
    
    dictionary = corpora.Dictionary(texts)
    corpus = [dictionary.doc2bow(text) for text in texts]
    lda_model = gensim.models.LdaModel(
        corpus=corpus, 
        id2word=dictionary, 
        num_topics=num_topics, 
        random_state=100, 
        passes=10
    )
    return [topic_str for _, topic_str in lda_model.print_topics(num_topics=num_topics, num_words=5)]

# def analyze_spikes_dips_topic_modeling(spark, df, flagged_df, output_dir):
#     """Analyze and save topic modeling results."""
#     ensure_output_dir(spark, output_dir)
#     results = []
    
#     for row in flagged_df.collect():
#         reviews = (df.filter((col("business_ID") == row["business_ID"]) &
#                              (col("year") == row["year"]) &
#                              (col("month") == row["month"]))
#                   .select("review_text")
#                   .rdd.flatMap(lambda x: x)
#                   .collect())
        
#         results.append({
#             "business_ID": row["business_ID"],
#             "business_name": row["business_name"],
#             "year": row["year"],
#             "month": row["month"],
#             "monthly_avg_rating": row["monthly_avg_rating"],
#             "prev_avg_rating": row["prev_avg_rating"],
#             "rating_diff": row["rating_diff"],
#             "spike_or_dip": row["spike_or_dip"],
#             "topics": run_topic_modeling(reviews) if reviews else []
#         })
    
#     output_path = os.path.join(output_dir, "spike_dip_topic_modeling.json")
#     if output_dir.startswith("hdfs://"):
#         # For HDFS output
#         spark.createDataFrame(results).write.mode("overwrite").json(output_path)
#     else:
#         # For local output
#         with open(output_path, "w") as f:
#             json.dump(results, f, indent=2)
    
#     print(f"Topic modeling results saved to: {output_path}")

def analyze_spikes_dips_topic_modeling(spark, df, flagged_df, output_dir):
    """Analyze and save topic modeling results for spikes/dips."""
    ensure_output_dir(spark, output_dir)
    results = []

    for row in flagged_df.collect():
        reviews = (df.filter((col("business_ID") == row["business_ID"]) &
                             (col("year") == row["year"]) &
                             (col("month") == row["month"]))
                  .select("review_text")
                  .rdd.flatMap(lambda x: x)
                  .collect())

        results.append({
            "business_ID": row["business_ID"],
            "business_name": row["business_name"],
            "year": row["year"],
            "month": row["month"],
            "monthly_avg_rating": row["monthly_avg_rating"],
            "prev_avg_rating": row["prev_avg_rating"],
            "rating_diff": row["rating_diff"],
            "spike_or_dip": row["spike_or_dip"],
            "topics": run_topic_modeling(reviews) if reviews else []
        })

    output_path = os.path.join(output_dir, "spike_dip_topic_modeling.json")

    if results:
        # Only write if we have results
        spark.createDataFrame(results).write.mode("overwrite").json(output_path)
        print(f"Topic modeling results saved to: {output_path}")
    else:
        print("No spike or dip data found. Skipping topic modeling JSON write.")

def main(input_path, output_dir):
    """Main execution function."""
    spark = prepare_spark_session()
    
    try:
        # Load and preprocess data
        reviews_df = load_and_preprocess_data(spark, input_path)
        
        # Compute trends and detect anomalies
        monthly_base = compute_monthly_trends(reviews_df)
        flagged_df = detect_spikes_and_dips(monthly_base)
        
        # Run analyses
        analyze_business_trends(spark, reviews_df, output_dir)
        analyze_spikes_dips_topic_modeling(spark, reviews_df, flagged_df, 
                                         os.path.join(output_dir, "topic_modeling"))
        
    except Exception as e:
        print(f"Error in time-based analysis: {str(e)}")
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main("hdfs:///input/dataset/small-raw-r-00000", "hdfs:///output/analysis/time-based")