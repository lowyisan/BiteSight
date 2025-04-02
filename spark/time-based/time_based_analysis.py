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


# Spark Functions and Processing   #

def prepare_spark_session():
    """
    Initialize Spark session with appropriate configurations.
    """
    return (SparkSession.builder
            .appName("Yelp Business Rating Trend Analysis with Spikes & Dips")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "4g")
            .getOrCreate())

def create_yelp_schema():
    """
    Create a custom schema for the new Yelp dataset:
    business_ID, business_name, address, city, state, postal,
    lat, lon, categories, opening_hours, stars, review_text, datetime
    """
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

def load_and_preprocess_data(spark, input_path):
    """
    Load and preprocess Yelp business reviews data using the new dataset schema.
    Converts the datetime string to a timestamp and extracts year, month, and quarter.
    """
    schema = create_yelp_schema()
    df = (spark.read
          .format("csv")
          .option("sep", "\t")
          .option("header", "false")
          .schema(schema)
          .load(input_path)
          # Convert datetime string to timestamp (adjust format if necessary)
          .withColumn("review_timestamp", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss"))
          .withColumn("year", year(col("review_timestamp")).cast("integer"))
          .withColumn("month", month(col("review_timestamp")).cast("integer"))
          .withColumn("quarter", quarter(col("review_timestamp")))
          .withColumn("formatted_review_date", date_format(col("review_timestamp"), "yyyy-MM-dd HH:mm:ss"))
          .na.drop(subset=["business_ID", "stars", "datetime"])
         )
    return df

def compute_monthly_trends(df):
    """
    Return a DataFrame with columns:
    business_ID, business_name, address, city, state, postal, year, month,
    monthly_avg_rating, monthly_rating_std, review_volume
    """
    monthly_base = (
        df.groupBy(
            "business_ID", "business_name", "address", "city", "state", "postal", "year", "month"
        )
        .agg(
            round(avg("stars"), 2).alias("monthly_avg_rating"),
            coalesce(round(stddev("stars"), 2), lit(0.0)).alias("monthly_rating_std"),
            count("*").alias("review_volume")
        )
    )
    return monthly_base

def analyze_business_trends(df, output_dir):
    """
    Compute and export business-specific time-based trends (monthly and quarterly).
    """
    os.makedirs(output_dir, exist_ok=True)

    # Quarterly Trends: grouping by business_ID, business_name, city, state, postal, year, quarter
    business_quarterly_trends = (df
        .groupBy(
            col("business_ID"),
            col("business_name"),
            col("city"),
            col("state"),
            col("postal"),
            col("year"),
            quarter("review_timestamp").alias("quarter")
        )
        .agg(
            round(avg("stars"), 2).alias("avg_quarterly_rating"),
            count("*").alias("total_reviews"),
            coalesce(round(stddev("stars"), 2), lit(0.0)).alias("rating_volatility")
        )
    )

    # Monthly Trends
    business_monthly_trends = (df
        .groupBy(
            col("business_ID"),
            col("business_name"),
            col("address"),
            col("city"),
            col("state"),
            col("postal"),
            col("year"),
            col("month")
        )
        .agg(
            round(avg("stars"), 2).alias("monthly_avg_rating"),
            coalesce(round(stddev("stars"), 2), lit(0.0)).alias("monthly_rating_std"),
            count("*").alias("review_volume")
        )
    )

    # Convert to Pandas and export JSON reports
    business_quarterly_report = business_quarterly_trends.toPandas().fillna(0)
    business_monthly_report = business_monthly_trends.toPandas().fillna(0)

    quarterly_output_path = os.path.join(output_dir, 'business_quarterly_trends.json')
    monthly_output_path = os.path.join(output_dir, 'business_monthly_trends.json')

    with open(quarterly_output_path, 'w') as f:
        json.dump(business_quarterly_report.to_dict(orient="records"), f, indent=2)
    with open(monthly_output_path, 'w') as f:
        json.dump(business_monthly_report.to_dict(orient="records"), f, indent=2)

    print(f"Business trend analysis reports saved to: {output_dir}")
    return {
        "quarterly_trends": business_quarterly_report.to_dict(orient="records"),
        "monthly_trends": business_monthly_report.to_dict(orient="records")
    }

def detect_spikes_and_dips(monthly_base, threshold=1.0):
    """
    For each business, compare monthly_avg_rating with previous month.
    Returns a DataFrame with columns:
      business_ID, business_name, year, month, monthly_avg_rating, prev_avg_rating, rating_diff, spike_or_dip
    """
    w = Window.partitionBy("business_ID").orderBy("year", "month")
    df_with_prev = monthly_base.withColumn("prev_avg_rating", lag("monthly_avg_rating").over(w))
    df_with_diff = df_with_prev.withColumn("rating_diff", col("monthly_avg_rating") - col("prev_avg_rating"))
    df_flagged = df_with_diff.withColumn(
        "spike_or_dip",
        when(col("rating_diff") > threshold, lit("spike"))
        .when(col("rating_diff") < -threshold, lit("dip"))
        .otherwise(lit(None))
    )
    df_flagged = df_flagged.filter(col("spike_or_dip").isNotNull())
    return df_flagged

#####################################
# Topic Modeling Functions
#####################################

def preprocess_reviews(review_texts):
    """
    Preprocess review texts: lowercase, remove non-alphabetic characters,
    tokenize, and remove stopwords.
    """
    processed = []
    for review in review_texts:
        text = review.lower()
        text = re.sub(r'[^a-z\s]', '', text)
        tokens = text.split()
        tokens = [token for token in tokens if token not in stop_words]
        processed.append(tokens)
    return processed

def run_topic_modeling(review_texts, num_topics=3):
    """
    Run LDA topic modeling on a list of review texts using Gensim.
    Returns a list of topics with their top words.
    """
    texts = preprocess_reviews(review_texts)
    dictionary = corpora.Dictionary(texts)
    corpus = [dictionary.doc2bow(text) for text in texts]
    if len(dictionary) == 0:
        return []
    lda_model = gensim.models.LdaModel(
        corpus=corpus, 
        id2word=dictionary, 
        num_topics=num_topics, 
        random_state=100, 
        passes=10
    )
    topics = lda_model.print_topics(num_topics=num_topics, num_words=5)
    return [topic_str for idx, topic_str in topics]

def analyze_spikes_dips_topic_modeling(spark, df, flagged_df, output_dir):
    """
    For each flagged row (spike or dip), gather review_texts for that business, year, and month,
    run topic modeling, and export the results as JSON.
    """
    flagged_pd = flagged_df.toPandas()
    results = []
    for _, row in flagged_pd.iterrows():
        biz_id = row["business_ID"]
        year_ = row["year"]
        month_ = row["month"]
        spike_or_dip = row["spike_or_dip"]
        rating_diff = row["rating_diff"]
        monthly_avg = row["monthly_avg_rating"]
        prev_avg = row["prev_avg_rating"]
        biz_name = row["business_name"]
        reviews_sdf = df.filter(
            (col("business_ID") == biz_id) &
            (col("year") == year_) &
            (col("month") == month_)
        )
        reviews = reviews_sdf.select("review_text").rdd.flatMap(lambda x: x).collect()
        if len(reviews) > 0:
            topics = run_topic_modeling(reviews, num_topics=3)
        else:
            topics = []
        results.append({
            "business_ID": biz_id,
            "business_name": biz_name,
            "year": int(year_),
            "month": int(month_),
            "monthly_avg_rating": float(monthly_avg) if monthly_avg else None,
            "prev_avg_rating": float(prev_avg) if prev_avg else None,
            "rating_diff": float(rating_diff) if rating_diff else None,
            "spike_or_dip": spike_or_dip,
            "topics": topics
        })
    spike_dip_output_path = os.path.join(output_dir, "spike_dip_topic_modeling.json")
    with open(spike_dip_output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Spike/Dip topic modeling results saved to: {spike_dip_output_path}")


def main(input_path, output_dir):
    spark = prepare_spark_session()
    # Load and preprocess data
    reviews_df = load_and_preprocess_data(spark, input_path)
    
    # Compute monthly trends
    monthly_base = compute_monthly_trends(reviews_df)
    
    # Detect spikes and dips
    flagged_df = detect_spikes_and_dips(monthly_base, threshold=1.0)
    
    # Run topic modeling for flagged months and export results
    analyze_spikes_dips_topic_modeling(spark, reviews_df, flagged_df, output_dir)
    
    # Also run and export business trends (monthly and quarterly)
    analyze_business_trends(reviews_df, output_dir)


if __name__ == "__main__":
    main("hdfs:///input/dataset/small-r-00000", "hdfs:///output/analysis/time-based")
