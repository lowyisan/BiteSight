#!/usr/bin/env python3
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
    date_format, coalesce, lit, collect_list, lag, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
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

#####################
# Spark Functions   #
#####################

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

def load_and_preprocess_data(spark, input_path):
    """
    Load and preprocess Yelp business reviews data.
    """
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


def compute_monthly_trends(df):
    """
    Return a DataFrame with columns:
    business_ID, business_name, city, state, year, month,
    monthly_avg_rating, monthly_rating_std, review_volume
    """
    monthly_base = (
        df.groupBy(
            "business_ID", "business_name", "city", "state", "year", "month"
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
    Perform comprehensive business-specific time-based analysis
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Business-level Quarterly Trends
    business_quarterly_trends = (df
        .groupBy(
            col("business_ID"),
            col("business_name"),
            col("year"),
            quarter("review_date").alias("quarter")
        )
        .agg(
            round(avg("stars"), 2).alias("avg_quarterly_rating"),
            count("*").alias("total_reviews"),
            # Use coalesce to replace potential NaN with 0
            coalesce(round(stddev("stars"), 2), lit(0.0)).alias("rating_volatility")
        )
    )

    # Business-level Monthly Trends
    business_monthly_trends = (df
        .groupBy(
            col("business_ID"),
            col("business_name"),
            col("year"),
            col("month")
        )
        .agg(
            round(avg("stars"), 2).alias("monthly_avg_rating"),
            # Use coalesce to replace potential NaN with 0
            coalesce(round(stddev("stars"), 2), lit(0.0)).alias("monthly_rating_std"),
            count("*").alias("review_volume")
        )
    )


    # Convert to Pandas and save reports
    business_quarterly_report = business_quarterly_trends.toPandas().fillna(0)
    business_monthly_report = business_monthly_trends.toPandas().fillna(0)


    # Prepare detailed reporting dictionary
    business_trend_report = {
        "quarterly_trends": business_quarterly_report.to_dict(orient="records"),
        "monthly_trends": business_monthly_report.to_dict(orient="records"),
    }

    # Save individual reports
    quarterly_output_path = os.path.join(output_dir, 'business_quarterly_trends.json')
    monthly_output_path = os.path.join(output_dir, 'business_monthly_trends.json')

    # Save JSON reports
    with open(quarterly_output_path, 'w') as f:
        json.dump(business_quarterly_report.to_dict(orient="records"), f, indent=2)
    with open(monthly_output_path, 'w') as f:
        json.dump(business_monthly_report.to_dict(orient="records"), f, indent=2)

    print(f"Business trend analysis reports saved to: {output_dir}")
    return business_trend_report

def detect_spikes_and_dips(monthly_base, threshold=1.0):
    """
    For each business, compare monthly_avg_rating with previous month.
    If difference is > +threshold => 'spike', if < -threshold => 'dip'.
    Returns a DataFrame with columns:
      business_ID, year, month, monthly_avg_rating, prev_avg_rating, rating_diff, spike_or_dip
    """
    # Window partitioned by business, ordered by year and month
    w = Window.partitionBy("business_ID").orderBy("year", "month")

    # Add previous month's rating
    df_with_prev = monthly_base.withColumn(
        "prev_avg_rating",
        lag("monthly_avg_rating").over(w)
    )

    # rating_diff = current - previous
    df_with_diff = df_with_prev.withColumn(
        "rating_diff",
        col("monthly_avg_rating") - col("prev_avg_rating")
    )

    # Define spike_or_dip
    # e.g., threshold=1.0 => if rating_diff > 1 => spike, if rating_diff < -1 => dip
    df_flagged = df_with_diff.withColumn(
        "spike_or_dip",
        when(col("rating_diff") > threshold, lit("spike"))
        .when(col("rating_diff") < -threshold, lit("dip"))
        .otherwise(lit(None))
    )

    # Filter out rows that are neither spike nor dip
    df_flagged = df_flagged.filter(col("spike_or_dip").isNotNull())

    return df_flagged
    

##############################
# Topic Modeling Functions  #
##############################

def preprocess_reviews(review_texts):
    """
    Preprocess review texts: lowercase, remove non-alphabetic characters, tokenize, remove stopwords.
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
    For each flagged row (spike or dip), gather the review texts from the original df
    for that business + year + month, run topic modeling, and store the result.
    """
    # Convert flagged_df to Pandas so we can iterate
    flagged_pd = flagged_df.toPandas()

    # We'll store results in a list of dict
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
        city = row["city"]
        state = row["state"]

        # Filter original df for that business and month
        # We'll gather all the review_text for that month
        reviews_sdf = df.filter(
            (col("business_ID") == biz_id) &
            (col("year") == year_) &
            (col("month") == month_)
        )

        # Collect the reviews
        reviews = reviews_sdf.select("review_text").rdd.flatMap(lambda x: x).collect()

        # Run topic modeling on these reviews
        if len(reviews) > 0:
            topics = run_topic_modeling(reviews, num_topics=3)
        else:
            topics = []

        results.append({
            "business_ID": biz_id,
            "business_name": biz_name,
            "city": city,
            "state": state,
            "year": int(year_),
            "month": int(month_),
            "monthly_avg_rating": float(monthly_avg) if monthly_avg else None,
            "prev_avg_rating": float(prev_avg) if prev_avg else None,
            "rating_diff": float(rating_diff) if rating_diff else None,
            "spike_or_dip": spike_or_dip,
            "topics": topics
        })

    # Write to JSON
    spike_dip_output_path = os.path.join(output_dir, "spike_dip_topic_modeling.json")
    with open(spike_dip_output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"Spike/Dip topic modeling results saved to: {spike_dip_output_path}")


def main(input_path, output_dir):
    spark = prepare_spark_session()
    try:
        # load data
        reviews_df = load_and_preprocess_data(spark, input_path)

        # compute monthly trends
        monthly_base = compute_monthly_trends(reviews_df)

        # detect spikes and dips
        flagged_df = detect_spikes_and_dips(monthly_base, threshold=1.0)

        # run topic modeling for flagged months
        analyze_spikes_dips_topic_modeling(spark, reviews_df, flagged_df, output_dir)

        # monthly/quarterly analysis
        analyze_business_trends(reviews_df, output_dir)

    except Exception as e:
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main("../dataset/small-r-00000", "output")
