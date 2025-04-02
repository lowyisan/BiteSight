# Contributor(s): Kee Han Xiang

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round

def process_sentiment_analysis(df):
    """
    Given a Spark DataFrame with review counts per star,
    this function calculates negative, neutral, and positive review counts,
    sentiment score, and average star rating.
    """
    df_with_sentiment = df.withColumn("negative_count", col("0star") + col("1star") + col("2star")) \
        .withColumn("neutral_count", col("3star")) \
        .withColumn("positive_count", col("4star") + col("5star")) \
        .withColumn("sentiment_score",
                    round((col("positive_count") - col("negative_count")) / col("totalreview"), 3)) \
        .withColumn("avg_star_rating",
                    round((col("1star") * 1 + col("2star") * 2 + col("3star") * 3 +
                           col("4star") * 4 + col("5star") * 5) / col("totalreview"), 2))

    return df_with_sentiment.select(
        "business_id", "name", "address", "city", "state", "postal",
        "lat", "lon", "categories", "opening_hours",
        "0star", "1star", "2star", "3star", "4star", "5star", "totalreview",
        "negative_count", "neutral_count", "positive_count",
        "sentiment_score", "avg_star_rating"
    )


def convert_to_json_format(df_spark):
    """
    Converts the final Spark DataFrame into a JSON array structure.
    Returns a Python list of dictionaries.
    """
    df_local = df_spark.select(
        "name", "lat", "lon", "sentiment_score", "address", "state",
        "postal", "avg_star_rating", "categories", "opening_hours"
    ).toPandas()

    json_data = []
    for _, row in df_local.iterrows():
        json_data.append({
            "name": row["name"],
            "lat": row["lat"],
            "lon": row["lon"],
            "weight": row["sentiment_score"],
            "address": row["address"],
            "state": row["state"],
            "postal": row["postal"],
            "avg_star_rating": row["avg_star_rating"],
            "categories": row["categories"],
            "opening_hours": row["opening_hours"]
        })

    return json_data


def main(spark):
    # === Paths ===
    hdfs_input_path = 'hdfs:///input/dataset/small-aggregated-r-00000'
    hdfs_output_csv_path = 'hdfs:///output/analysis/geospatial/business_sentiment'
    local_json_output_path = './output/geospatial_sentiment.json'  # For frontend use or download

    # === STEP 1: Load data from HDFS ===
    df = spark.read.csv(hdfs_input_path, sep="\t", header=False)

    df = df.toDF(
        "business_id", "name", "address", "city", "state", "postal",
        "lat", "lon", "categories", "opening_hours",
        "0star", "1star", "2star", "3star", "4star", "5star", "totalreview"
    )

    # === STEP 2: Compute sentiment score & average rating ===
    enriched_df = process_sentiment_analysis(df)

    # === STEP 3: Save enriched DataFrame to HDFS CSV ===
    enriched_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(hdfs_output_csv_path)
    print(f"CSV written to: {hdfs_output_csv_path}")

    # === STEP 4: Convert to frontend-friendly JSON (LOCAL) ===
    json_data = convert_to_json_format(enriched_df)

    # Save to local file system for use in frontend
    import os
    os.makedirs("./output", exist_ok=True)

    with open(local_json_output_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)

    print(f"Local JSON written to: {local_json_output_path}")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("GeospatialSentimentAnalysis") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    main(spark)
    spark.stop()
