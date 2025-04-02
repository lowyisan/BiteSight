# Contributor(s): Kee Han Xiang

import os
import shutil
import glob
import csv
import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round

def main(spark):
    # === File Paths ===
    input_file_path = 'hdfs:///input/dataset/small-aggregated-r-00000'
    intermediate_csv_path = 'output.csv'
    final_csv_path = 'hdfs:///output/analysis/geospatial/business_sentiment.csv'
    json_output_path = 'hdfs:///output/analysis/geospatial_sentiment.json'

    # === STEP 1: Convert MapReduce Output to CSV ===
    with open(input_file_path, 'r', encoding='utf-8-sig') as infile, open(intermediate_csv_path, 'w', newline='', encoding='utf-8-sig') as outfile:
        reader = infile.readlines()
        writer = csv.writer(outfile)

        writer.writerow([
            'business_id', 'name', 'address', 'city', 'state', 'postal',
            'lat', 'lon', 'categories', 'opening_hours',
            '0star', '1star', '2star', '3star', '4star', '5star', 'totalreview'
        ])

        for line in reader:
            row = line.strip().split("\t")
            writer.writerow(row)

    print("Step 1: MapReduce output converted to output.csv")

    # === STEP 2: Spark - Sentiment & Average Rating Computation ===
    df = spark.read.csv(intermediate_csv_path, header=True, inferSchema=True)

    df_with_sentiment = df.withColumn("negative_count", col("0star") + col("1star") + col("2star")) \
                        .withColumn("neutral_count", col("3star")) \
                        .withColumn("positive_count", col("4star") + col("5star")) \
                        .withColumn("sentiment_score",
                                    round((col("positive_count") - col("negative_count")) / col("totalreview"), 3)) \
                        .withColumn("avg_star_rating",
                                    round((col("1star") * 1 + col("2star") * 2 + col("3star") * 3 +
                                            col("4star") * 4 + col("5star") * 5) / col("totalreview"), 2))

    output_df = df_with_sentiment.select(
        "business_id", "name", "address", "city", "state", "postal",
        "lat", "lon", "categories", "opening_hours",
        "0star", "1star", "2star", "3star", "4star", "5star", "totalreview",
        "negative_count", "neutral_count", "positive_count",
        "sentiment_score", "avg_star_rating"
    )

    # Save as CSV for JSON conversion
    output_df.coalesce(1).write.csv("temp_output", header=True, mode="overwrite")

    # Move the single CSV file from Spark output dir to final name
    # Find the part file
    part_file = glob.glob('temp_output/part-*.csv')[0]
    shutil.move(part_file, final_csv_path)
    shutil.rmtree('temp_output')

    print("Step 2: Spark sentiment & star rating saved to business_sentiment.csv")

    # === STEP 3: Convert Final CSV to JSON ===
    df_final = pd.read_csv(final_csv_path)

    data = []
    for _, row in df_final.iterrows():
        data.append({
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

    # with open("json_output.json", "w", encoding='utf-8') as f:
    with open(json_output_path, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("Step 3: Final JSON saved to sentiment.json ")
    
if __name__ == "__main__":
    main()