# Contributor(s): Kee Han Xiang

import os
import json
from pyspark.sql.functions import col, lit, round
from py4j.java_gateway import java_import

def process_sentiment_analysis(df):
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

def save_as_named_json(spark, data, hdfs_path):
    from pyspark.sql import Row
    from pyspark.sql import DataFrame

    # Convert list of dicts to a DataFrame directly
    rdd = spark.sparkContext.parallelize(data)
    df = spark.read.json(rdd)

    # Write to temp directory
    temp_path = hdfs_path + "_tmp"
    df.coalesce(1).write.mode("overwrite").json(temp_path)

    # Rename part file to desired file name
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    temp_path_obj = spark._jvm.Path(temp_path)
    final_path_obj = spark._jvm.Path(hdfs_path)

    for file_status in fs.listStatus(temp_path_obj):
        name = file_status.getPath().getName()
        if name.startswith("part-") and name.endswith(".json"):
            fs.rename(file_status.getPath(), final_path_obj)

    fs.delete(temp_path_obj, True)
    print(f"HDFS JSON written to: {hdfs_path}")

def main(spark):
    hdfs_input_path = 'hdfs:///input/dataset/small-aggregated-r-00000'
    hdfs_output_json_path = 'hdfs:///output/analysis/geospatial/geospatial_sentiment.json'
    local_json_output_path = './output/geospatial_sentiment.json'

    df = spark.read.csv(hdfs_input_path, sep="\t", header=False).toDF(
        "business_id", "name", "address", "city", "state", "postal",
        "lat", "lon", "categories", "opening_hours",
        "0star", "1star", "2star", "3star", "4star", "5star", "totalreview"
    )

    enriched_df = process_sentiment_analysis(df)
    json_data = convert_to_json_format(enriched_df)

    # Save to local (frontend use)
    os.makedirs("./output", exist_ok=True)
    with open(local_json_output_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)
    print(f"Local JSON written to: {local_json_output_path}")

    # Save to HDFS
    save_as_named_json(spark, json_data, hdfs_output_json_path)

# NO spark session creation here – only use when run standalone
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    main(spark)
    spark.stop()
