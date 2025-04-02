import os
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType
from py4j.java_gateway import java_import

def create_yelp_schema():
    return StructType([
        StructField("business_id", StringType(), True),
        StructField("name", StringType(), True),
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

def save_summary_as_single_json(spark, summary_df, hdfs_output_dir, filename):
    """Save summary KPIs as a single JSON file to HDFS."""
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    json_list = summary_df.to_dict(orient="records")
    tmp_path = f"{hdfs_output_dir}/.tmp_{filename}"
    final_path = f"{hdfs_output_dir}/{filename}"

    df = spark.createDataFrame(json_list)
    df.coalesce(1).write.mode("overwrite").json(tmp_path)

    # Rename part file to actual file name
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    tmp_dir = spark._jvm.Path(tmp_path)
    target_path = spark._jvm.Path(final_path)

    for file_status in fs.listStatus(tmp_dir):
        name = file_status.getPath().getName()
        if name.startswith("part-") and name.endswith(".json"):
            fs.rename(file_status.getPath(), target_path)
            break

    fs.delete(tmp_dir, True)
    print(f"[HDFS] Written: {final_path}")

def calculate_summary_kpis(spark, file_path, output_dir):
    df = spark.read.csv(file_path, schema=create_yelp_schema(), header=False, sep="\t")
    
    total_businesses = df.select("business_id").distinct().count()
    total_reviews = df.count()
    avg_rating = df.select(avg("stars")).first()[0]
    num_states = df.select("state").distinct().count()
    num_cities = df.select("city").distinct().count()

    summary_df = pd.DataFrame({
        'metric': [
            'total_businesses', 
            'total_reviews', 
            'num_states', 
            'num_cities', 
            'overall_avg_rating'
        ],
        'value': [
            f"{total_businesses:,}", 
            f"{total_reviews:,}",
            f"{num_states:,}",
            f"{num_cities:,}",
            round(avg_rating, 2) if avg_rating is not None else 0
        ]
    })

    save_summary_as_single_json(spark, summary_df, output_dir, "yelp_summary_kpis.json")
    return summary_df

def main():
    spark = SparkSession.builder.appName("YelpSummaryKPIsExport").getOrCreate()
    input_file = "hdfs:///input/dataset/small-raw-r-00000"
    output_directory = "hdfs:///output/analysis/summary"

    calculate_summary_kpis(spark, input_file, output_directory)

if __name__ == "__main__":
    main()
