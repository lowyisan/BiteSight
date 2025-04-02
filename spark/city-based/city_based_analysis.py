# Contributor(s): Michelle Magdalene Trisoeranto

import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, split, trim, lower, avg, count, desc, when, sum as spark_sum,
    from_json, regexp_replace, round as spark_round, array
)
from pyspark.sql.types import MapType, StringType, IntegerType

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

def save_to_hdfs_json(df_result, hdfs_path):
    df_result.coalesce(1).write.mode("overwrite").json(hdfs_path)
    print(f"Written to HDFS: {hdfs_path}")

def main(spark):
    raw_dataset = "hdfs:///input/dataset/small-aggregated-r-00000"
    output_dir = "hdfs:///output/analysis/city-based"

    output_files = {
        "top_categories_per_city.json": f"{output_dir}/top_categories_per_city.json",
        "avg_business_hours.json": f"{output_dir}/avg_business_hours.json",
        "hotspot_cities_per_category.json": f"{output_dir}/hotspot_cities_per_category.json",
        "business_details.json": f"{output_dir}/business_details.json"
    }

    df = spark.read.csv(raw_dataset, sep="\t", header=False, inferSchema=True).toDF(
        "business_id", "name", "address", "city", "state", "postal",
        "lat", "lon", "categories", "opening_hours",
        "sum_star0", "sum_star1", "sum_star2", "sum_star3", "sum_star4", "sum_star5", "sum_totalreviews"
    )

    for col_name in ["sum_star0", "sum_star1", "sum_star2", "sum_star3", "sum_star4", "sum_star5", "sum_totalreviews"]:
        df = df.withColumn(col_name, col(col_name).cast(IntegerType()))

    exploded_df = df.withColumn("category", explode(split(col("categories"), ",")))
    cleaned_categories = exploded_df.withColumn("category", trim(lower(col("category"))))

    top_categories = (
        cleaned_categories.groupBy("state", "city", "category")
        .agg(count("*").alias("business_count"))
        .orderBy("state", "city", desc("business_count"))
    )

    # Opening hours cleaning
    opening_hours_schema = MapType(StringType(), StringType())
    df = df.withColumn("opening_hours_clean", from_json(
        regexp_replace(col("opening_hours"), "'", '"'),
        opening_hours_schema
    ))

    with_hours = df.filter(col("opening_hours_clean").isNotNull())

    for day in DAYS:
        with_hours = with_hours.withColumn(
            f"{day}_open", when(col(f"opening_hours_clean.{day}").isNotNull(), 1).otherwise(0)
        )

    hours_per_business = with_hours.withColumn(
        "days_open", sum(col(f"{day}_open") for day in DAYS)
    )

    avg_hours = (
        hours_per_business.groupBy("state", "city")
        .agg(avg("days_open").alias("avg_days_open_per_business"))
    )

    hotspot = (
        cleaned_categories.groupBy("category", "state", "city")
        .agg(
            spark_sum("sum_totalreviews").alias("total_reviews"),
            spark_sum("sum_star5").alias("sum_5stars"),
            spark_sum("sum_star4").alias("sum_4stars"),
            spark_sum("sum_star3").alias("sum_3stars"),
            spark_sum("sum_star2").alias("sum_2stars"),
            spark_sum("sum_star1").alias("sum_1stars"),
            spark_sum("sum_star0").alias("sum_0stars"),
        )
        .orderBy("category", desc("total_reviews"))
    )

    # Average rating
    df = df.withColumn("avg_rating", spark_round(
        (
            col("sum_star0") * 0 +
            col("sum_star1") * 1 +
            col("sum_star2") * 2 +
            col("sum_star3") * 3 +
            col("sum_star4") * 4 +
            col("sum_star5") * 5
        ) / when(col("sum_totalreviews") > 0, col("sum_totalreviews")).otherwise(1),
        2
    ))

    for day in DAYS:
        df = df.withColumn(
            f"hour_{day}",
            col("opening_hours_clean").getItem(day)
        )

    df = df.withColumn("hours", array(
        col("hour_Monday"),
        col("hour_Tuesday"),
        col("hour_Wednesday"),
        col("hour_Thursday"),
        col("hour_Friday"),
        col("hour_Saturday"),
        col("hour_Sunday")
    ))

    business_details = df.select(
        col("business_id"),
        col("name").alias("business_name"),
        "city", "state",
        col("lat").alias("latitude"),
        col("lon").alias("longitude"),
        "sum_star0", "sum_star1", "sum_star2", "sum_star3", "sum_star4", "sum_star5",
        "sum_totalreviews",
        "avg_rating",
        "categories",
        "address",
        col("postal").alias("postal_code"),
        "hours"
    )

    save_to_hdfs_json(top_categories, output_files["top_categories_per_city.json"])
    save_to_hdfs_json(avg_hours, output_files["avg_business_hours.json"])
    save_to_hdfs_json(hotspot, output_files["hotspot_cities_per_category.json"])
    save_to_hdfs_json(business_details, output_files["business_details.json"])


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("CityBasedAnalysis") \
        .getOrCreate()

    main(spark)
    spark.stop()
