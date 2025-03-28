from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, split, trim, lower, avg, count, desc, when, sum as spark_sum
)
import json
import boto3

# ==== PATH CONFIGS ====
aggregated_path = "s3://sg.edu.sit.inf2006.group14/aggregated-r-00000"
yelp_json_path = "s3://sg.edu.sit.inf2006.group14/yelp_dataset/yelp_academic_dataset_business.json"
city_enriched_dataset = "s3://sg.edu.sit.inf2006.group14/final-output/enriched_businesses/part-00000-14744aaa-4b29-41eb-a31f-63f036c0d618-c000.json"

bucket = "sg.edu.sit.inf2006.group14"
output_prefix = "final-output/analysis/"
output_files = {
    "top_categories_per_city.json": "/tmp/top_categories_per_city.json",
    "avg_business_hours.json": "/tmp/avg_business_hours.json",
    "hotspot_cities_per_category.json": "/tmp/hotspot_cities_per_category.json"
}

# ==== SPARK INIT ====
spark = SparkSession.builder.appName("CityBasedAnalysis").getOrCreate()
df = spark.read.json(city_enriched_dataset)

# ============ 1. TOP CATEGORIES PER CITY ============
exploded_df = df.withColumn("category", explode(split(col("categories"), ",")))
cleaned_categories = exploded_df.withColumn("category", trim(lower(col("category"))))

top_categories = (
    cleaned_categories.groupBy("state", "city", "category")
    .agg(count("*").alias("business_count"))
    .orderBy("state", "city", desc("business_count"))
)

# ============ 2. AVERAGE BUSINESS HOURS ============
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
with_hours = df.filter(col("hours").isNotNull())

hours_per_business = with_hours.withColumn(
    "days_open",
    sum(when(col(f"hours.{day}").isNotNull(), 1).otherwise(0) for day in DAYS)
)

avg_hours = (
    hours_per_business.groupBy("state", "city")
    .agg(avg("days_open").alias("avg_days_open_per_business"))
)

# ============ 3. HOTSPOT CITIES FOR ALL CATEGORIES ============
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

# ============ WRITE TO SINGLE-FILE JSON OUTPUTS ============

def save_as_single_json(df_result, local_path, s3_key):
    collected = df_result.toJSON().collect()
    parsed = [json.loads(x) for x in collected]

    with open(local_path, "w") as f:
        json.dump(parsed, f, indent=2)

    boto3.client("s3").upload_file(local_path, bucket, output_prefix + s3_key)

save_as_single_json(top_categories, output_files["top_categories_per_city.json"], "top_categories_per_city.json")
save_as_single_json(avg_hours, output_files["avg_business_hours.json"], "avg_business_hours.json")
save_as_single_json(hotspot, output_files["hotspot_cities_per_category.json"], "hotspot_cities_per_category.json")

spark.stop()
