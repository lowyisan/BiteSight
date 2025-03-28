from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, split, trim, lower, avg, count, desc, when, sum as spark_sum
)

# ==== PATH CONFIGS ====
aggregated_path = "s3://sg.edu.sit.inf2006.group14/aggregated-r-00000"
yelp_json_path = "s3://sg.edu.sit.inf2006.group14/yelp_dataset/yelp_academic_dataset_business.json"
city_enriched_dataset = "s3://sg.edu.sit.inf2006.group14/final-output/enriched_businesses/part-00000-14744aaa-4b29-41eb-a31f-63f036c0d618-c000.json"

output_path_top_categories = "s3://sg.edu.sit.inf2006.group14/final-output/analysis/top_categories_per_city"
output_path_avg_hours = "s3://sg.edu.sit.inf2006.group14/final-output/analysis/avg_business_hours"
output_path_hotspot_categories = "s3://sg.edu.sit.inf2006.group14/final-output/analysis/hotspot_cities_per_category"

# ==== SPARK INIT ====
spark = SparkSession.builder.appName("CityBasedAnalysis").getOrCreate()

# ==== LOAD ENRICHED JSON ====
df = spark.read.json(city_enriched_dataset)

# ============ 1. TOP CATEGORIES PER CITY ============
exploded_df = df.withColumn("category", explode(split(col("categories"), ",")))
cleaned_categories = exploded_df.withColumn("category", trim(lower(col("category"))))

top_categories = (
    cleaned_categories.groupBy("city", "category")
    .agg(count("*").alias("business_count"))
    .orderBy("city", desc("business_count"))
)

top_categories.write.mode("overwrite").json(output_path_top_categories)

# ============ 2. AVERAGE BUSINESS HOURS / AVAILABILITY ============
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
with_hours = df.filter(col("hours").isNotNull())

hours_per_business = with_hours.withColumn(
    "days_open",
    sum(when(col(f"hours.{day}").isNotNull(), 1).otherwise(0) for day in DAYS)
)

avg_hours = (
    hours_per_business.groupBy("city")
    .agg(avg("days_open").alias("avg_days_open_per_business"))
)

avg_hours.write.mode("overwrite").json(output_path_avg_hours)

# ============ 3. HOTSPOT CITIES FOR ALL CATEGORIES ============
hotspot = (
    cleaned_categories.groupBy("category", "city")
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

hotspot.write.mode("overwrite").json(output_path_hotspot_categories)

# ==== DONE ====
spark.stop()