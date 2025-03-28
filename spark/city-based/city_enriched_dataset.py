from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import boto3

# ==== PATH CONFIGS ====
aggregated_path = "s3://sg.edu.sit.inf2006.group14/aggregated-r-00000"
yelp_json_path = "s3://sg.edu.sit.inf2006.group14/yelp_dataset/yelp_academic_dataset_business.json"
city_enriched_dataset = "/home/hadoop/enriched_businesses.json"

# ==== INIT SPARK ====
spark = SparkSession.builder.appName("EnrichYelpAggregatedWithCategoriesAndHours").getOrCreate()

# ==== LOAD AGGREGATED FILE ====
aggregated_df = spark.read.option("delimiter", "\t").csv(aggregated_path).toDF(
    "business_id", "business_name", "city", "state", "latitude", "longitude",
    "sum_star0", "sum_star1", "sum_star2", "sum_star3", "sum_star4", "sum_star5", "sum_totalreviews"
)

# ==== ADD AVERAGE RATING ====
aggregated_df = aggregated_df.withColumn(
    "avg_rating",
    (
     	1 * col("sum_star1").cast("int") +
        2 * col("sum_star2").cast("int") +
        3 * col("sum_star3").cast("int") +
        4 * col("sum_star4").cast("int") +
        5 * col("sum_star5").cast("int")
    ) / col("sum_totalreviews").cast("int")
)

# ==== LOAD YELP DATA ====
yelp_df = spark.read.json(yelp_json_path).select("business_id", "categories", "hours")

# ==== JOIN ====
joined_df = aggregated_df.join(yelp_df, on="business_id", how="left")

# ==== CONVERT TO SINGLE JSON FILE ====
# 1. Convert to Pandas DataFrame (only works if < ~1M rows)
pandas_df = joined_df.toPandas()

# 2. Save as pretty JSON array
pandas_df.to_json(city_enriched_dataset, orient="records", indent=2)

# 3. (Optional) Upload to S3 if needed (e.g., using boto3)
s3 = boto3.client('s3')
s3.upload_file(city_enriched_dataset, "sg.edu.sit.inf2006.group14", "final-output/city_enriched_business.json")

# ==== DONE ====
spark.stop()