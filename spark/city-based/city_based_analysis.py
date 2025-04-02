# Contributor(s): Michelle Magdalene Trisoeranto

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, split, trim, lower, avg, count, desc, when, sum as spark_sum,
    from_json, regexp_replace, round as spark_round, array
)
from pyspark.sql.types import MapType, StringType, IntegerType

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

def write_as_single_json_file(df, hdfs_path):
    """Write DataFrame as a single .json file with actual filename."""
    temp_path = hdfs_path + "_tmp"
    df.coalesce(1).write.mode("overwrite").json(temp_path)

    from py4j.java_gateway import java_import
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    output_path = spark._jvm.Path(hdfs_path)
    temp_output_path = spark._jvm.Path(temp_path)

    # Remove old if exists
    if fs.exists(output_path):
        fs.delete(output_path, True)

    # Rename the generated part file to final .json name
    for fileStatus in fs.listStatus(temp_output_path):
        name = fileStatus.getPath().getName()
        if name.startswith("part-") and name.endswith(".json"):
            fs.rename(fileStatus.getPath(), output_path)

    fs.delete(temp_output_path, True)
    print(f"✔ Wrote: {hdfs_path}")

def main(spark):
    raw_dataset = "hdfs:///input/dataset/small-aggregated-r-00000"
    output_dir = "hdfs:///output/analysis/city-based"

    df = spark.read.csv(raw_dataset, sep="\t", header=False, inferSchema=True).toDF(
        "business_id", "name", "address", "city", "state", "postal",
        "lat", "lon", "categories", "opening_hours",
        "sum_star0", "sum_star1", "sum_star2", "sum_star3", "sum_star4", "sum_star5", "sum_totalreviews"
    )

    for col_name in ["sum_star0", "sum_star1", "sum_star2", "sum_star3", "sum_star4", "sum_star5", "sum_totalreviews"]:
        df = df.withColumn(col_name, col(col_name).cast(IntegerType()))

    # ===== Top Categories per City =====
    exploded_df = df.withColumn("category", explode(split(col("categories"), ",")))
    cleaned_categories = exploded_df.withColumn("category", trim(lower(col("category"))))
    top_categories = (
        cleaned_categories.groupBy("state", "city", "category")
        .agg(count("*").alias("business_count"))
        .orderBy("state", "city", desc("business_count"))
    )
    write_as_single_json_file(top_categories, f"{output_dir}/top_categories_per_city.json")

    # ===== Average Business Hours per City =====
    opening_hours_schema = MapType(StringType(), StringType())
    df = df.withColumn("opening_hours_clean", from_json(
        regexp_replace(col("opening_hours"), "'", '"'), opening_hours_schema
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
    write_as_single_json_file(avg_hours, f"{output_dir}/avg_business_hours.json")

    # ===== Hotspot Cities per Category =====
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
    write_as_single_json_file(hotspot, f"{output_dir}/hotspot_cities_per_category.json")

    # ===== Business Details =====
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
        df = df.withColumn(f"hour_{day}", col("opening_hours_clean").getItem(day))
    df = df.withColumn("hours", array(
        col("hour_Monday"), col("hour_Tuesday"), col("hour_Wednesday"),
        col("hour_Thursday"), col("hour_Friday"), col("hour_Saturday"), col("hour_Sunday")
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
    write_as_single_json_file(business_details, f"{output_dir}/business_details.json")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CityBasedAnalysis").getOrCreate()
    main(spark)
    spark.stop()

