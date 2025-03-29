import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round

# Step 1: Convert MapReduce TSV output to CSV
input_file_path = 'spark/dataset/small-aggregated-r-00000'  # TSV input
intermediate_csv_path = 'output.csv'          # Intermediate CSV output

with open(input_file_path, 'r', encoding='utf-8-sig') as infile, open(intermediate_csv_path, 'w', newline='', encoding='utf-8-sig') as outfile:
    reader = infile.readlines()
    writer = csv.writer(outfile)
    
    # Write header
    writer.writerow([
        'business_id', 'name', 'address', 'city', 'state', 'postal',
        'lat', 'lon', 'categories', 'opening_hours',
        '0star', '1star', '2star', '3star', '4star', '5star', 'totalreview'
    ])
    
    # Write rows
    for line in reader:
        row = line.strip().split("\t")
        writer.writerow(row)

print(f"[STEP 1] MapReduce TSV converted to CSV: {intermediate_csv_path}")

# Step 2: Start Spark session
spark = SparkSession.builder \
    .appName("Business Sentiment Scoring") \
    .getOrCreate()

# Step 3: Load CSV into Spark
df = spark.read.csv(intermediate_csv_path, header=True, inferSchema=True)

# Step 4: Compute sentiment counts
df_with_sentiment = df.withColumn("negative_count", col("0star") + col("1star") + col("2star")) \
                      .withColumn("neutral_count", col("3star")) \
                      .withColumn("positive_count", col("4star") + col("5star"))

# Step 5: Compute sentiment score
df_with_sentiment = df_with_sentiment.withColumn(
    "sentiment_score",
    round(
        (col("positive_count") - col("negative_count")) / col("totalreview"),
        3
    )
)

# Step 6: Compute average star rating
df_with_sentiment = df_with_sentiment.withColumn(
    "avg_star_rating",
    round(
        (col("1star") * 1 + col("2star") * 2 + col("3star") * 3 + col("4star") * 4 + col("5star") * 5) / col("totalreview"),
        2
    )
)

# Step 7: Select columns
output_df = df_with_sentiment.select(
    "business_id", "name", "address", "city", "state", "postal",
    "lat", "lon", "categories", "opening_hours",
    "0star", "1star", "2star", "3star", "4star", "5star", "totalreview",
    "negative_count", "neutral_count", "positive_count",
    "sentiment_score", "avg_star_rating"
)

# Step 8: Save to final output
output_path = "spark/geospatial-sentiment/output/business_sentiment"
output_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

print(f"[STEP 2-8] Spark processing complete! Enriched sentiment data saved to: {output_path}")
