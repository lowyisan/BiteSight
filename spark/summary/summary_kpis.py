import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType
import pandas as pd

def create_yelp_schema():
    """
    Create a custom schema for the new Yelp dataset:
    business_id, name, address, city, state, postal,
    lat, lon, categories, opening_hours, stars, review_text, datetime
    """
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

def calculate_summary_kpis(spark, file_path, output_dir):
    """
    Calculate summary KPIs and export to CSV
    
    Parameters:
    spark (SparkSession): Active Spark session
    file_path (str): Input data file path
    output_dir (str): Directory to save output CSV
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Read TSV with the new schema
    df = spark.read.csv(
        file_path, 
        schema=create_yelp_schema(),
        header=False,  # Assuming no header in the file
        sep='\t'       # Tab-separated values
    )
    
    # Calculate KPIs
    total_businesses = df.select("business_id").distinct().count()
    total_reviews = df.count()
    avg_rating = df.select(avg("stars")).first()[0]

    # Also count number of distinct states and cities
    num_states = df.select("state").distinct().count()
    num_cities = df.select("city").distinct().count()
    
    # Create a pandas DataFrame for easy CSV export
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
    
    # Convert to CSV string
    csv_str = summary_df.to_csv(index=False)
    # Strip trailing newline so the file doesn't end with "\n"
    csv_str = csv_str.rstrip('\n')
    
    # Write CSV string to file without a trailing newline
    output_path = os.path.join(output_dir, 'yelp_summary_kpis.csv')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(csv_str)
    
    print(f"Summary KPIs exported to: {output_path}")
    
    return summary_df

def main():
    # Initialize Spark Session
    spark = (SparkSession.builder
        .appName("YelpSummaryKPIsExport")
        .getOrCreate()
    )
    
    try:
        input_file = "../dataset/small-raw-r-00000"
        output_directory = "output"
        
        calculate_summary_kpis(spark, input_file, output_directory)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
