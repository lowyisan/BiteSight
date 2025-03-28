import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType
import pandas as pd

def create_yelp_schema():
    """
    Create a custom schema for the Yelp dataset
    """
    return StructType([
        StructField("business_ID", StringType(), True),
        StructField("business_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("lat", FloatType(), True),
        StructField("lon", FloatType(), True),
        StructField("star", DoubleType(), True),
        StructField("review_text", StringType(), True),
        StructField("review_date", StringType(), True)
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
    
    # Read CSV with explicit schema
    df = spark.read.csv(
        file_path, 
        schema=create_yelp_schema(),
        header=False,  # Assuming no header in the file
        sep='\t'  # Tab-separated values
    )
    
    # Calculate KPIs
    total_businesses = df.select("business_ID").distinct().count()
    total_reviews = df.count()
    avg_rating = df.select(avg("star")).first()[0]
    
    # Create a pandas DataFrame for easy CSV export
    summary_df = pd.DataFrame({
        'metric': ['total_businesses', 'total_reviews', 'overall_avg_rating'],
        'value': [
            f"{total_businesses:,}", 
            f"{total_reviews:,}", 
            round(avg_rating, 2)
        ]
    })
    
    # Export to CSV
    output_path = os.path.join(output_dir, 'yelp_summary_kpis.csv')
    summary_df.to_csv(output_path, index=False)
    
    print(f"Summary KPIs exported to: {output_path}")
    
    return summary_df

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("YelpSummaryKPIsExport") \
        .getOrCreate()
    
    try:
        # Adjust these paths as needed
        input_file = "../dataset/small-r-00000"
        output_directory = "output"
        
        calculate_summary_kpis(spark, input_file, output_directory)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()