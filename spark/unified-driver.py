# Contributor(s): Jeremy Lim Jie En

from pyspark.sql import SparkSession

def run_city_based_analysis(spark):
    import city_based_analysis
    city_based_analysis.main(spark)

def run_content_based_filtering(spark):
    import content_based_filtering
    content_based_filtering.main()

def run_geospatial_sentiment(spark):
    import geospatial_sentiment_analysis
    geospatial_sentiment_analysis.main(spark)

def run_vader_sentiment_analysis(spark):
    import sentiment
    sentiment.main(spark)

def run_summary_kpis(spark):
    import summary_kpis
    summary_kpis.main()

def run_time_trend_topic_modeling(spark):
    import time_based_analysis
    time_based_analysis.main(spark, "hdfs:///input/dataset/small-raw-r-00000", "hdfs:///output/analysis/time-based")

def main():
    spark = SparkSession.builder.appName("Unified Yelp Analysis Pipeline").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").getOrCreate()

    try:
        run_city_based_analysis(spark)
        run_content_based_filtering(spark)
        run_geospatial_sentiment(spark)
        run_vader_sentiment_analysis(spark)
        run_summary_kpis(spark)
        run_time_trend_topic_modeling(spark)
    finally:
        spark.stop()
        print("All modules completed successfully.")

if __name__ == "__main__":
    main()
