from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, row_number, collect_list, struct
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.linalg import Vectors
from pyspark.sql.window import Window
from pyspark.ml.stat import Summarizer

def main():
    # ----------------------------------------
    # 1. Start Spark Session
    # ----------------------------------------
    spark = SparkSession.builder \
        .appName("LocalContentRecommender") \
        .getOrCreate()

    # ----------------------------------------
    # 2. Load ONLY the raw data
    # ----------------------------------------
    raw_columns = [
        "business_ID", "business_name", "city", "state", "lat", "lon",
        "star", "review_text", "review_date"
    ]

    raw_df = (
        spark.read.csv(
            "../dataset/small-r-00000",
            sep="\t",
            header=False,
            inferSchema=True
        )
        .toDF(*raw_columns)

    )

    # ----------------------------------------
    # 3. Text Preprocessing → TF-IDF
    # ----------------------------------------
    # Tokenize
    tokenizer = Tokenizer(inputCol="review_text", outputCol="words")
    words_df = tokenizer.transform(raw_df)

    # Remove stopwords
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    filtered_df = remover.transform(words_df)

    # Convert words to raw TF vectors
    hashingTF = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=1000)
    tf_df = hashingTF.transform(filtered_df)

    # Compute IDF and create TF-IDF features
    idf = IDF(inputCol="raw_features", outputCol="tfidf_features")
    idf_model = idf.fit(tf_df)
    tfidf_df = idf_model.transform(tf_df)

    # ----------------------------------------
    # 4. Aggregate TF-IDF Per Business
    # ----------------------------------------
    business_profiles = (
        tfidf_df.groupBy("business_ID")
        .agg(
            Summarizer.mean(col("tfidf_features")).alias("profile_vector"),
            first("business_name").alias("business_name"),
            first("city").alias("city"),
            first("state").alias("state")
        )
        .dropna(subset=["profile_vector"])
    )

    # ----------------------------------------
    # 5. Set Up Cross Join
    # ----------------------------------------
    a = business_profiles.alias("a")
    b = business_profiles.alias("b")

    # Define cosine similarity udf
    def cosine_sim(v1, v2):
        if not v1 or not v2:
            return 0.0
        dot = float(v1.dot(v2))
        norm = float(v1.norm(2) * v2.norm(2))
        return dot / norm if norm != 0 else 0.0

    cosine_udf = spark.udf.register("cosine_sim", cosine_sim, DoubleType())

    # ----------------------------------------
    # 6. Cross Join Businesses Within Same City
    # ----------------------------------------
    joined_df = a.join(
        b,
        (a["business_ID"] != b["business_ID"]) & (a["city"] == b["city"])
    )

    similarity_df = joined_df.withColumn(
        "similarity",
        cosine_udf(col("a.profile_vector"), col("b.profile_vector"))
    )

    # ----------------------------------------
    # 7. Top 5 Recommendations Per Business
    # ----------------------------------------
    windowSpec = Window.partitionBy("a.business_ID").orderBy(col("similarity").desc())

    top_n = (
        similarity_df.withColumn("rank", row_number().over(windowSpec))
        .filter(col("rank") <= 5)
        .select(
            col("a.business_ID").alias("business_ID"),
            col("a.business_name").alias("business_name"),
            col("b.business_ID").alias("recommended_business_ID"),
            col("b.business_name").alias("recommended_name"),
            col("similarity"),
            col("a.city").alias("city")
        )
    )

    # Group the top recommendations by business_ID
    grouped_df = top_n.groupBy("business_ID", "business_name", "city").agg(
        collect_list(
            struct(
                "recommended_business_ID",
                "recommended_name",
                "similarity"
            )
        ).alias("recommendations")
    )

    # ----------------------------------------
    # 8. Save as JSON
    # ----------------------------------------
    grouped_df.write.mode("overwrite").json("output/content_recommendations_grouped.json")

    print("Recommendations grouped and saved in 'output/content_recommendations_grouped.json'")

    spark.stop()

if __name__ == "__main__":
    main()
