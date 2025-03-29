from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, row_number, collect_list, struct
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from pyspark.ml.feature import Tokenizer, StopWordsRemover, Word2Vec

def main():
    # ----------------------------------------
    # 1. Start Spark Session
    # ----------------------------------------
    spark = SparkSession.builder \
        .appName("MLBasedContentRecommender") \
        .getOrCreate()

    # ----------------------------------------
    # 2. Load the raw business review data with new schema
    # ----------------------------------------
    raw_columns = [
        "business_id", "name", "address", "city", "state", "postal",
        "lat", "lon", "categories", "opening_hours", "stars", "review_text", "datetime"
    ]

    raw_df = (
        spark.read.csv(
            "../dataset/small-raw-r-00000",
            sep="\t",
            header=False,
            inferSchema=True
        )
        .toDF(*raw_columns)
    )

    # ----------------------------------------
    # 3. Text Preprocessing → Word2Vec
    # ----------------------------------------
    tokenizer = Tokenizer(inputCol="review_text", outputCol="words")
    tokenized_df = tokenizer.transform(raw_df)

    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    filtered_df = remover.transform(tokenized_df)

    # ----------------------------------------
    # 4. Train Word2Vec Model
    # ----------------------------------------
    word2Vec = Word2Vec(
        vectorSize=100,
        minCount=2,
        inputCol="filtered_words",
        outputCol="features"
    )

    model = word2Vec.fit(filtered_df)
    vector_df = model.transform(filtered_df)

    # ----------------------------------------
    # 5. Aggregate Vectors Per Business
    # ----------------------------------------
    business_profiles = (
        vector_df.groupBy("business_id")
        .agg(
            first("name").alias("name"),
            first("city").alias("city"),
            first("state").alias("state"),
            collect_list("features").alias("feature_vectors")
        )
    )

    # Average the vectors per business
    from pyspark.ml.linalg import Vectors, VectorUDT
    import numpy as np
    from pyspark.sql.functions import udf

    def avg_vector(vecs):
        if not vecs:
            return Vectors.dense([0.0]*100)
        sum_vec = np.sum(vecs, axis=0)
        avg = sum_vec / len(vecs)
        return Vectors.dense(avg.tolist())

    avg_vector_udf = udf(avg_vector, VectorUDT())

    business_profiles = business_profiles.withColumn("profile_vector", avg_vector_udf(col("feature_vectors"))) \
                                         .drop("feature_vectors")

    # ----------------------------------------
    # 6. Cross Join for Cosine Similarity
    # ----------------------------------------
    a = business_profiles.alias("a")
    b = business_profiles.alias("b")

    def cosine_sim(v1, v2):
        if not v1 or not v2:
            return 0.0
        dot = float(v1.dot(v2))
        norm = float(v1.norm(2) * v2.norm(2))
        return dot / norm if norm != 0 else 0.0

    cosine_udf = spark.udf.register("cosine_sim", cosine_sim, DoubleType())

    joined_df = a.join(
        b,
        (a["business_id"] != b["business_id"]) & (a["city"] == b["city"])
    )

    similarity_df = joined_df.withColumn(
        "similarity",
        cosine_udf(col("a.profile_vector"), col("b.profile_vector"))
    )

    # ----------------------------------------
    # 7. Top 5 Similar Businesses Per Business
    # ----------------------------------------
    windowSpec = Window.partitionBy("a.business_id").orderBy(col("similarity").desc())

    top_n = (
        similarity_df.withColumn("rank", row_number().over(windowSpec))
        .filter(col("rank") <= 5)
        .select(
            col("a.business_id").alias("business_id"),
            col("a.name").alias("name"),
            col("b.business_id").alias("recommended_business_id"),
            col("b.name").alias("recommended_name"),
            col("similarity"),
            col("a.city").alias("city")
        )
    )

    # Group and save the recommendations per business
    grouped_df = top_n.groupBy("business_id", "name", "city").agg(
        collect_list(
            struct(
                "recommended_business_id",
                "recommended_name",
                "similarity"
            )
        ).alias("recommendations")
    )

    grouped_df.write.mode("overwrite").json("output/ml_content_recommendations_grouped.json")

    print("ML-based recommendations saved in 'output/ml_content_recommendations_grouped.json'")
    spark.stop()

if __name__ == "__main__":
    main()
