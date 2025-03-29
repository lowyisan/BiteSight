from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, row_number, collect_list, struct, concat_ws
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from pyspark.ml.feature import Tokenizer, StopWordsRemover, Word2Vec

def main():
    # 1. Start Spark Session
    spark = SparkSession.builder \
        .appName("MLBasedContentRecommender") \
        .getOrCreate()

    # 2. Load dataset with updated schema
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

    # 3. Combine categories + review text
    raw_df = raw_df.withColumn("combined_text", concat_ws(" ", col("categories"), col("review_text")))

    # 4. Preprocessing: Tokenize + Remove Stop Words
    tokenizer = Tokenizer(inputCol="combined_text", outputCol="words")
    tokenized_df = tokenizer.transform(raw_df)

    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    filtered_df = remover.transform(tokenized_df)

    # 5. Train Word2Vec on combined text
    word2Vec = Word2Vec(
        vectorSize=100,
        minCount=2,
        inputCol="filtered_words",
        outputCol="features"
    )

    model = word2Vec.fit(filtered_df)
    vector_df = model.transform(filtered_df)

    # 6. Aggregate Vectors Per Business
    business_profiles = (
        vector_df.groupBy("business_id")
        .agg(
            first("name").alias("name"),
            first("city").alias("city"),
            first("state").alias("state"),
            collect_list("features").alias("feature_vectors")
        )
    )

    # Compute average vector per business
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

    business_profiles = business_profiles.withColumn(
        "profile_vector", avg_vector_udf(col("feature_vectors"))
    ).drop("feature_vectors")

    # 7. Cross Join for Cosine Similarity (within same city)
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

    # 8. Top 5 Similar Businesses Per Business
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

    # 9. Group recommendations and save to S3
    grouped_df = top_n.groupBy("business_id", "name", "city").agg(
        collect_list(
            struct(
                "recommended_business_id",
                "recommended_name",
                "similarity"
            )
        ).alias("recommendations")
    )

    grouped_df.write.mode("overwrite").json(
        "output/top5_similar_businesses.json"
    )

    print("ML-based recommendations saved at: output/top5_similar_businesses.json")
    spark.stop()

if __name__ == "__main__":
    main()
