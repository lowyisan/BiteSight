# Contributor(s): Nadhirah Binti Ayub Khan, updated by ChatGPT

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, row_number, collect_list, struct, concat_ws
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from pyspark.ml.feature import Tokenizer, StopWordsRemover, Word2Vec

from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf
import numpy as np

# ==== FILE PATHS ====
input_path = "hdfs:///input/dataset/small-raw-r-00000"
output_file = "hdfs:///output/analysis/content-based/top5_similar_businesses.json"

def save_as_named_json(df, output_path):
    # Write to a temp path
    temp_path = output_path + "_tmp"
    df.coalesce(1).write.mode("overwrite").json(temp_path)

    # Rename part file to desired file name
    spark = SparkSession.getActiveSession()
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    temp_path_obj = spark._jvm.org.apache.hadoop.fs.Path(temp_path)
    final_path_obj = spark._jvm.org.apache.hadoop.fs.Path(output_path)

    for file_status in fs.listStatus(temp_path_obj):
        name = file_status.getPath().getName()
        if name.startswith("part-") and name.endswith(".json"):
            fs.rename(file_status.getPath(), final_path_obj)

    fs.delete(temp_path_obj, True)
    print(f"Written to HDFS: {output_path}")

def main():
    spark = SparkSession.builder \
        .appName("MLBasedContentRecommender") \
        .getOrCreate()

    raw_columns = [
        "business_id", "name", "address", "city", "state", "postal",
        "lat", "lon", "categories", "opening_hours", "stars", "review_text", "datetime"
    ]

    raw_df = (
        spark.read.csv(
            input_path,
            sep="\t",
            header=False,
            inferSchema=True
        )
        .toDF(*raw_columns)
    )

    raw_df = raw_df.withColumn("combined_text", concat_ws(" ", col("categories"), col("review_text")))

    tokenizer = Tokenizer(inputCol="combined_text", outputCol="words")
    tokenized_df = tokenizer.transform(raw_df)

    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    filtered_df = remover.transform(tokenized_df)

    word2Vec = Word2Vec(
        vectorSize=100,
        minCount=2,
        inputCol="filtered_words",
        outputCol="features"
    )

    model = word2Vec.fit(filtered_df)
    vector_df = model.transform(filtered_df)

    business_profiles = (
        vector_df.groupBy("business_id")
        .agg(
            first("name").alias("name"),
            first("city").alias("city"),
            first("state").alias("state"),
            collect_list("features").alias("feature_vectors")
        )
    )

    def avg_vector(vecs):
        if not vecs:
            return Vectors.dense([0.0]*100)
        sum_vec = np.sum(vecs, axis=0)
        avg = sum_vec / len(vecs)
        return Vectors.dense(avg.tolist())

    avg_vector_udf = udf(avg_vector, VectorUDT())
    business_profiles = business_profiles.withColumn("profile_vector", avg_vector_udf(col("feature_vectors"))).drop("feature_vectors")

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

    grouped_df = top_n.groupBy("business_id", "name", "city").agg(
        collect_list(
            struct(
                "recommended_business_id",
                "recommended_name",
                "similarity"
            )
        ).alias("recommendations")
    )

    save_as_named_json(grouped_df, output_file)

if __name__ == "__main__":
    main()
