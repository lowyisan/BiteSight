from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, row_number, collect_list, struct
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.linalg import Vectors
from pyspark.sql.window import Window
from pyspark.ml.stat import Summarizer


# ----------------------------------------
# 1. Start Spark Session
# ----------------------------------------
spark = SparkSession.builder.appName("LocalContentRecommender").getOrCreate()

# ----------------------------------------
# 2. Load Raw + Aggregated Data (tab-separated, no headers)
# ----------------------------------------

# Define column names for raw data
raw_columns = [
    "business_ID", "business_name", "city", "state", "lat", "lon",
    "star", "review_text", "review_date"
]
raw_df = spark.read.csv("raw-r-00000.csv", sep="\t", header=False, inferSchema=True)\
    .toDF(*raw_columns)

# 🔽 Limit to just 50000 rows
raw_df = raw_df.limit(50000)

# Define column names for aggregated data
agg_columns = [
    "business_ID", "business_name", "city", "state", "lat", "lon",
    "0star", "1star", "2star", "3star", "4star", "5star", "totalreview"
]
# Drop extra duplicate columns (e.g. business_name, city, state, etc.) from aggregated data so they don’t conflict in the join.
agg_df = spark.read.csv("aggregated-r-00000.csv", sep="\t", header=False, inferSchema=True)\
    .toDF(*agg_columns)

# ----------------------------------------
# 3. Merge on business_ID (drop duplicate columns from agg_df)
# ----------------------------------------
merged_df = raw_df.join(
    agg_df.drop("business_name", "city", "state", "lat", "lon"),
    on="business_ID", 
    how="left"
).dropDuplicates(["business_ID", "review_text"])

# ----------------------------------------
# 4. Text Preprocessing → TF-IDF
# ----------------------------------------
# Tokenize the review_text into words
tokenizer = Tokenizer(inputCol="review_text", outputCol="words")
words_df = tokenizer.transform(merged_df)

# Remove stopwords
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
filtered_df = remover.transform(words_df)

# Convert words to raw feature vectors (TF)
hashingTF = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=1000)
tf_df = hashingTF.transform(filtered_df)

# Compute IDF and create TF-IDF features
idf = IDF(inputCol="raw_features", outputCol="tfidf_features")
idf_model = idf.fit(tf_df)
tfidf_df = idf_model.transform(tf_df)

# ----------------------------------------
# 5. Aggregate TF-IDF Per Business Using Summarizer
# ----------------------------------------
# Instead of avg(), use Summarizer.mean on the tfidf_features vector column
business_profiles = tfidf_df.groupBy("business_ID").agg(
    Summarizer.mean(col("tfidf_features")).alias("profile_vector"),
    first("business_name").alias("business_name"),
    first("city").alias("city"),
    first("state").alias("state"),
    first("totalreview").alias("totalreview")
).dropna(subset=["profile_vector"])

# ----------------------------------------
# 6. Set Up for Cross Join
# ----------------------------------------
a = business_profiles.alias("a")
b = business_profiles.alias("b")

# ----------------------------------------
# 7. Cosine Similarity UDF
# ----------------------------------------
def cosine_sim(v1, v2):
    if not v1 or not v2:
        return 0.0
    dot = float(v1.dot(v2))
    norm = float(v1.norm(2) * v2.norm(2))
    return dot / norm if norm != 0 else 0.0

cosine_udf = spark.udf.register("cosine_sim", cosine_sim, DoubleType())

# ----------------------------------------
# 8. Cross Join Businesses Within Same City
# ----------------------------------------
joined_df = a.join(b, (a["business_ID"] != b["business_ID"]) & (a["city"] == b["city"]))

similarity_df = joined_df.withColumn(
    "similarity", cosine_udf(col("a.profile_vector"), col("b.profile_vector"))
)

# ----------------------------------------
# 9. Top 5 Recommendations Per Business
# ----------------------------------------
windowSpec = Window.partitionBy("a.business_ID").orderBy(col("similarity").desc())

top_n = similarity_df.withColumn("rank", row_number().over(windowSpec)) \
    .filter(col("rank") <= 5) \
    .select(
        col("a.business_ID").alias("business_ID"),
        col("a.business_name").alias("business_name"),
        col("b.business_ID").alias("recommended_business_ID"),
        col("b.business_name").alias("recommended_name"),
        col("similarity"),
        col("a.city").alias("city")
    )

# Assuming `top_n` DataFrame has already been created

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

# Save as JSON
grouped_df.write.mode("overwrite").json("output/content_recommendations_grouped.json")

print("Recommendations grouped and saved in 'output/content_recommendations_grouped.json'")


