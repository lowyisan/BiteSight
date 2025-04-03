# Contributor(s): Jeremy Lim Min En

import nltk
nltk.download('vader_lexicon')
nltk.download('stopwords')

import json
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.ml.feature import RegexTokenizer
from py4j.java_gateway import java_import

input_path = "hdfs:///input/dataset/small-raw-r-00000"
output_folder = "hdfs:///output/analysis/sentiment"

def save_single_json_file(spark, data_list, hdfs_output_path, filename):
    """
    Saves a single JSON file to a given HDFS path with a specific filename.
    """
    # Convert list of dicts to DataFrame directly
    df = spark.createDataFrame(data_list)

    temp_path = f"{hdfs_output_path}/.tmp_{filename}"
    final_path = f"{hdfs_output_path}/{filename}"

    df.coalesce(1).write.mode("overwrite").json(temp_path)

    # Rename part file to desired file name
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    tmp_dir = spark._jvm.Path(temp_path)
    target_file = spark._jvm.Path(final_path)

    for file_status in fs.listStatus(tmp_dir):
        name = file_status.getPath().getName()
        if name.startswith("part-") and name.endswith(".json"):
            fs.rename(file_status.getPath(), target_file)
            break

    fs.delete(tmp_dir, True)
    print(f"[HDFS] Written: {final_path}")

def main(spark):
    df = spark.read.csv(input_path, sep='\t', header=False, inferSchema=True).toDF(
        "business_id", "name", "address", "city", "state", "postal", "lat", "lon",
        "categories", "opening_hours", "review_star", "review_text", "datetime"
    )

    tokenizer = RegexTokenizer(inputCol="review_text", outputCol="words", pattern="\\W+")
    df = tokenizer.transform(df)

    sia = SentimentIntensityAnalyzer()

    def get_sentiment(text):
        if text is None:
            return "neutral"
        score = sia.polarity_scores(text)
        return "positive" if score['compound'] >= 0.05 else ("negative" if score['compound'] <= -0.05 else "neutral")

    sentiment_udf = udf(get_sentiment, StringType())
    df = df.withColumn("sentiment", sentiment_udf(col("review_text")))

    def rate_category(stars):
        if stars is None:
            return "unknown"
        elif stars >= 4.0:
            return "positive"
        elif stars == 3.0:
            return "neutral"
        else:
            return "negative"

    rate_category_udf = udf(rate_category, StringType())
    df = df.withColumn("rating_category", rate_category_udf(col("review_star")))

    # Cache and filter
    positive_reviews = df.filter(df.sentiment == "positive").cache().repartition(10)
    negative_reviews = df.filter(df.sentiment == "negative").cache().repartition(10)

    stop_words = set(stopwords.words('english'))
    restaurant_words = set([
        "food", "restaurant", "service", "staff", "menu", "drink", "meal", "dish", "chef",
        "waiter", "waitress", "ambiance", "dining", "delicious", "taste", "flavor", "appetizer",
        "main", "course", "dessert", "bills", "tip", "reservation", "sitting", "cooking",
        "presentation", "bar", "table", "order", "eat", "served", "customers", "experience",
        "spicy", "hot", "cold", "sommelier", "waitlist", "specials", "birthday", "celebration",
        "gourmet", "fresh", "local", "organic", "price", "rude", "polite", "helpful"
    ])

    def filter_words(word_list):
        return [w for w in word_list if w and len(w) > 2 and w.isalpha() and w.lower() not in stop_words and w.lower() in restaurant_words]

    def count_words(words):
        word_count = {}
        for word in words:
            word = word.lower()
            word_count[word] = word_count.get(word, 0) + 1
        return word_count

    pos_words = positive_reviews.select("words").rdd.flatMap(lambda x: x[0]).take(50000)
    neg_words = negative_reviews.select("words").rdd.flatMap(lambda x: x[0]).take(50000)

    pos_filtered = filter_words(pos_words)
    neg_filtered = filter_words(neg_words)

    pos_count = count_words(pos_filtered)
    neg_count = count_words(neg_filtered)

    combined = {}
    for word in set(pos_count) | set(neg_count):
        combined[word] = {
            "positive": pos_count.get(word, 0),
            "negative": neg_count.get(word, 0)
        }

    sorted_combined = sorted(combined.items(), key=lambda x: x[1]["positive"] + x[1]["negative"], reverse=True)[:20]
    wordcloud_data = [{"word": w, "positive": d["positive"], "negative": d["negative"]} for w, d in sorted_combined]

    top_pos = [{"word": w, "count": c} for w, c in sorted(pos_count.items(), key=lambda x: x[1], reverse=True)[:50]]
    top_neg = [{"word": w, "count": c} for w, c in sorted(neg_count.items(), key=lambda x: x[1], reverse=True)[:50]]

    sentiment_counts = df.groupBy("sentiment").count().toPandas().set_index("sentiment")["count"].to_dict()
    rating_counts = df.groupBy("rating_category").count().toPandas().set_index("rating_category")["count"].to_dict()

    summary = {}
    for key, val in sentiment_counts.items():
        summary[f"sentiment_{key}"] = str(val)
    for key, val in rating_counts.items():
        summary[f"rating_category_{key}"] = str(val)

    # === Write all JSONs to HDFS ===
    save_single_json_file(spark, wordcloud_data, output_folder, "word_frequencies.json")
    save_single_json_file(spark, top_pos, output_folder, "positive_wordcloud.json")
    save_single_json_file(spark, top_neg, output_folder, "negative_wordcloud.json")
    save_single_json_file(spark, [summary], output_folder, "sentiment_rating_summary.json")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()
    main(spark)
    spark.stop()
