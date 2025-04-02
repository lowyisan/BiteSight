import nltk
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
import json
import os
input_path = "hdfs:///input/dataset/small-raw-r-00000"
def main(spark):
    # Download stopwords if not already present
    nltk.download('stopwords')
    # Load the TSV file (header=False because there are no headers)
    df = spark.read.csv(input_path, sep='\t', header=False, inferSchema=True)
    
    # Manually assign column names
    df = df.toDF("business_id", "name", "address", "city", "state", "postal", "lat", "lon", "categories", "opening_hours", "review_star", "review_text", "datetime")

    # Tokenize the review text
    tokenizer = RegexTokenizer(inputCol="review_text", outputCol="words", pattern="\\W+")
    df = tokenizer.transform(df)

    # Sentiment analysis setup
    sia = SentimentIntensityAnalyzer()

    def get_sentiment(text):
        if text is None:
            return "neutral"
        score = sia.polarity_scores(text)
        return "positive" if score['compound'] >= 0.05 else ("negative" if score['compound'] <= -0.05 else "neutral")

    sentiment_udf = udf(get_sentiment, StringType())
    df = df.withColumn("sentiment", sentiment_udf(col("review_text")))

    # Convert star ratings into categories
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

    # Filter and cache positive/negative reviews
    positive_reviews = df.filter(df.sentiment == "positive").cache().repartition(10)
    negative_reviews = df.filter(df.sentiment == "negative").cache().repartition(10)

    # Stop words and domain words
    stop_words = set(stopwords.words('english'))
    restaurant_related_words = set([
        "food", "restaurant", "service", "staff", "menu", "drink", "meal", "dish", "chef", 
        "waiter", "waitress", "ambiance", "dining", "delicious", "taste", "flavor", "appetizer",
        "main", "course", "dessert", "bills", "tip", "reservation", "sitting", "cooking", 
        "presentation", "bar", "table", "order", "eat", "served", "customers", "experience", 
        "spicy", "hot", "cold", "sommelier", "waitlist", "specials", "birthday", "celebration", 
        "gourmet", "fresh", "local", "organic", "price", "rude", "polite", "helpful"
    ])

    def filter_relevant_words(word_list):
        return [word for word in word_list if len(word) > 2 and word.isalpha() and word.lower() not in stop_words and word.lower() in restaurant_related_words]

    def process_word_frequency(words):
        word_count = {}
        for word in words:
            word = word.lower()
            word_count[word] = word_count.get(word, 0) + 1
        return word_count

    # Sample and process word frequencies
    positive_words = positive_reviews.select("words").rdd.flatMap(lambda x: x[0]).take(50000)
    negative_words = negative_reviews.select("words").rdd.flatMap(lambda x: x[0]).take(50000)

    positive_words_filtered = filter_relevant_words(positive_words)
    negative_words_filtered = filter_relevant_words(negative_words)

    positive_word_count = process_word_frequency(positive_words_filtered)
    negative_word_count = process_word_frequency(negative_words_filtered)

    combined_word_count = {}
    for word in set(positive_word_count.keys()).union(set(negative_word_count.keys())):
        combined_word_count[word] = {
            'positive': positive_word_count.get(word, 0),
            'negative': negative_word_count.get(word, 0)
        }

    sorted_words = sorted(combined_word_count.items(), key=lambda x: (x[1]['positive'] + x[1]['negative']), reverse=True)[:20]
    wordcloud_data = [{"word": word, "positive": data['positive'], "negative": data['negative']} for word, data in sorted_words]

    top_50_positive = [{"word": word, "count": count} for word, count in sorted(positive_word_count.items(), key=lambda x: x[1], reverse=True)[:50]]
    top_50_negative = [{"word": word, "count": count} for word, count in sorted(negative_word_count.items(), key=lambda x: x[1], reverse=True)[:50]]

    # Output paths
    output_folder = "hdfs:///output/analysis/sentiment"
    os.makedirs(output_folder, exist_ok=True)

    with open(os.path.join(output_folder, "word_frequencies.json"), 'w') as f:
        json.dump(wordcloud_data, f)

    with open(os.path.join(output_folder, "positive_wordcloud.json"), 'w') as f:
        json.dump(top_50_positive, f)

    with open(os.path.join(output_folder, "negative_wordcloud.json"), 'w') as f:
        json.dump(top_50_negative, f)

    # Sentiment + Rating summary
    sentiment_counts = df.groupBy("sentiment").count().toPandas().set_index("sentiment")["count"].to_dict()
    rating_counts = df.groupBy("rating_category").count().toPandas().set_index("rating_category")["count"].to_dict()

    combined_counts = {}
    for key, val in sentiment_counts.items():
        combined_counts[f"sentiment_{key}"] = str(val)
    for key, val in rating_counts.items():
        combined_counts[f"rating_category_{key}"] = str(val)

    with open(os.path.join(output_folder, "sentiment_rating_summary.json"), 'w') as f:
        json.dump([combined_counts], f, indent=2)

    print(f" Word frequencies saved to '{output_folder}/word_frequencies.json'")
    print(f" Top 50 positive/negative words saved.")
    print(f" Sentiment + rating summary saved to '{output_folder}/sentiment_rating_summary.json'")

# Allow running this directly
if __name__ == "__main__":
   main()
