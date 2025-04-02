import nltk
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
import json
import os

def main(spark):
    nltk.download('stopwords')

    # Load TSV from HDFS
    df = spark.read.csv("hdfs:///input/dataset/small-r-00000", sep='\t', header=False, inferSchema=True)

    df = df.toDF("business_id", "name", "address", "city", "state", "postal", "lat", "lon", "categories", "opening_hours", "stars", "review_text", "datetime")

    # Tokenize text
    tokenizer = RegexTokenizer(inputCol="review_text", outputCol="words", pattern="\\W+")
    df = tokenizer.transform(df)

    # Initialize VADER
    sia = SentimentIntensityAnalyzer()

    def get_sentiment(text):
        if text is None:
            return "neutral"
        score = sia.polarity_scores(text)
        return "positive" if score['compound'] >= 0.05 else ("negative" if score['compound'] <= -0.05 else "neutral")

    sentiment_udf = udf(get_sentiment, StringType())
    df = df.withColumn("sentiment", sentiment_udf(col("review_text")))

    # Rating category
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
    df = df.withColumn("rating_category", rate_category_udf(col("stars")))

    # Filter reviews
    positive_reviews = df.filter(df.sentiment == "positive").cache().repartition(10)
    negative_reviews = df.filter(df.sentiment == "negative").cache().repartition(10)

    # Stopwords
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

    sentiment_vs_star = df.select("sentiment", "rating_category").limit(50000).toPandas()

    # Output paths
    local_output_folder = "./output/sentiment-analysis/data"
    os.makedirs(local_output_folder, exist_ok=True)

    with open(os.path.join(local_output_folder, "word_frequencies.json"), 'w') as f:
        json.dump(wordcloud_data, f)
    with open(os.path.join(local_output_folder, "positive_wordcloud.json"), 'w') as f:
        json.dump(top_50_positive, f)
    with open(os.path.join(local_output_folder, "negative_wordcloud.json"), 'w') as f:
        json.dump(top_50_negative, f)

    sentiment_vs_star.to_csv(os.path.join(local_output_folder, "sentiment.csv"), index=False)

    print("Sentiment analysis completed and results saved.")

if __name__ == "__main__":
    main()