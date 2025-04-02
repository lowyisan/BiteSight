import nltk
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
import json
import os

# Download stopwords from NLTK (only need to run once)
nltk.download('stopwords')

# Initialize Spark Session with increased memory
spark = SparkSession.builder \
    .appName("SentimentAnalysis") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Load the TSV file (header=False because there are no headers)
df = spark.read.csv("hdfs:///input/dataset/small-r-00000", sep='\t', header=False, inferSchema=True)

# Manually assign column names as per your dataset structure
df = df.toDF("business_id", "name", "address", "city", "state", "postal", "lat", "lon", "categories", "opening_hours","stars", "review_text", "datetime")

# Tokenize the review text
tokenizer = RegexTokenizer(inputCol="review_text", outputCol="words", pattern="\\W+")
df = tokenizer.transform(df)

# Initialize VADER Sentiment Intensity Analyzer
sia = SentimentIntensityAnalyzer()

# Create a UDF to apply sentiment analysis on the text
def get_sentiment(text):
    if text is None:  # Add a check for None values
        return "neutral"
    score = sia.polarity_scores(text)
    return "positive" if score['compound'] >= 0.05 else ("negative" if score['compound'] <= -0.05 else "neutral")

sentiment_udf = udf(get_sentiment, StringType())
df = df.withColumn("sentiment", sentiment_udf(col("review_text")))

# Categorize star rating into positive, neutral, and negative
def rate_category(stars):
    if stars is None:
        return "unknown"  # or another category for missing values
    elif stars >= 4.0:
        return "positive"
    elif stars == 3.0:
        return "neutral"
    else:
        return "negative"

rate_category_udf = udf(rate_category, StringType())
df = df.withColumn("rating_category", rate_category_udf(col("review_star")))

# Filter out positive and negative reviews
positive_reviews = df.filter(df.sentiment == "positive")
negative_reviews = df.filter(df.sentiment == "negative")

# Cache the reviews to avoid recomputation
positive_reviews.cache()
negative_reviews.cache()

# Repartition data to avoid large tasks
positive_reviews = positive_reviews.repartition(10)
negative_reviews = negative_reviews.repartition(10)

# Get the list of stop words from NLTK
stop_words = set(stopwords.words('english'))

# List of words related to restaurants and dining experience
restaurant_related_words = set([
    "food", "restaurant", "service", "staff", "menu", "drink", "meal", "dish", "chef", 
    "waiter", "waitress", "ambiance", "dining", "delicious", "taste", "flavor", "appetizer",
    "main", "course", "dessert", "bills", "tip", "reservation", "sitting", "cooking", 
    "presentation", "bar", "table", "order", "eat", "served", "customers", "experience", 
    "spicy", "hot", "cold","sommelier", "reservation", "waitlist", 
    "specials","birthday", "celebration", "gourmet", 
    "fresh", "local", "organic","price","rude","polite","helpful"
])

# Function to filter unwanted words (including stopwords) and filter for restaurant-related terms
def filter_relevant_words(word_list):
    return [word for word in word_list if len(word) > 2 and word.isalpha() and word.lower() not in stop_words and word.lower() in restaurant_related_words]

# Function to calculate word frequency for a given set of words
def process_word_frequency(words):
    word_count = {}
    for word in words:
        word = word.lower()
        word_count[word] = word_count.get(word, 0) + 1
    return word_count

# Use take() to reduce memory overhead and filter words for positive and negative reviews
positive_words = positive_reviews.select("words").rdd.flatMap(lambda x: x[0]).take(50000)  # Taking first 50000 words for sample
negative_words = negative_reviews.select("words").rdd.flatMap(lambda x: x[0]).take(50000)

# Filter out the unwanted words and focus on restaurant-related words
positive_words_filtered = filter_relevant_words(positive_words)
negative_words_filtered = filter_relevant_words(negative_words)

# Calculate word frequency for positive and negative reviews
positive_word_count = process_word_frequency(positive_words_filtered)
negative_word_count = process_word_frequency(negative_words_filtered)

# Combine both positive and negative word counts
combined_word_count = {}
for word in set(positive_word_count.keys()).union(set(negative_word_count.keys())):
    combined_word_count[word] = {
        'positive': positive_word_count.get(word, 0),
        'negative': negative_word_count.get(word, 0)
    }

# Sort by combined frequency (sum of positive and negative frequencies) and take the top 20 words
sorted_words = sorted(combined_word_count.items(), key=lambda x: (x[1]['positive'] + x[1]['negative']), reverse=True)[:20]

# Prepare the final data format for word cloud (split by positive and negative)
wordcloud_data = [{"word": word, "positive": data['positive'], "negative": data['negative']} for word, data in sorted_words]

# Top 50 words used in positive and negative reviews (for word cloud)
top_50_positive = [{"word": word, "count": count} for word, count in sorted(positive_word_count.items(), key=lambda x: x[1], reverse=True)[:50]]
top_50_negative = [{"word": word, "count": count} for word, count in sorted(negative_word_count.items(), key=lambda x: x[1], reverse=True)[:50]]

# Prepare sentiment vs star rating comparison
sentiment_vs_star = df.select("sentiment", "rating_category").limit(50000).toPandas()

# Define the output folder and file name for JSON
output_folder = "hdfs:///output/analysis/sentiment-analysis/data"  # Folder called "output" in the current directory
output_wordcloud_json = "word_frequencies.json"  # File name for the output JSON
output_sentiment_csv = "sentiment.csv"  # File name for the output CSV
output_positive_wordcloud_json = "positive_wordcloud.json"
output_negative_wordcloud_json = "negative_wordcloud.json"

# Create the output folder if it does not exist
os.makedirs(output_folder, exist_ok=True)

# Save the word frequencies as a JSON file
with open(os.path.join(output_folder, output_wordcloud_json), 'w') as f:
    json.dump(wordcloud_data, f)

# Save the top 50 positive and negative word cloud JSON data
with open(os.path.join(output_folder, output_positive_wordcloud_json), 'w') as f:
    json.dump(top_50_positive, f)

with open(os.path.join(output_folder, output_negative_wordcloud_json), 'w') as f:
    json.dump(top_50_negative, f)

# Save sentiment vs star rating comparison CSV
sentiment_vs_star.to_csv(os.path.join(output_folder, output_sentiment_csv), index=False)

print(f"Word frequencies saved as JSON to '{output_folder}/{output_wordcloud_json}'.")
print(f"Sentiment data saved as CSV to '{output_folder}/{output_sentiment_csv}'.")
print(f"Top 50 positive words saved as JSON to '{output_folder}/{output_positive_wordcloud_json}'.")
print(f"Top 50 negative words saved as JSON to '{output_folder}/{output_negative_wordcloud_json}'.")
