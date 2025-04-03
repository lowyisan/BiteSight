# 🍽️ BiteSight

**BiteSight** is a data analytics pipeline that leverages Hadoop MapReduce and Apache Spark (PySpark) to uncover insights from Yelp's food business reviews across the U.S. It provides sentiment analysis, trend detection, category-based hotspots, and personalized recommendations—all visualized via an interactive dashboard.

## 🔍 Project Overview

Yelp contains millions of reviews that are often overwhelming for users to interpret and difficult for businesses to analyze. **BiteSight** solves this by:

- Helping **users** discover top-rated food spots and trends.
- Assisting **food businesses** in understanding customer feedback and rating dynamics.

The system focuses on food-related businesses in the U.S., extracted from Yelp’s open dataset (~6.99M reviews and 150K businesses).

---

## ⚙️ System Architecture

1. **Data Collection:** Yelp JSON dataset
2. **Preprocessing:** Converted to TSV for efficient parsing with Hadoop
3. **Data Storage:** Amazon S3 (raw), HDFS on EMR (intermediate & compute)
4. **Processing:**  
   - **Hadoop MapReduce:** Data cleaning, joining, aggregation  
   - **Apache Spark (PySpark):** 5 analytical modules
5. **Visualization:** Frontend built in React with Chart.js, Recharts, and word clouds

Hosted on [Vercel](https://yelp-dining-trends.vercel.app/)

---

## 📊 Analytical Modules

| Module                   | Description                                                                 |
|--------------------------|-----------------------------------------------------------------------------|
| Sentiment Analysis       | VADER-based scoring and word cloud comparison with review stars             |
| Time-Series Analysis     | Trend detection and LDA-based topic modeling for rating spikes/dips         |
| Geospatial Sentiment     | Heatmap visualization of sentiment across states and cities                 |
| City-Based Analysis      | Trends in category popularity and operational hours across U.S. cities      |
| Content-Based Filtering  | Recommender system based on business similarity using review content        |

---

## 🛠 Technologies Used

- **Backend/Data Processing:**  
  - Hadoop MapReduce  
  - Apache Spark (PySpark)  
  - Amazon EMR & HDFS  
  - Amazon S3

- **Frontend Visualization:**  
  - React.js  
  - Recharts, Chart.js, wordcloud2  
  - Hosted on [Vercel](https://vercel.com)

- **Topic Modeling:**  
  - Gensim LDA  
  - NLTK for preprocessing

---

## 📦 Outputs

All JSON/CSV outputs from the Spark analyses are stored in HDFS and later copied to S3 for frontend access:

sentiment.json, quarterly_trends.json, spike_dip_topic_modeling.json, etc.

## 📈 Live Dashboard

View the live interactive dashboard at: https://yelp-dining-trends.vercel.app

## 👥 Contributors

- Low Yi San
- Kee Han Xiang
- Michelle Magdalene Trisoeranto
- Jeremy Lim Min En
- Nadhirah Binti Ayub Khan



