import React, { useEffect, useState } from 'react';
import Papa from 'papaparse';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from 'recharts';
import WordCloud from 'react-wordcloud';

const SentimentAnalysis = () => {
  const [data, setData] = useState([]);
  const [wordFrequency, setWordFrequency] = useState([]);
  const [positiveWordCloudData, setPositiveWordCloudData] = useState([]);
  const [negativeWordCloudData, setNegativeWordCloudData] = useState([]);
  const [summaryData, setSummaryData] = useState({
    sentiment: { positive: 0, negative: 0, neutral: 0 },
    rating: { positive: 0, negative: 0, neutral: 0 },
  });

  // Load sentiment data (CSV for comparison between text and stars)
  useEffect(() => {
    Papa.parse('/sentiment.csv', {
      download: true,
      header: true,
      complete: (result) => {
        const rawData = result.data.slice(1); // Remove header
        setData(rawData);
      },
    });
  }, []);

  // Load the word frequencies data (for top 20 words)
  useEffect(() => {
    fetch('/word_frequencies.json')
      .then((response) => response.json())
      .then((jsonData) => {
        setWordFrequency(jsonData);
      })
      .catch((error) => console.error('Error loading word frequencies:', error));
  }, []);

  // Load word cloud data for positive and negative reviews
  useEffect(() => {
    fetch('/positive_wordcloud.json')
      .then((response) => response.json())
      .then((jsonData) => {
        const positiveCloudData = jsonData.map(item => ({
          text: item.word,
          value: item.count,
        }));
        setPositiveWordCloudData(positiveCloudData);
      })
      .catch((error) => console.error('Error loading positive word cloud:', error));

    fetch('/negative_wordcloud.json')
      .then((response) => response.json())
      .then((jsonData) => {
        const negativeCloudData = jsonData.map(item => ({
          text: item.word,
          value: item.count,
        }));
        setNegativeWordCloudData(negativeCloudData);
      })
      .catch((error) => console.error('Error loading negative word cloud:', error));
  }, []);

  // Load sentiment + rating summary
  useEffect(() => {
    fetch('/sentiment_rating_summary.json')
      .then((res) => res.json())
      .then((json) => {
        const summary = json[0];
        setSummaryData({
          sentiment: {
            positive: parseInt(summary['sentiment_positive'] || 0),
            negative: parseInt(summary['sentiment_negative'] || 0),
            neutral: parseInt(summary['sentiment_neutral'] || 0),
          },
          rating: {
            positive: parseInt(summary['rating_category_positive'] || 0),
            negative: parseInt(summary['rating_category_negative'] || 0),
            neutral: parseInt(summary['rating_category_neutral'] || 0),
          },
        });
      })
      .catch((err) => console.error('Failed to load sentiment summary:', err));
  }, []);

  // Create sentiment comparison data
  const sentimentComparisonData = () => {
    const sentimentCounts = { positive: 0, negative: 0, neutral: 0 };
    const ratingCounts = { positive: 0, negative: 0, neutral: 0 };

    data.forEach((row) => {
      // Sentiment from text
      if (row.sentiment === 'positive') sentimentCounts.positive++;
      if (row.sentiment === 'negative') sentimentCounts.negative++;
      if (row.sentiment === 'neutral') sentimentCounts.neutral++;

      // Sentiment from rating
      if (row.sentiment === 'positive') ratingCounts.positive++;
      if (row.sentiment === 'negative') ratingCounts.neutral++;
      if (row.sentiment === 'neutral') ratingCounts.negative++;
    });

    return { sentimentCounts, ratingCounts };
  };

  const { sentimentCounts, ratingCounts } = sentimentComparisonData();

  // Colors for Pie charts
  const pieColors = ['#00C49F', '#FF8042', '#8B8B8B'];

  return (
    <div
      style={{
        maxWidth: '1200px',
        margin: '0 auto',
        padding: '2rem 1rem',
      }}
    >
      <h2 style={{ marginBottom: '2rem' }}>Sentiment Analysis of Reviews</h2>

      {/* Word Frequency Chart */}
      {wordFrequency.length > 0 && (
        <div style={{ marginBottom: '3rem' }}>
          <h3 style={{ marginBottom: '1rem' }}>Top Word Frequencies</h3>
          <div style={{ width: '100%', height: '300px' }}>
            <ResponsiveContainer>
              <BarChart data={wordFrequency}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="word" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="positive" fill="#82ca9d" />
                <Bar dataKey="negative" fill="#FF8042" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Sentiment Comparison Chart */}
      <div style={{ marginBottom: '3rem' }}>
        <h3 style={{ marginBottom: '1rem' }}>Sentiment Comparison: Text vs. Star Rating</h3>
        <div
          style={{
            display: 'flex',
            justifyContent: 'center',
            gap: '3rem',
            flexWrap: 'wrap',
          }}
        >
                    {/* Pie: Text Sentiment */}
          <div style={{ textAlign: 'center' }}>
            <h4 style={{ marginBottom: '1rem' }}>Text-Based Sentiment</h4>
            <PieChart width={260} height={260}>
              <Pie
                data={[
                  { name: 'Positive', value: summaryData.sentiment.positive },
                  { name: 'Negative', value: summaryData.sentiment.negative },
                  { name: 'Neutral', value: summaryData.sentiment.neutral },
                ]}
                dataKey="value"
                outerRadius={80}
              >
                {pieColors.map((color, index) => (
                  <Cell key={`cell-${index}`} fill={color} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </div>

          {/* Pie: Star Rating Sentiment */}
          <div style={{ textAlign: 'center' }}>
            <h4 style={{ marginBottom: '1rem' }}>Star Rating Sentiment</h4>
            <PieChart width={260} height={260}>
              <Pie
                data={[
                  { name: 'Positive', value: summaryData.rating.positive },
                  { name: 'Negative', value: summaryData.rating.negative },
                  { name: 'Neutral', value: summaryData.rating.neutral },
                ]}
                dataKey="value"
                outerRadius={80}
              >
                {pieColors.map((color, index) => (
                  <Cell key={`cell-${index}`} fill={color} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </div>
      </div>
      </div>

      {/* Word Clouds side by side */}
      <div>
        <h3 style={{ marginBottom: '1rem' }}>Word Clouds</h3>
        <div
          style={{
            display: 'flex',
            gap: '2rem',
            justifyContent: 'center',
            flexWrap: 'wrap',
          }}
        >
          {/* Positive Word Cloud */}
          <div style={{ textAlign: 'center' }}>
            <h4>Positive Reviews</h4>
            {positiveWordCloudData.length > 0 && (
              <WordCloud
                words={positiveWordCloudData}
                size={[350, 350]}
                options={{
                  rotationAngles: [0],
                  fontSizes: [16, 80],
                  padding: 2,
                  spiral: 'archimedean',
                  scale: 'sqrt',
                }}
              />
            )}
          </div>

          {/* Negative Word Cloud */}
          <div style={{ textAlign: 'center' }}>
            <h4>Negative Reviews</h4>
            {negativeWordCloudData.length > 0 && (
              <WordCloud
                words={negativeWordCloudData}
                size={[350, 350]}
                options={{
                  rotationAngles: [0],
                  fontSizes: [16, 80],
                  padding: 2,
                  spiral: 'archimedean',
                  scale: 'sqrt',
                }}
              />
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default SentimentAnalysis;
