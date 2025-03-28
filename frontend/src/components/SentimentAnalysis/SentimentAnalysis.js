import React, { useEffect, useState } from 'react';
import Papa from 'papaparse';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { PieChart, Pie, Cell } from 'recharts';
import WordCloud from 'react-wordcloud'; // Use react-wordcloud for word cloud

const SentimentAnalysis = () => {
  const [data, setData] = useState([]);
  const [wordFrequency, setWordFrequency] = useState([]);
  const [positiveWordCloudData, setPositiveWordCloudData] = useState([]);
  const [negativeWordCloudData, setNegativeWordCloudData] = useState([]);

  // Load sentiment data (CSV for comparison between text and stars)
  useEffect(() => {
    Papa.parse('/sentiment.csv', {
      download: true,
      complete: (result) => {
        const rawData = result.data.slice(1); // Remove header
        setData(rawData);
      },
      header: true,
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

  return (
    <div>
      <h2>Sentiment Analysis of Reviews</h2>

      {/* Word Frequency Chart */}
      {wordFrequency.length > 0 && (
        <ResponsiveContainer width="100%" height={300}>
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
      )}

      {/* Sentiment Comparison Chart */}
      <h3>Sentiment Comparison: Text vs. Star Rating</h3>
      <div style={{ display: 'flex' }}>
        <PieChart width={300} height={300}>
          <Pie
            data={[
              { name: 'Positive (Text)', value: sentimentCounts.positive },
              { name: 'Negative (Text)', value: sentimentCounts.negative },
              { name: 'Neutral (Text)', value: sentimentCounts.neutral },
            ]}
            dataKey="value"
            outerRadius={80}
            fill="#82ca9d"
          >
            <Cell fill="#00C49F" />
            <Cell fill="#FF8042" />
            <Cell fill="#8B8B8B" />
          </Pie>
          <Tooltip />
        </PieChart>

        <PieChart width={300} height={300}>
          <Pie
            data={[
              { name: 'Positive (Rating)', value: ratingCounts.positive },
              { name: 'Negative (Rating)', value: ratingCounts.negative },
              { name: 'Neutral (Rating)', value: ratingCounts.neutral },
            ]}
            dataKey="value"
            outerRadius={80}
            fill="#FF8042"
          >
            <Cell fill="#00C49F" />
            <Cell fill="#FF8042" />
            <Cell fill="#8B8B8B" />
          </Pie>
          <Tooltip />
        </PieChart>
      </div>

      {/* Positive Word Cloud */}
      <h3>Positive Reviews Word Cloud</h3>
      {positiveWordCloudData.length > 0 && (
        <WordCloud
            words={positiveWordCloudData}
            size={[500,500]}
            options={{
            rotationAngles: [0],
            fontSizes:[20, 200], // Reduce font size difference
            padding: 2, // Reduce space between words
            spiral: 'archimedean', // Adjust spiral to make it compact
            scale: 'sqrt', // Scaling option for words
        }}
      />
        
      )}

      {/* Negative Word Cloud */}
      <h3>Negative Reviews Word Cloud</h3>
      {negativeWordCloudData.length > 0 && (
        <WordCloud
            words={negativeWordCloudData}
            size={[500,500]}
            options={{
            rotationAngles: [0],
            fontSizes:[20, 200], // Reduce font size difference
            padding: 2, // Reduce space between words
            spiral: 'archimedean', // Adjust spiral to make it compact
            scale: 'sqrt', // Scaling option for words
        }}/>
      )}
    </div>
  );
};

export default SentimentAnalysis;
