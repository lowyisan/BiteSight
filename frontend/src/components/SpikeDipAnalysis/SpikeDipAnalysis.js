/* Contributor(s): Low Yi San */

import React, { useMemo } from 'react';
import './SpikeDipAnalysis.css';

// Parse a topic string like "0.05*'service' + 0.05*'cold'" into words & weights
const parseTopicString = (topicStr) => {
  // Handle both single and double quotes, and potential quote variations
  const wordRegex = /(\d+\.\d+)\*["']?([^"'\s]+)["']?/g;
  const words = [];
  let match;

  while ((match = wordRegex.exec(topicStr)) !== null) {
    if (match[1] && match[2]) {
      words.push({
        text: match[2],
        value: Math.round(parseFloat(match[1]) * 100)
      });
    }
  }

  return words;
};

const mergeTopics = (topicsArray) => {
  const wordMap = {};
  
  // Ensure topicsArray is an array and has elements
  if (!Array.isArray(topicsArray) || topicsArray.length === 0) {
    return [];
  }

  topicsArray.forEach(topicStr => {
    const words = parseTopicString(topicStr);
    words.forEach(({ text, value }) => {
      // Convert text to lowercase to merge similar words
      const normalizedText = text.toLowerCase();
      if (wordMap[normalizedText]) {
        wordMap[normalizedText] += value;
      } else {
        wordMap[normalizedText] = value;
      }
    });
  });

  // Convert map to array, filter out very low weight words, and sort descending
  return Object.entries(wordMap)
    .map(([text, value]) => ({ text, value }))
    .filter(word => word.value > 0.5)
    .sort((a, b) => b.value - a.value);
};

// Generate a random pastel color
const getRandomPastelColor = () => {
  const hue = Math.floor(Math.random() * 360);
  return `hsl(${hue}, 70%, 80%)`;
};

const CustomWordCloud = ({ words }) => {
  return (
    <div
      className="custom-word-cloud"
      style={{
        display: 'flex',
        flexWrap: 'wrap',
        justifyContent: 'center',
        gap: '10px',
        padding: '20px',
        backgroundColor: '#f0f0f0',
        borderRadius: '8px'
      }}
    >
      {words.map((word, index) => (
        <div
          key={index}
          style={{
            backgroundColor: getRandomPastelColor(),
            padding: '5px 10px',
            borderRadius: '4px',
            fontSize: `${Math.max(12, Math.min(24, word.value))}px`,
            fontWeight: 'bold',
            margin: '5px',
            boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
            transform: `rotate(${Math.random() * 20 - 10}deg)` // Slight random rotation
          }}
        >
          {word.text} ({word.value})
        </div>
      ))}
    </div>
  );
};

const SpikeDipAnalysis = ({ business, selectedYear, selectedMonth, spikeDipData }) => {
  // Convert month to number, handling potential string input
  const monthNum = parseInt(selectedMonth, 10);

  // Find matching entry with robust comparison
  const matchingEntry = spikeDipData.find(
    item =>
      item.business_ID === business.business_ID &&
      item.year === parseInt(selectedYear, 10) &&
      item.month === monthNum
  );

  // Compute combined words safely
  const combinedWords = useMemo(() => {
    if (!matchingEntry || !matchingEntry.topics) {
      return [];
    }

    try {
      return mergeTopics(matchingEntry.topics);
    } catch (error) {
      console.error('Error parsing topics:', error);
      return [];
    }
  }, [matchingEntry]);

  // If no matching entry or no topics, return null or informative message
  if (!matchingEntry) {
    return <p>No spike/dip data found for this specific month.</p>;
  }

  // Display the data point (year-month) that was clicked
  const clickedYearMonth = `${matchingEntry.year}-${String(matchingEntry.month).padStart(2, '0')}`;

  return (
    <div className="spike-dip-analysis">
      <h3>Spike/Dip Analysis</h3>
      <p>
        <strong>Data Point Clicked:</strong> {clickedYearMonth}
      </p>
      <p>
        <strong>Type:</strong> {matchingEntry.spike_or_dip} &nbsp; 
        <strong>Rating Change:</strong> {matchingEntry.rating_diff.toFixed(2)}
      </p>
      <p>
        <strong>Previous Avg Rating:</strong>{' '}
        {matchingEntry.prev_avg_rating ? matchingEntry.prev_avg_rating.toFixed(2) : 'N/A'} &nbsp;
        <strong>Current Avg Rating:</strong>{' '}
        {matchingEntry.monthly_avg_rating ? matchingEntry.monthly_avg_rating.toFixed(2) : 'N/A'}
      </p>
      <h4>Topics Breakdown</h4>
      {combinedWords.length > 0 ? (
        <CustomWordCloud words={combinedWords} />
      ) : (
        <p>No significant topics found for this month.</p>
      )}
    </div>
  );
};

export default SpikeDipAnalysis;
