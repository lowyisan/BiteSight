import React from 'react';
import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Tooltip,
  Title,
  Legend
} from 'chart.js';

import '../TimeBasedAnalysis/AnalysisGraph.css';

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Title, Legend);

const CityAnalysisGraph = ({ business }) => {
  if (!business) return null;

  const stars = [0, 1, 2, 3, 4, 5];
  const starCounts = stars.map(star => parseInt(business[`sum_star${star}`] || 0));

  const chartData = {
    labels: stars.map(s => `${s}★`),
    datasets: [
      {
        label: 'Number of Ratings',
        data: starCounts,
        backgroundColor: '#3498db'
      }
    ]
  };

  const chartOptions = {
    responsive: true,
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: {
          label: (context) => `Ratings: ${context.parsed.y}`
        }
      }
    },
    scales: {
      y: {
        beginAtZero: true,
        title: {
          display: true,
          text: 'Number of Ratings'
        }
      }
    }
  };

  const avgRating = parseFloat(business.avg_rating).toFixed(2);

  const renderHours = () => {
    if (!business.hours || !Array.isArray(business.hours)) return "No hours available";
    const days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
    return business.hours.map((h, i) => (
      <div key={i}>
        <strong>{days[i]}:</strong> {h === null || h === "0:0-0:0" ? "Closed" : h}
      </div>
    ));
  };

  return (
    <div className="chart-container">
      <Bar data={chartData} options={chartOptions} />
      <p className="chart-note">
        <strong>Average Rating:</strong> {avgRating}
        <br />
        <strong>Operating Hours:</strong><br />
        {renderHours()}
      </p>
    </div>
  );
};

export default CityAnalysisGraph;
