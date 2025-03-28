import React from 'react';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import './AnalysisGraph.css';

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend);

const AnalysisGraph = ({ data, viewType }) => {
  if (!data || data.length === 0) {
    return <p>No data available for the selected business and year.</p>;
  }

  // Build chart data
  let labels = [];
  let ratings = [];

  if (viewType === 'monthly') {
    const sorted = data.sort((a, b) => {
      if (a.year === b.year) return a.month - b.month;
      return a.year - b.year;
    });
    labels = sorted.map(item => `${item.year}-${String(item.month).padStart(2, '0')}`);
    ratings = sorted.map(item => item.monthly_avg_rating);
  } else {
    const sorted = data.sort((a, b) => {
      if (a.year === b.year) return a.quarter - b.quarter;
      return a.year - b.year;
    });
    labels = sorted.map(item => `${item.year}-Q${item.quarter}`);
    ratings = sorted.map(item => item.avg_quarterly_rating);
  }

  const chartData = {
    labels,
    datasets: [
      {
        label: 'Average Rating',
        data: ratings,
        fill: false,
        borderColor: '#3498db',
        tension: 0.1,
      },
    ],
  };

  return (
    <div className="chart-container">
      <Line data={chartData} />
    </div>
  );
};

export default AnalysisGraph;
