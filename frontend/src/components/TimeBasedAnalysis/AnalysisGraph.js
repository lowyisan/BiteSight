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

  // Build chart data with explicit handling of missing data
  let labels = [];
  let ratings = [];
  let hasData = false;

  if (viewType === 'monthly') {
    const sorted = data
      .filter(item => item.monthly_avg_rating !== null)
      .sort((a, b) => {
        if (a.year === b.year) return a.month - b.month;
        return a.year - b.year;
      });

    hasData = sorted.length > 0;
    labels = sorted.map(item => `${item.year}-${String(item.month).padStart(2, '0')}`);
    ratings = sorted.map(item => item.monthly_avg_rating);
  } else {
    const sorted = data
      .filter(item => item.avg_quarterly_rating !== null)
      .sort((a, b) => {
        if (a.year === b.year) return a.quarter - b.quarter;
        return a.year - b.year;
      });

    hasData = sorted.length > 0;
    labels = sorted.map(item => `${item.year}-Q${item.quarter}`);
    ratings = sorted.map(item => item.avg_quarterly_rating);
  }

  if (!hasData) {
    return <p>No rating data available for the selected business and time period.</p>;
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
        pointStyle: 'circle',
        pointRadius: 5,
        pointHoverRadius: 8,
      },
    ],
  };

  const chartOptions = {
    responsive: true,
    plugins: {
      legend: {
        display: true,
      },
      tooltip: {
        mode: 'index',
        intersect: false,
        callbacks: {
          label: function(context) {
            let label = context.dataset.label || '';
            if (label) {
              label += ': ';
            }
            if (context.parsed.y !== null) {
              label += context.parsed.y.toFixed(2);
            }
            return label;
          },
        },
      },
    },
    scales: {
      y: {
        beginAtZero: false,
        title: {
          display: true,
          text: 'Average Rating',
        },
      },
    },
  };

  return (
    <div className="chart-container">
      <Line data={chartData} options={chartOptions} />
      <p className="chart-note">
        * Gaps in the graph represent periods with no review data
      </p>
    </div>
  );
};

export default AnalysisGraph;