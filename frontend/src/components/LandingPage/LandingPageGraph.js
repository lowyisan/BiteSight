/* Contributor(s): Low Yi San, Nadhirah Binti Ayub Khan, Michelle Magdelene Trisoeranto */

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

import RecommendationCard from '../RecommendationCard/RecommendationCard';
import '../TimeBasedAnalysis/AnalysisGraph.css';

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Title, Legend);

const LandingPageGraph = ({ business, recommendations = [], businesses, onBusinessSelect }) => {
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

  const avgRating = parseFloat(business.avg_rating || business.stars || 0).toFixed(2);

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

      <div style={{ display: 'flex', justifyContent: 'flex-start', paddingLeft: '50px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: '100px', flexWrap: 'wrap', maxWidth: '600px', width: '100%' }}>
          {/* LEFT SIDE */}
          <div style={{ flex: 1, minWidth: '200px' }}>
            <p style={{ marginBottom: 8 }}><strong>Average Rating:</strong> {avgRating}</p>
            <p style={{ marginBottom: 8 }}><strong>Address:</strong> {business.address}</p>
            <p style={{ marginBottom: 8 }}>
              <strong>Location:</strong> {business.city}, {business.state}, {business.postal_code}
            </p>
          </div>

          {/* RIGHT SIDE - HOURS */}
          <div style={{ flex: 1, minWidth: '200px' }}>
            <p><strong>Operating Hours:</strong></p>
            {renderHours()}
          </div>
        </div>
      </div>


            {recommendations.length > 0 && (
        <div style={{ marginTop: '20px' }}>
          <h4>Businesses Similar to <span style={{ color: '#0073e6' }}>{business.business_name}</span></h4>
          <div style={{
            display: 'flex',
            flexWrap: 'nowrap',
            gap: '12px',
            overflowX: 'auto',
            padding: '10px 0',
          }}>
            {recommendations.map((rec, idx) => (
              <RecommendationCard
                key={idx}
                recommendation={rec}
                onSelect={(selected) => {
                  const newBusiness = businesses.find(b => b.business_id === selected.recommended_business_id);
                  if (newBusiness) {
                    onBusinessSelect(newBusiness);
                  }
                }}
              />
            ))}
          </div>
        </div>
      )}

    </div>
  );
};

export default LandingPageGraph;
