import React, { useEffect, useState } from 'react';
import {
  Bar,
  Line
} from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend
);

const CityBasedInsights = ({
  analysisType,
  selectedCategory,
  selectedState,
  selectedCity
}) => {
  const [data, setData] = useState([]);

  const dataMap = {
    "Top Categories": "/top_categories_per_city.json",
    "Average Business Hours": "/avg_business_hours.json",
    "Hotspot Cities": "/hotspot_cities_per_category.json"
  };

  // Reset and fetch new data when analysis type changes
  useEffect(() => {
    const path = dataMap[analysisType];
    if (!path) return;

    setData([]); // Clear previous data

    fetch(path)
      .then(res => res.json())
      .then(setData)
      .catch(err => console.error("Failed to fetch insight data", err));
  }, [analysisType]);

  if (!analysisType || analysisType === "None") return null;

  // Normalize helper
  const normalize = (val) => val?.toLowerCase().trim();

  const city = normalize(selectedCity);
  const state = normalize(selectedState);

  const noDataMessage = (
    <div className="insight-card">
      <h2>{analysisType}</h2>
      <p>
        Please select {analysisType === "Average Business Hours" ? "a state" :
          analysisType === "Top Categories" ? "a state and city" : "a category"} to view insights.
      </p>
    </div>
  );

  let chartData = { labels: [], datasets: [] };
  let title = "";

  if (analysisType === "Top Categories") {
    if (!state || !city) return noDataMessage;

    const filtered = data.filter(
      d => normalize(d.city) === city && normalize(d.state) === state
    );

    if (!filtered.length) return (
      <div className="insight-card">
        <h2>Top Categories</h2>
        <p>No data available for {selectedCity}, {selectedState}.</p>
      </div>
    );

    const top = filtered
      .sort((a, b) => b.business_count - a.business_count)
      .slice(0, 10);

    chartData = {
      labels: top.map(d => d.category),
      datasets: [{
        label: 'Business Count',
        data: top.map(d => d.business_count),
        backgroundColor: '#3498db'
      }]
    };

    title = `Top Categories in ${selectedCity}`;
  }

  else if (analysisType === "Average Business Hours") {
    if (!state) return noDataMessage;

    const filtered = data.filter(d => normalize(d.state) === state);

    if (!filtered.length) return (
      <div className="insight-card">
        <h2>Average Business Hours</h2>
        <p>No data available for {selectedState}.</p>
      </div>
    );

    const sorted = [...filtered].sort((a, b) =>
      b.avg_days_open_per_business - a.avg_days_open_per_business
    );

    chartData = {
      labels: sorted.map(d => d.city),
      datasets: [{
        label: 'Avg Days Open Per Week',
        data: sorted.map(d => d.avg_days_open_per_business),
        borderColor: '#2ecc71',
        fill: false,
        tension: 0.3
      }]
    };

    title = `Average Business Hours (All Cities in ${selectedState})`;
  }

  else if (analysisType === "Hotspot Cities") {
    if (!selectedCategory) return noDataMessage;

    const filtered = data.filter(d =>
      normalize(d.category) === normalize(selectedCategory)
    );

    if (!filtered.length) return (
      <div className="insight-card">
        <h2>Hotspot Cities</h2>
        <p>No data available for the category "{selectedCategory}".</p>
      </div>
    );

    const topCities = filtered
      .sort((a, b) => b.total_reviews - a.total_reviews)
      .slice(0, 10);

    chartData = {
      labels: topCities.map(d => `${d.city}, ${d.state}`),
      datasets: [{
        label: 'Total Reviews',
        data: topCities.map(d => d.total_reviews),
        backgroundColor: '#f39c12'
      }]
    };

    title = `Hotspot Cities for ${selectedCategory}`;
  }

  return (
    <div className="insight-card">
      <h2>{title}</h2>
      {analysisType === "Average Business Hours" ? (
        <Line data={chartData} options={{ responsive: true }} />
      ) : (
        <Bar data={chartData} options={{ responsive: true }} />
      )}
    </div>
  );
};

export default CityBasedInsights;
