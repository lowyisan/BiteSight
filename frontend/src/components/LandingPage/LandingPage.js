import React, { useState, useEffect } from 'react';
import Papa from 'papaparse';
import './LandingPage.css';

const LandingPage = () => {
  const [summaryKPIs, setSummaryKPIs] = useState([]);

  // Human-friendly labels
  const metricLabels = {
    total_businesses: "Total Businesses",
    total_reviews: "Total Reviews",
    overall_avg_rating: "Overall Average Rating",
    num_states: "Number of States",
    num_cities: "Number of Cities"
  };

  useEffect(() => {
    Papa.parse('/yelp_summary_kpis.csv', {
      download: true,
      header: true,
      complete: (results) => setSummaryKPIs(results.data),
    });
  }, []);

  // expected to have 5 rows in summaryKPIs
  // slice first 3 for top row, next 2 for bottom row
  const topRowKpis = summaryKPIs.slice(0, 3);
  const bottomRowKpis = summaryKPIs.slice(3, 5);

  return (
    <div className="landing-container">
      <div className="top-bar">
        <h1>Analytics Dashboard</h1>
      </div>

      {/* top row: 3 KPI cards */}
      <div className="kpi-row top-row">
        {topRowKpis.map((kpi, index) => {
          const displayLabel = metricLabels[kpi.metric] || kpi.metric;
          return (
            <div key={index} className="kpi-card">
              <h3>{displayLabel}</h3>
              <p>{kpi.value}</p>
            </div>
          );
        })}
      </div>

      {/* bottom row: 2 KPI cards */}
      <div className="kpi-row bottom-row">
        {bottomRowKpis.map((kpi, index) => {
          const displayLabel = metricLabels[kpi.metric] || kpi.metric;
          return (
            <div key={index} className="kpi-card">
              <h3>{displayLabel}</h3>
              <p>{kpi.value}</p>
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default LandingPage;
