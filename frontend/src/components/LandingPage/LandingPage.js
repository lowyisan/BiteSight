import React, { useState, useEffect } from 'react';
import Papa from 'papaparse';
import './LandingPage.css';

const LandingPage = () => {
  const [summaryKPIs, setSummaryKPIs] = useState([]);

  const metricLabels = {
    total_businesses: "Total Businesses",
    total_reviews: "Total Reviews",
    overall_avg_rating: "Overall Average Rating"
  };

  useEffect(() => {
    Papa.parse('/yelp_summary_kpis.csv', {
      download: true,
      header: true,
      complete: (results) => setSummaryKPIs(results.data),
    });
  }, []);

  return (
    <div className="landing-container">
      <div className="top-bar">
        <h1>Analytics Dashboard</h1>
      </div>

      <div className="cards-container">
        {summaryKPIs.map((kpi, index) => {
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
