import React, { useState, useEffect } from 'react';
import Papa from 'papaparse';
import './LandingPage.css';

const LandingPage = () => {
  const [summaryKPIs, setSummaryKPIs] = useState([]);

  useEffect(() => {
    Papa.parse('/yelp_summary_kpis.csv', {
      download: true,
      header: true,
      complete: (results) => setSummaryKPIs(results.data),
    });
  }, []);

  return (
    <div className="landing-container">
      <h1>Welcome Back, Jeremy</h1>
      <div className="cards-container">
        {summaryKPIs.map((kpi, index) => (
          <div key={index} className="kpi-card">
            <h3>{kpi.metric}</h3>
            <p>{kpi.value}</p>
          </div>
        ))}
      </div>
    </div>
  );
};

export default LandingPage;
