import React, { useState, useEffect } from 'react';
import BusinessCard from '../BusinessCard/BusinessCard';
import AnalysisGraph from './AnalysisGraph';
import './TimeBasedAnalysis.css';

const TimeBasedAnalysis = () => {
  const [businesses, setBusinesses] = useState([]);
  const [selectedBusiness, setSelectedBusiness] = useState(null);

  // For text search, state dropdown, and city dropdown
  const [searchText, setSearchText] = useState('');
  const [selectedState, setSelectedState] = useState('');
  const [selectedCity, setSelectedCity] = useState('');

  // For chart data
  const [graphData, setGraphData] = useState([]);
  const [viewType, setViewType] = useState('monthly');
  const [selectedYear, setSelectedYear] = useState('');

  // Load business list from monthly trends JSON
  useEffect(() => {
    fetch('/business_monthly_trends.json')
      .then((res) => res.json())
      .then((data) => {
        const unique = {};
        data.forEach((record) => {
          if (!unique[record.business_ID]) {
            unique[record.business_ID] = {
              business_ID: record.business_ID,
              business_name: record.business_name,
              state: record.state || '',
              city: record.city || ''
            };
          }
        });
        setBusinesses(Object.values(unique));
      });
  }, []);

  // Select a business and load its chart data
  const handleBusinessSelect = (business) => {
    setSelectedBusiness(business);
    loadGraphData(business, viewType, selectedYear);
  };

  // Load chart data from either monthly or quarterly JSON
  const loadGraphData = (business, view, year) => {
    const file = view === 'monthly'
      ? '/business_monthly_trends.json'
      : '/business_quarterly_trends.json';
    fetch(file)
      .then((res) => res.json())
      .then((data) => {
        let filteredData = data.filter(item => item.business_ID === business.business_ID);
        if (year) {
          filteredData = filteredData.filter(
            (item) => parseInt(item.year, 10) === parseInt(year, 10)
          );
        }
        setGraphData(filteredData);
      });
  };

  // Reload chart when view type or year changes (and a business is selected)
  useEffect(() => {
    if (selectedBusiness) {
      loadGraphData(selectedBusiness, viewType, selectedYear);
    }
  }, [viewType, selectedYear]);

  // Gather unique states from the businesses
  const uniqueStates = Array.from(new Set(businesses.map(b => b.state).filter(Boolean))).sort();

  // Gather unique cities based on the selected state
  const uniqueCities = selectedState
    ? Array.from(
        new Set(
          businesses
            .filter(b => b.state === selectedState)
            .map(b => b.city)
            .filter(Boolean)
        )
      ).sort()
    : [];

  // Filter businesses based on search text, selected state, and selected city
  const filteredBusinesses = businesses.filter((b) => {
    const matchesSearch = b.business_name.toLowerCase().includes(searchText.toLowerCase());
    const matchesState = selectedState ? b.state === selectedState : true;
    const matchesCity = selectedCity ? b.city === selectedCity : true;
    return matchesSearch && matchesState && matchesCity;
  });

  return (
    <div className="analysis-container">
      {/* Left Panel: Filters + Business List */}
      <div className="analysis-left-panel">
        <input
          type="text"
          placeholder="Search by business name"
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          className="analysis-filter"
        />

        <div className="dropdown-group">
          <label>State:</label>
          <select
            value={selectedState}
            onChange={(e) => {
              setSelectedState(e.target.value);
              setSelectedCity(''); // reset city when state changes
            }}
          >
            <option value="">All States</option>
            {uniqueStates.map((state, idx) => (
              <option key={idx} value={state}>
                {state}
              </option>
            ))}
          </select>
        </div>

        {selectedState && (
          <div className="dropdown-group">
            <label>City:</label>
            <select
              value={selectedCity}
              onChange={(e) => setSelectedCity(e.target.value)}
            >
              <option value="">All Cities</option>
              {uniqueCities.map((city, idx) => (
                <option key={idx} value={city}>
                  {city}
                </option>
              ))}
            </select>
          </div>
        )}

        {filteredBusinesses.map((business) => (
          <BusinessCard
            key={business.business_ID}
            business={business}
            onSelect={handleBusinessSelect}
          />
        ))}
      </div>

      {/* Right Panel: Chart Area */}
      <div className="analysis-right-panel">
        {selectedBusiness ? (
          <div className="analysis-card">
            <h2>{selectedBusiness.business_name}</h2>
            <div className="analysis-controls">
              <label>
                View Type:
                <select
                  value={viewType}
                  onChange={(e) => setViewType(e.target.value)}
                >
                  <option value="monthly">Monthly</option>
                  <option value="quarterly">Quarterly</option>
                </select>
              </label>
              <label>
                Year:
                <input
                  type="number"
                  placeholder="e.g., 2020"
                  value={selectedYear}
                  onChange={(e) => setSelectedYear(e.target.value)}
                />
              </label>
            </div>
            <AnalysisGraph data={graphData} viewType={viewType} />
          </div>
        ) : (
          <div className="analysis-card">
            <p>Please select a business to view its rating trends.</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default TimeBasedAnalysis;
