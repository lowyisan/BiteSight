/* Contributor(s): Low Yi San */

import React, { useState, useEffect } from 'react';
import BusinessCard from '../BusinessCard/BusinessCard';
import AnalysisGraph from './AnalysisGraph';
import SpikeDipAnalysis from '../SpikeDipAnalysis/SpikeDipAnalysis';
import './TimeBasedAnalysis.css';

const TimeBasedAnalysis = () => {
  const [businesses, setBusinesses] = useState([]);
  const [selectedBusiness, setSelectedBusiness] = useState(null);
  const [spikeDipData, setSpikeDipData] = useState([]);

  // For text search, state dropdown, and city dropdown
  const [searchText, setSearchText] = useState('');
  const [selectedState, setSelectedState] = useState('');
  const [selectedCity, setSelectedCity] = useState('');

  // For chart data
  const [graphData, setGraphData] = useState([]);
  const [viewType, setViewType] = useState('monthly');
  const [selectedYear, setSelectedYear] = useState('');
  const [selectedMonth, setSelectedMonth] = useState(''); // For monthly view

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

  // Load spike/dip topic modeling data
  useEffect(() => {
    fetch('/spike_dip_topic_modeling.json')
      .then((res) => res.json())
      .then((data) => setSpikeDipData(data))
      .catch((err) => {
        console.error("Error fetching spike/dip topic modeling data:", err);
        setSpikeDipData([]);
      });
  }, []);

  // Select a business and reset year & month, then load its chart data
  const handleBusinessSelect = (business) => {
    setSelectedBusiness(business);
    // Reset year and month when a new card is clicked.
    setSelectedYear('');
    setSelectedMonth('');
    loadGraphData(business, viewType, '');
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

  // Reload chart when view type or year changes (if a business is selected)
  useEffect(() => {
    if (selectedBusiness) {
      loadGraphData(selectedBusiness, viewType, selectedYear);
    }
  }, [viewType, selectedYear]);

  // Callback for clicking a data point on the chart
  const handleDataPointClick = (label) => {
    // Label format "YYYY-MM" for monthly view
    const [year, month] = label.split("-");
    setSelectedYear(year);
    setSelectedMonth(month);
  };

  // Get unique states from the businesses
  const uniqueStates = Array.from(new Set(businesses.map(b => b.state).filter(Boolean))).sort();
  // Get unique cities based on the selected state
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

  // Filter businesses based on search text, state, and city
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
        {/* Sticky filters at the top */}
        <div className="filters-section">
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
                setSelectedCity('');
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
        </div>

        {/* Scrollable business list below the filters */}
        <div className="business-list">
          {filteredBusinesses.map((business) => (
            <BusinessCard
              key={business.business_ID}
              business={business}
              onSelect={handleBusinessSelect}
            />
          ))}
        </div>
      </div>

      {/* Right Panel: Chart Area and Spike/Dip Word Cloud */}
      <div className="analysis-right-panel">
        {selectedBusiness ? (
          <div className="analysis-card">
            <h2>{selectedBusiness.business_name}</h2>
            <div className="analysis-controls">
              <label>
                View Type:
                <select
                  value={viewType}
                  onChange={(e) => {
                    setViewType(e.target.value);
                    // Reset month when switching view types.
                    setSelectedMonth('');
                  }}
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
                  onChange={(e) => {
                    setSelectedYear(e.target.value);
                    setSelectedMonth('');
                  }}
                />
              </label>
            </div>
            <AnalysisGraph data={graphData} viewType={viewType} onDataPointClick={handleDataPointClick} />
            {viewType === 'monthly' && selectedYear && selectedMonth && (
              <SpikeDipAnalysis
                business={selectedBusiness}
                selectedYear={selectedYear}
                selectedMonth={selectedMonth}
                spikeDipData={spikeDipData}
              />
            )}
          </div>
        ) : (
          <div className="analysis-card">
            <p>Please select a business to view its rating trends and spike/dip analysis.</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default TimeBasedAnalysis;
