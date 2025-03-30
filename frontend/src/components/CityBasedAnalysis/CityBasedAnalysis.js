// Contributor(s): Michelle Magdalene Trisoeranto

import React, { useState, useEffect } from 'react';
import CityBasedInsights from './CityBasedInsights';
import { extractFoodCategories } from '../../utils/categoryUtils';

const CityBasedAnalysis = () => {
  const [businesses, setBusinesses] = useState([]);
  const [categories, setCategories] = useState([]);
  const [selectedCategory, setSelectedCategory] = useState('');
  const [selectedState, setSelectedState] = useState('');
  const [selectedCity, setSelectedCity] = useState('');
  const [analysisType, setAnalysisType] = useState('Top Categories');

  useEffect(() => {
    fetch('/business_details.json')
      .then(res => res.json())
      .then(data => {
        setBusinesses(data);
        setCategories(extractFoodCategories(data)); // replaces extractUniqueFoodCategories
      });
  }, []);

  const uniqueStates = Array.from(new Set(businesses.map(b => b.state))).sort();
  const uniqueCities = selectedState
    ? Array.from(new Set(businesses.filter(b => b.state === selectedState).map(b => b.city)))
    : [];

  return (
    <div className="analysis-container">
      <div className="analysis-left-panel">
        <div className="filters-section">
          <div className="dropdown-group">
            <label>Analysis Type:</label>
            <select value={analysisType} onChange={(e) => setAnalysisType(e.target.value)}>
              <option value="Top Categories">Top Categories</option>
              <option value="Average Business Hours">Average Business Hours</option>
              <option value="Hotspot Cities">Hotspot Cities</option>
            </select>
          </div>

          {analysisType === 'Hotspot Cities' && (
            <div className="dropdown-group">
              <label>Category:</label>
              <select value={selectedCategory} onChange={(e) => setSelectedCategory(e.target.value)}>
                <option value="">All Categories</option>
                {categories.map((c, i) => <option key={i} value={c}>{c}</option>)}
              </select>
            </div>
          )}

          {(analysisType === 'Top Categories' || analysisType === 'Average Business Hours') && (
            <div className="dropdown-group">
              <label>State:</label>
              <select value={selectedState} onChange={(e) => {
                setSelectedState(e.target.value);
                setSelectedCity('');
              }}>
                <option value="">All States</option>
                {uniqueStates.map((s, i) => <option key={i} value={s}>{s}</option>)}
              </select>
            </div>
          )}

          {selectedState && analysisType === 'Top Categories' && (
            <div className="dropdown-group">
              <label>City:</label>
              <select value={selectedCity} onChange={(e) => setSelectedCity(e.target.value)}>
                <option value="">All Cities</option>
                {uniqueCities.map((c, i) => <option key={i} value={c}>{c}</option>)}
              </select>
            </div>
          )}
        </div>
      </div>

      <div className="analysis-right-panel">
        <div className="analysis-card">
          <CityBasedInsights
            analysisType={analysisType}
            selectedCategory={selectedCategory}
            selectedState={selectedState}
            selectedCity={selectedCity}
          />
        </div>
      </div>
    </div>
  );
};

export default CityBasedAnalysis;
