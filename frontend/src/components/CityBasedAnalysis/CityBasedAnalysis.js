import React, { useState, useEffect } from 'react';
import CityBasedInsights from './CityBasedInsights';

const CityBasedAnalysis = () => {
  const [businesses, setBusinesses] = useState([]);
  const [selectedCategory, setSelectedCategory] = useState('');
  const [selectedState, setSelectedState] = useState('');
  const [selectedCity, setSelectedCity] = useState('');
  const [analysisType, setAnalysisType] = useState('Top Categories'); // default to Top Categories

  useEffect(() => {
    fetch('/city_enriched_business.json')
      .then(res => res.json())
      .then(setBusinesses);
  }, []);

  const foodKeywords = [
    "food", "restaurant", "restaurants", "cafe", "bakeries", "coffee", "tea", "deli",
    "desserts", "juice", "smoothies", "ice cream", "steak", "pizza", "bar", "grill",
    "bbq", "sushi", "chinese", "thai", "korean", "asian", "indian", "mediterranean",
    "mexican", "halal", "vegan", "vegetarian", "burgers", "seafood", "noodles", "hotpot",
    "ramen", "brunch", "bubble tea", "wine", "beer", "snacks", "salad", "soup"
  ];

  const uniqueStates = Array.from(new Set(businesses.map(b => b.state))).sort();
  const uniqueCities = selectedState
    ? Array.from(new Set(businesses.filter(b => b.state === selectedState).map(b => b.city)))
    : [];

  const uniqueCategories = Array.from(
    new Set(
      businesses
        .flatMap(b => b.categories?.split(',').map(c => c.trim().toLowerCase()) || [])
        .filter(cat => foodKeywords.includes(cat))
    )
  ).sort();

  return (
    <div className="analysis-container">
      <div className="analysis-left-panel">
        <div className="filters-section">
          
          <div className="dropdown-group">
            <label>Analysis Type:</label>
            <select
              value={analysisType}
              onChange={(e) => {
                setAnalysisType(e.target.value);
              }}
            >
              <option value="Top Categories">Top Categories</option>
              <option value="Average Business Hours">Average Business Hours</option>
              <option value="Hotspot Cities">Hotspot Cities</option>
            </select>
          </div>

          {/* Category only for Hotspot Cities */}
          {analysisType === 'Hotspot Cities' && (
            <div className="dropdown-group">
              <label>Category:</label>
              <select
                value={selectedCategory}
                onChange={(e) => setSelectedCategory(e.target.value)}
              >
                <option value="">All Categories</option>
                {uniqueCategories.map((c, i) => <option key={i} value={c}>{c}</option>)}
              </select>
            </div>
          )}

          {/* State for Top Categories and Avg Business Hours */}
          {(analysisType === 'Top Categories' || analysisType === 'Average Business Hours') && (
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
                {uniqueStates.map((s, i) => <option key={i} value={s}>{s}</option>)}
              </select>
            </div>
          )}

          {/* City only for Top Categories */}
          {selectedState && analysisType === 'Top Categories' && (
            <div className="dropdown-group">
              <label>City:</label>
              <select
                value={selectedCity}
                onChange={(e) => setSelectedCity(e.target.value)}
              >
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
