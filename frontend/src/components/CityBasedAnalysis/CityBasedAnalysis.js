import React, { useState, useEffect } from 'react';
import BusinessCard from '../BusinessCard/BusinessCard';
import CityAnalysisGraph from './CityAnalysisGraph';
import CityBasedInsights from './CityBasedInsights';
import './CityBasedAnalysis.css';

const CityBasedAnalysis = () => {
  const [businesses, setBusinesses] = useState([]);
  const [selectedBusiness, setSelectedBusiness] = useState(null);
  const [selectedCategory, setSelectedCategory] = useState('');
  const [selectedState, setSelectedState] = useState('');
  const [selectedCity, setSelectedCity] = useState('');
  const [searchText, setSearchText] = useState('');
  const [analysisType, setAnalysisType] = useState('None');

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

  const filteredBusinesses = businesses.filter(b => {
    const nameMatch = b.business_name.toLowerCase().includes(searchText.toLowerCase());
    const stateMatch = selectedState ? b.state === selectedState : true;
    const cityMatch = selectedCity ? b.city === selectedCity : true;
    const categoryMatch = selectedCategory
      ? b.categories?.toLowerCase().includes(selectedCategory)
      : true;
    return nameMatch && stateMatch && cityMatch && categoryMatch;
  });

  const handleBusinessSelect = (b) => setSelectedBusiness(b);

  return (
    <div className="analysis-container">
      <div className="analysis-left-panel">
        <div className="filters-section">
          <input
            type="text"
            placeholder="Search by business name"
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            className="analysis-filter"
          />
          
          <div className="dropdown-group">
            <label>Analysis Type:</label>
            <select value={analysisType} onChange={(e) => {
                setAnalysisType(e.target.value);
                setSelectedBusiness(null); // reset selected business when switching to insights
            }}>
                <option value="None">None</option>
                <option value="Top Categories">Top Categories</option>
                <option value="Average Business Hours">Average Business Hours</option>
                <option value="Hotspot Cities">Hotspot Cities</option>
            </select>
          </div>

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

          {selectedState && (
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

        {analysisType === 'None' && (
          <div className="business-list">
            {filteredBusinesses.map(b => (
              <BusinessCard
                key={b.business_id}
                business={b}
                onSelect={handleBusinessSelect}
              />
            ))}
          </div>
        )}
      </div>

      <div className="analysis-right-panel">
        {analysisType === 'None' ? (
          selectedBusiness ? (
            <div className="analysis-card">
              <h2>{selectedBusiness.business_name}</h2>
              <CityAnalysisGraph business={selectedBusiness} />
            </div>
          ) : (
            <div className="analysis-card">
              <p>Please select a business to view its review breakdown and details.</p>
            </div>
          )
        ) : (
            <div className="analysis-card">
            <CityBasedInsights
                analysisType={analysisType}
                selectedCategory={selectedCategory}
                selectedState={selectedState}
                selectedCity={selectedCity}
            />
          </div>
        )}
      </div>
    </div>
  );
};

export default CityBasedAnalysis;