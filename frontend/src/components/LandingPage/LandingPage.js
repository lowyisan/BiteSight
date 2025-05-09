/* Contributor(s): Low Yi San, Nadhirah Binti Ayub Khan, Michelle Magdelene Trisoeranto */

import React, { useState, useEffect } from 'react';
import Papa from 'papaparse';
import BusinessCard from '../BusinessCard/BusinessCard';
import CityAnalysisGraph from './LandingPageGraph';
import './LandingPage.css';
import { extractFoodCategories } from '../../utils/categoryUtils'; // ✅ Dynamic filter

// MUI
import Dialog from '@mui/material/Dialog';
import DialogTitle from '@mui/material/DialogTitle';
import IconButton from '@mui/material/IconButton';
import CloseIcon from '@mui/icons-material/Close';
import Fade from '@mui/material/Fade';

const Transition = React.forwardRef((props, ref) => (
  <Fade ref={ref} timeout={{ enter: 300, exit: 250 }} {...props} />
));

const LandingPage = () => {
  const [summaryKPIs, setSummaryKPIs] = useState([]);
  const [businesses, setBusinesses] = useState([]);
  const [categories, setCategories] = useState([]);
  const [recommendationMap, setRecommendationMap] = useState({});
  const [selectedBusiness, setSelectedBusiness] = useState(null);
  const [searchText, setSearchText] = useState('');
  const [selectedCategory, setSelectedCategory] = useState('');
  const [selectedState, setSelectedState] = useState('');
  const [selectedCity, setSelectedCity] = useState('');

  useEffect(() => {
    // Load KPIs
    Papa.parse('/yelp_summary_kpis.csv', {
      download: true,
      header: true,
      complete: (results) => setSummaryKPIs(results.data),
    });

    // Load businesses + similar recs
    fetch('/business_details.json')
      .then(res => res.json())
      .then((businessesData) => {
        setBusinesses(businessesData);
        setCategories(extractFoodCategories(businessesData)); // ✅ Dynamic category extraction

        fetch('/top5_similar_businesses.json')
          .then(res => res.text())
          .then(text => {
            const lines = text.split('\n').filter(Boolean);
            const map = {};
            lines.forEach(line => {
              try {
                const entry = JSON.parse(line);
                const enrichedRecs = entry.recommendations.map((rec) => {
                  const meta = businessesData.find(b => b.business_id === rec.recommended_business_id);
                  return {
                    ...rec,
                    city: meta?.city || '',
                    state: meta?.state || '',
                    address: meta?.address || '',
                    postal: meta?.postal_code || '',
                    avg_rating: meta?.avg_rating || null
                  };
                });
                map[entry.business_id] = enrichedRecs;
              } catch (e) {
                console.warn('Invalid JSON line in recommendations:', line);
              }
            });
            setRecommendationMap(map);
          });
      });
  }, []);

  const metricLabels = {
    total_businesses: "Total Businesses",
    total_reviews: "Total Reviews",
    overall_avg_rating: "Overall Average Rating",
    num_states: "Number of States",
    num_cities: "Number of Cities"
  };

  const uniqueStates = Array.from(new Set(businesses.map(b => b.state))).sort();
  const uniqueCities = selectedState
    ? Array.from(new Set(businesses.filter(b => b.state === selectedState).map(b => b.city)))
    : [];

  const filteredBusinesses = businesses.filter(b => {
    const nameMatch = b.business_name.toLowerCase().includes(searchText.toLowerCase());
    const stateMatch = selectedState ? b.state === selectedState : true;
    const cityMatch = selectedCity ? b.city === selectedCity : true;
    const categoryMatch = selectedCategory
      ? b.categories?.toLowerCase().includes(selectedCategory)
      : true;
    return nameMatch && stateMatch && cityMatch && categoryMatch;
  });

  return (
    <div className="landing-container">
      <div className="top-bar">
        <h1>Analytics Dashboard</h1>
      </div>

      <div className="kpi-row single-row">
        {summaryKPIs.map((kpi, index) => (
          <div key={index} className="kpi-card">
            <h3>{metricLabels[kpi.metric] || kpi.metric}</h3>
            <p>{kpi.value}</p>
          </div>
        ))}
      </div>

      <div className="overview-search-container">
        <div className="overview-search-filters">
          <input
            type="text"
            placeholder="Search by business name"
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            className="overview-search-bar"
          />

          <select value={selectedCategory} onChange={(e) => setSelectedCategory(e.target.value)}>
            <option value="">All Categories</option>
            {categories.map((cat, i) => <option key={i} value={cat}>{cat}</option>)}
          </select>

          <select value={selectedState} onChange={(e) => {
            setSelectedState(e.target.value);
            setSelectedCity('');
          }}>
            <option value="">All States</option>
            {uniqueStates.map((s, i) => <option key={i} value={s}>{s}</option>)}
          </select>

          <select value={selectedCity} onChange={(e) => setSelectedCity(e.target.value)}>
            <option value="">All Cities</option>
            {uniqueCities.map((c, i) => <option key={i} value={c}>{c}</option>)}
          </select>
        </div>

        <div className="overview-business-cards-container">
          <div className="horizontal-scroll">
            {filteredBusinesses.map((b) => (
              <BusinessCard
                key={b.business_id}
                business={b}
                onSelect={() => setSelectedBusiness(b)}
              />
            ))}
          </div>
        </div>
      </div>

      {/* Modal for business chart and details */}
      <Dialog
        open={!!selectedBusiness}
        onClose={() => setSelectedBusiness(null)}
        TransitionComponent={Transition}
        keepMounted={false}
        unmountOnExit
        fullWidth
        maxWidth="md"
        PaperProps={{ style: { borderRadius: 12 } }}
        BackdropProps={{ style: { backdropFilter: 'blur(2px)' } }}
      >
        {selectedBusiness && (
          <>
            <DialogTitle sx={{ m: 0, p: 2 }}>
              <div style={{ fontSize: '1.25rem', fontWeight: 600 }}>
                {selectedBusiness.business_name}
              </div>
              <IconButton
                aria-label="close"
                onClick={() => setSelectedBusiness(null)}
                sx={{ position: 'absolute', right: 8, top: 8, color: 'gray' }}
              >
                <CloseIcon />
              </IconButton>
            </DialogTitle>
            <div style={{ padding: '0 24px 24px' }}>
              <CityAnalysisGraph
                business={selectedBusiness}
                recommendations={recommendationMap[selectedBusiness.business_id] || []}
                businesses={businesses}
                onBusinessSelect={setSelectedBusiness}
              />
            </div>
          </>
        )}
      </Dialog>
    </div>
  );
};

export default LandingPage;
