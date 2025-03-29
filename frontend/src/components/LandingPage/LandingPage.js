import React, { useState, useEffect } from 'react';
import Papa from 'papaparse';
import BusinessCard from '../BusinessCard/BusinessCard';
import CityAnalysisGraph from './LandingPageGraph';
import './LandingPage.css';

// MUI components
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
  const [selectedBusiness, setSelectedBusiness] = useState(null);
  const [searchText, setSearchText] = useState('');
  const [selectedCategory, setSelectedCategory] = useState('');
  const [selectedState, setSelectedState] = useState('');
  const [selectedCity, setSelectedCity] = useState('');

  useEffect(() => {
    Papa.parse('/yelp_summary_kpis.csv', {
      download: true,
      header: true,
      complete: (results) => setSummaryKPIs(results.data),
    });

    fetch('/city_enriched_business.json')
      .then(res => res.json())
      .then(setBusinesses);
  }, []);

  const metricLabels = {
    total_businesses: "Total Businesses",
    total_reviews: "Total Reviews",
    overall_avg_rating: "Overall Average Rating",
    num_states: "Number of States",
    num_cities: "Number of Cities"
  };

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
            {uniqueCategories.map((cat, i) => <option key={i} value={cat}>{cat}</option>)}
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

      {/* MODAL */}
      <Dialog
        open={!!selectedBusiness}
        onClose={() => setSelectedBusiness(null)}
        TransitionComponent={Transition}
        keepMounted={false}
        unmountOnExit
        fullWidth
        maxWidth="md"
        PaperProps={{
          style: {
            borderRadius: 12,
            transition: 'all 0.35s ease-in-out'
          }
        }}
        BackdropProps={{
          style: { backdropFilter: 'blur(2px)' }
        }}
      >
        {selectedBusiness && (
          <>
            <DialogTitle sx={{ m: 0, p: 2 }}>
              {selectedBusiness.business_name}
              <IconButton
                aria-label="close"
                onClick={() => setSelectedBusiness(null)}
                sx={{
                  position: 'absolute',
                  right: 8,
                  top: 8,
                  color: (theme) => theme.palette.grey[500],
                }}
              >
                <CloseIcon />
              </IconButton>
            </DialogTitle>
            <div style={{ padding: '0 24px 24px' }}>
              <CityAnalysisGraph business={selectedBusiness} />
            </div>
          </>
        )}
      </Dialog>
    </div>
  );
};

export default LandingPage;
