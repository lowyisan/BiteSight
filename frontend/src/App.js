import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import NavBar from './components/NavBar/NavBar';
import LandingPage from './components/LandingPage/LandingPage';
import TimeBasedAnalysis from './components/TimeBasedAnalysis/TimeBasedAnalysis';
import SentimentAnalysis from './components/SentimentAnalysis/SentimentAnalysis';
import CityBasedAnalysis from './components/CityBasedAnalysis/CityBasedAnalysis';
import GeospatialSentimentAnalysis from './components/GeospatialSentimentAnalysis/GeospatialSentimentAnalysis';
import './App.css';

const App = () => {
  return (
    <Router>
      <div className="app-container">
        <NavBar />
        <div className="main-content">
          <Routes>
            <Route path="/" element={<LandingPage />} />
            <Route path="/analysis" element={<TimeBasedAnalysis />} />
            <Route path="/sentiment" element={<SentimentAnalysis />} />
            <Route path="/city-analysis" element={<CityBasedAnalysis />} />
            <Route path="/GeospatialSentimentAnalysis" element={<GeospatialSentimentAnalysis />} />
          </Routes>
        </div>
      </div>
    </Router>
  );
};

export default App;
