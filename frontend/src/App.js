import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import NavBar from './components/NavBar/NavBar';
import LandingPage from './components/LandingPage/LandingPage';
import TimeBasedAnalysis from './components/TimeBasedAnalysis/TimeBasedAnalysis';
import './App.css';
import SentimentAnalysis from './components/SentimentAnalysis/SentimentAnalysis';

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
          </Routes>
        </div>
      </div>
    </Router>
  );
};

export default App;
