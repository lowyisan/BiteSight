import React from 'react';
import { NavLink } from 'react-router-dom';
import './NavBar.css';
import { 
  FaHome, 
  FaChartLine, 
  FaCity, 
  FaClock, 
  FaRegSmileBeam,
  FaMapMarkedAlt
} from 'react-icons/fa';
import logo from '../../assets/images/logo.png'

const NavBar = () => {
  return (
    <div className="sidebar">
      <div className="sidebar-logo">
        <img src={logo} alt="Logo" />
      </div>
      <ul className="sidebar-menu">
        <li>
          <NavLink 
            to="/" 
            className={({ isActive }) => (isActive ? 'active-link' : undefined)}
          >
            <FaHome className="sidebar-icon" />
            <span>Overview</span>
          </NavLink>
        </li>
        <li>
          <NavLink
            to="/sentiment"
            className={({ isActive }) => (isActive ? 'active-link' : undefined)}
          >
            <FaRegSmileBeam className="sidebar-icon" />
            <span>Sentiment-Based Analysis</span>
          </NavLink>
        </li>
        <li>
          <NavLink 
            to="/analysis"
            className={({ isActive }) => (isActive ? 'active-link' : undefined)}
          >
            <FaClock className="sidebar-icon" />
            <span>Time-Series Analysis</span>
          </NavLink>
        </li>
        <li>
          <a href="#/">
            <FaMapMarkedAlt className="sidebar-icon" />
            <span>Geospatial Analysis</span>
          </a>
        </li>
        <li>
          <NavLink 
            to="/city-analysis"
            className={({ isActive }) => (isActive ? 'active-link' : undefined)}
          >
            <FaCity className="sidebar-icon" />
            <span>City-Based Analysis</span>
          </NavLink>
        </li>
      </ul>
    </div>
  );
};

export default NavBar;
