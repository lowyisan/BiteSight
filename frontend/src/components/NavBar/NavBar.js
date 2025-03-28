import React from 'react';
import { Link } from 'react-router-dom';
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
          <Link to="/">
            <FaHome className="sidebar-icon" />
            <span>Overview</span>
          </Link>
        </li>
        <li>
          <a href="#/">
            <FaRegSmileBeam className="sidebar-icon" />
            <span>Sentiment-Based Analysis</span>
          </a>
        </li>
        <li>
          <Link to="/analysis">
            <FaClock className="sidebar-icon" />
            <span>Time-Series Analysis</span>
          </Link>
        </li>
        <li>
          <a href="#/">
            <FaMapMarkedAlt className="sidebar-icon" />
            <span>Geospatial Analysis</span>
          </a>
        </li>
        <li>
          <a href="#/">
            <FaCity className="sidebar-icon" />
            <span>City-Based Analysis</span>
          </a>
        </li>
      </ul>
    </div>
  );
};

export default NavBar;
