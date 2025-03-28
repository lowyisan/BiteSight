import React from 'react';
import { Link } from 'react-router-dom';
import './NavBar.css';
import { 
  FaHome, 
  FaChartLine, 
  FaShoppingCart, 
  FaCog, 
  FaSignOutAlt 
} from 'react-icons/fa';

const NavBar = () => {
  return (
    <div className="sidebar">
      <div className="sidebar-logo">
        Big Data
      </div>
      <ul className="sidebar-menu">
        <li>
          <Link to="/">
            <FaHome className="sidebar-icon" />
            <span>Overview</span>
          </Link>
        </li>
        <li>
          <Link to="/analysis">
            <FaChartLine className="sidebar-icon" />
            <span>Time-Series Analysis</span>
          </Link>
        </li>
        <li>
          <a href="#/">
            <FaShoppingCart className="sidebar-icon" />
            <span>City-Based Analysis</span>
          </a>
        </li>
        <li>
          <a href="#/">
            <FaCog className="sidebar-icon" />
            <span>Settings</span>
          </a>
        </li>
        <li>
          <a href="#/">
            <FaSignOutAlt className="sidebar-icon" />
            <span>Logout</span>
          </a>
        </li>
      </ul>
    </div>
  );
};

export default NavBar;
