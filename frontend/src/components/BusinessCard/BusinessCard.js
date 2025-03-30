/* Contributor(s): Low Yi San */

import React from 'react';
import './BusinessCard.css';

const BusinessCard = ({ business, onSelect }) => {
  return (
    <div className="business-card" onClick={() => onSelect(business)}>
      <h3>{business.business_name}</h3>
      <p>{business.city}, {business.state}</p>
    </div>
  );
};

export default BusinessCard;
