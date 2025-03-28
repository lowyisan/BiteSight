import React from 'react';
import './BusinessCard.css';

const BusinessCard = ({ business, onSelect }) => {
  return (
    <div className="business-card" onClick={() => onSelect(business)}>
      <h3>{business.business_name}</h3>
      {/* <p>ID: {business.business_ID}</p> */}
    </div>
  );
};

export default BusinessCard;
