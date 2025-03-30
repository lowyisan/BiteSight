/* Contributor(s): Nadhirah Binti Ayub Khan */

import React from 'react';
import './RecommendationCard.css';

const RecommendationCard = ({ recommendation, onSelect }) => {
  return (
    <div
      className="reco-card"
      onClick={() => onSelect(recommendation)}
      style={{ cursor: 'pointer' }}
    >
      <h5>{recommendation.recommended_name}</h5>

      {recommendation.address && (
        <p style={{ margin: '4px 0', fontSize: '0.9rem' }}>
          {recommendation.address}, {recommendation.postal}
        </p>
      )}

      {(recommendation.city || recommendation.state) && (
        <p style={{ margin: '0 0 4px 0', fontSize: '0.85rem', color: '#555' }}>
          {recommendation.city}{recommendation.city && recommendation.state ? ', ' : ''}{recommendation.state}
        </p>
      )}

      {recommendation.avg_rating && (
        <p style={{ margin: '0', fontSize: '0.9rem' }}>
          ⭐ Rating: {parseFloat(recommendation.avg_rating).toFixed(1)}
        </p>
      )}
    </div>
  );
};

export default RecommendationCard;
