// // MiniCard.js
// import React from 'react';
// import './RecommendationCard.css';

// const RecommendationCard = ({ recommendation }) => {
//   return (
//     <div className="reco-card">
//       <h5>{recommendation.recommended_name}</h5>
//       <p>Similarity: {(recommendation.similarity * 100).toFixed(1)}%</p>
//     </div>
//   );
// };

// export default RecommendationCard;


import React from 'react';
import './RecommendationCard.css';

const RecommendationCard = ({ recommendation }) => {
  return (
    <div className="reco-card">
      <h5>{recommendation.recommended_name}</h5>
      {recommendation.state && (
        <p style={{ margin: 0 }}>{recommendation.city}, {recommendation.state}</p>
      )}
      {recommendation.avg_rating && (
        <p style={{ margin: '4px 0' }}>
          ⭐ Rating: {parseFloat(recommendation.avg_rating).toFixed(1)}
        </p>
      )}
    </div>
  );
};

export default RecommendationCard;
