import pandas as pd
import json

# Load your processed CSV
df = pd.read_csv("business_sentiment.csv")

# Extract the data format needed for heatmap
data = []
for _, row in df.iterrows():
    data.append({
        "name": row["name"],
        "lat": row["lat"],
        "lon": row["lon"],
        "weight": row["sentiment_score"],
        "address": row["address"],
        "postal": row["postal"],
        "avg_star_rating": row["avg_star_rating"],
        "categories": row["categories"],
        "opening_hours": row["opening_hours"]
    })

# Save as JSON
with open("sentiment.json", "w", encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("Conversion complete! The data is saved to sentiment.json.")
