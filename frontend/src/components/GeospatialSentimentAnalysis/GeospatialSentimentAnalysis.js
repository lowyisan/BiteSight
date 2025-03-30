// Contributor(s): Kee Han Xiang

import { useEffect, useState } from "react";
import { MarkerClusterer } from "@googlemaps/markerclusterer";
import STATE_CODE_TO_NAME from './stateCodeMap.json';

function GeospatialSentimentAnalysis() {
  const [allData, setAllData] = useState([]);
  const [filteredData, setFilteredData] = useState([]);
  const [mapInstance, setMapInstance] = useState(null);
  const [circleInstances, setCircleInstances] = useState([]);
  const [infoWindows, setInfoWindows] = useState([]);
  const [markerCluster, setMarkerCluster] = useState(null);
  const [zoomLevel, setZoomLevel] = useState(4);
  const [selectedState, setSelectedState] = useState("");
  const [geoJsonData, setGeoJsonData] = useState(null);
  const [validStates, setValidStates] = useState([]);

  const applyFilter = (type) => {
    let filtered = allData;
    if (type === "positive") filtered = allData.filter((p) => p.weight > 0.25);
    else if (type === "neutral") filtered = allData.filter((p) => p.weight >= -0.25 && p.weight <= 0.25);
    else if (type === "negative") filtered = allData.filter((p) => p.weight < -0.25);
    setFilteredData(filtered);
  };

  useEffect(() => {
    fetch("/geospatial_sentiment.json")
      .then((res) => res.json())
      .then((data) => {
        setAllData(data);
        setFilteredData(data);
        const uniqueStates = [...new Set(data.map((d) => STATE_CODE_TO_NAME[d.state]).filter(Boolean))];
        setValidStates(uniqueStates);
      });

    fetch("/gz_2010_us_040_00_500k.json")
      .then((res) => res.json())
      .then((data) => setGeoJsonData(data));
  }, []);

  useEffect(() => {
    if (!window.google) return;
    const map = new window.google.maps.Map(document.getElementById("map"), {
      center: { lat: 39.8283, lng: -98.5795 },
      zoom: 4,
      mapTypeId: "satellite",
      mapTypeControl: false,
    });
    setMapInstance(map);
    setZoomLevel(map.getZoom());
    map.addListener("zoom_changed", () => setZoomLevel(map.getZoom()));
  }, []);

  useEffect(() => {
    if (!mapInstance || !geoJsonData) return;
    geoJsonData.features.forEach((feature) => mapInstance.data.addGeoJson(feature));
    mapInstance.data.setStyle({ strokeColor: "white", strokeWeight: 1, fillOpacity: 0 });
    geoJsonData.features.forEach((feature) => {
      const coords = feature.geometry.coordinates;
      const bounds = new window.google.maps.LatLngBounds();
      if (feature.geometry.type === "Polygon") coords[0].forEach(([lng, lat]) => bounds.extend({ lat, lng }));
      else if (feature.geometry.type === "MultiPolygon") coords.forEach(p => p[0].forEach(([lng, lat]) => bounds.extend({ lat, lng })));
      const center = bounds.getCenter();
      const stateName = feature.properties.NAME;
      new window.google.maps.Marker({
        position: center,
        map: mapInstance,
        icon: { path: window.google.maps.SymbolPath.CIRCLE, scale: 0.01, strokeWeight: 0 },
        label: { text: stateName, color: "white", fontSize: "12px", fontWeight: "bold" },
        clickable: false,
        zIndex: 999,
      });
    });
  }, [geoJsonData, mapInstance]);

  useEffect(() => {
    if (!mapInstance || filteredData.length === 0) return;
    if (markerCluster) markerCluster.clearMarkers();
    circleInstances.forEach((c) => c.setMap(null));
    infoWindows.forEach((i) => i.close());
    setCircleInstances([]);
    setInfoWindows([]);

    if (zoomLevel < 15) {
      const markers = filteredData.slice(0, 2000).map((p) => {
        const marker = new window.google.maps.Marker({ position: { lat: p.lat, lng: p.lon } });
        marker.sentiment = p.weight;
        return marker;
      });
      const cluster = new MarkerClusterer({
        map: mapInstance,
        markers,
        renderer: {
          render({ count, markers, position }) {
            const avg = markers.reduce((sum, m) => sum + (m.sentiment ?? 0), 0) / markers.length;
            const color = avg > 0.25 ? "green" : avg < -0.25 ? "red" : "gold";
            return new window.google.maps.Marker({
              position,
              label: { text: String(count), color: "white", fontSize: "12px" },
              icon: {
                path: window.google.maps.SymbolPath.CIRCLE,
                fillColor: color,
                fillOpacity: 0.8,
                strokeColor: "white",
                strokeWeight: 1,
                scale: 20,
              },
            });
          },
        },
      });
      setMarkerCluster(cluster);
      return;
    }

    const bounds = mapInstance.getBounds();
    if (!bounds) return;
    const visiblePoints = filteredData.filter((p) => bounds.contains(new window.google.maps.LatLng(p.lat, p.lon)));
    const newCircles = [];
    const newInfoWindows = [];

    visiblePoints.forEach((p) => {
      let fillColor = "#ffeb3b", category = "Neutral";
      if (p.weight > 0.25) [fillColor, category] = ["#00ff00", "Positive"];
      else if (p.weight < -0.25) [fillColor, category] = ["#ff0000", "Negative"];

      const circle = new window.google.maps.Circle({
        strokeColor: fillColor,
        strokeOpacity: 0.7,
        strokeWeight: 1,
        fillColor,
        fillOpacity: 0.4,
        map: mapInstance,
        center: { lat: p.lat, lng: p.lon },
        radius: 20,
      });

      let formattedHours = "Unavailable";
      try {
        const hoursObj = typeof p.opening_hours === "string" ? JSON.parse(p.opening_hours.replace(/'/g, '"')) : p.opening_hours;
        if (hoursObj && typeof hoursObj === "object") {
          formattedHours = Object.entries(hoursObj).map(([day, time]) => `${day}: ${time}`).join("<br/>");
        }
      } catch {}

      const infoWindow = new window.google.maps.InfoWindow({
        content: `<div style="font-family: Arial; font-size: 13px; line-height: 1.6; min-width: 240px; padding: 8px; border-radius: 8px;">
            <div style="font-size: 15px; font-weight: bold; margin-bottom: 6px;">Name: <u>${p.name}</u></div>
            <div><b>Star Rating:</b> ⭐ ${p.avg_star_rating}</div>
            <div><b>Address:</b> ${p.address}</div>
            <div><b>Postal Code:</b> ${p.postal}</div>
            <div><b>Categories:</b> ${p.categories}</div>
            <div><b>Hours:</b><br/>${formattedHours}</div>
            <div style="margin-top: 6px;"><b>Sentiment Score:</b> ${p.weight.toFixed(3)}</div>
          </div>`,
        position: { lat: p.lat, lng: p.lon },
      });

      circle.addListener("click", () => {
        newInfoWindows.forEach((i) => i.close());
        infoWindow.open(mapInstance);
      });

      newCircles.push(circle);
      newInfoWindows.push(infoWindow);
    });

    setCircleInstances(newCircles);
    setInfoWindows(newInfoWindows);
  }, [filteredData, mapInstance, zoomLevel]);

  const handleStateSelect = (stateName) => {
    setSelectedState(stateName);
    if (!geoJsonData || !mapInstance) return;
    const feature = geoJsonData.features.find((f) => f.properties.NAME === stateName);
    if (!feature) return;
    const bounds = new window.google.maps.LatLngBounds();
    const coords = feature.geometry.coordinates;
    if (feature.geometry.type === "Polygon") coords[0].forEach(([lng, lat]) => bounds.extend({ lat, lng }));
    else if (feature.geometry.type === "MultiPolygon") coords.forEach(p => p[0].forEach(([lng, lat]) => bounds.extend({ lat, lng })));
    mapInstance.fitBounds(bounds);
  };

  return (
    <>
      <div style={{ position: "absolute", zIndex: 1, padding: "10px" }}>
        <div style={{ marginBottom: "8px" }}>
          <select value={selectedState} onChange={(e) => handleStateSelect(e.target.value)}>
            <option value="">Zoom to State</option>
            {validStates.map((state) => <option key={state} value={state}>{state}</option>)}
          </select>
        </div>
        <div style={{ marginBottom: "8px" }}>
          <button onClick={() => setFilteredData(allData)}>All</button>
          <button onClick={() => applyFilter("positive")}>Positive</button>
          <button onClick={() => applyFilter("neutral")}>Neutral</button>
          <button onClick={() => applyFilter("negative")}>Negative</button>
        </div>
        <span style={{ color: "white", fontWeight: "bold" }}>
          {filteredData.length} locations — Zoom: {zoomLevel}
        </span>
      </div>
      <div id="map" style={{ height: "100vh", width: "100%" }}></div>
    </>
  );
}

export default GeospatialSentimentAnalysis;
