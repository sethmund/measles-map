# 🗺️ US Measles Tracker (Live)

![Status](https://img.shields.io/badge/Status-Live-green)

An interactive, drill-down map of Measles cases in the United States, built with **D3.js**.

### 🔴 [Click Here to View the Live Map](https://sethmund.github.io/measles-map/)

---

## ℹ️ About
This project visualizes current measles outbreak data using a "drill-down" interface.
- **State Level:** Shows a national overview.
- **County Level:** Click on any state to zoom in and reveal granular county-level data.
- **Live Data:** The map fetches the latest daily report directly from the JHU CSSE raw data file every time the page loads.

## 📊 Data Source
Data is sourced dynamically from the [Johns Hopkins University (JHU) CSSE Measles Data Repository](https://github.com/CSSEGISandData/measles_data).

- **Raw Data Source:** `measles_daily_cases_by_county.csv`
- **Update Frequency:** As updated by the JHU team.

## 🛠️ Tech Stack
- **D3.js (v7):** For map rendering, transitions, and interaction.
- **TopoJSON:** For lightweight map geometry (`us-atlas`).
- **HTML/CSS:** For layout and styling.
