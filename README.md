# 🗺️ North American Measles Tracker (Live)

![Status](https://img.shields.io/badge/Status-Live-green)

An interactive, drill-down epidemiological map of laboratory-confirmed measles cases across North America (United States, Canada, and Mexico), built with **D3.js**.

### 🔴 [Click Here to View the Live Map](https://sethmund.github.io/measles-map/)

---

## ℹ️ About
This project visualizes the current North American measles outbreak using a multi-resolution geographic interface.
- **State/Provincial Level:** Displays a unified continental overview mapping Canadian provinces, Mexican states, and US states.
- **County Level (US):** Click-to-zoom functionality on US states reveals granular county-level case data.
- **Automated Data Pipeline:** A GitHub Actions ETL workflow executes daily to scrape, clean, and merge disparate international data sources into a standardized `measles_na_update.csv`.

## 📊 Data Sources
Data is sourced and aggregated dynamically from international health authorities and academic repositories:
- **United States:** [Johns Hopkins University (JHU) CSSE Measles Data Repository](https://github.com/CSSEGISandData/measles_data).
- **Canada:** [Public Health Agency of Canada (PHAC) Health Infobase](https://health-infobase.canada.ca/measles-rubella/).
- **Mexico:** Secretaría de Salud / Dirección General de Epidemiología (DGE) Daily Epidemiological Reports (extracted dynamically from official PDFs).

## 🛠️ System Architecture & Tech Stack

**Frontend (Visualization):**
- **D3.js (v7):** For spatial rendering, geometric transitions, and choropleth threshold scaling.
- **TopoJSON:** Custom unified continental topology (`NA.json`) to handle multi-national geometries and eliminate spatial gaps.
- **HTML/CSS:** Interface layout, legend generation, and tooltip styling.

**Backend (ETL Pipeline):**
- **Python 3:** Data extraction, transformation, and loading.
- **Libraries:** `pandas` (data manipulation), `pdfplumber` (positional extraction of Mexican clinical tables), `BeautifulSoup` (dynamic URL resolution for daily reports), `requests`.
- **GitHub Actions:** CI/CD cron job for daily data synchronization and automated repository commits.
