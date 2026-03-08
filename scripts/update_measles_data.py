import pandas as pd
import requests
import io
import zipfile
import json
import re
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
import os
import pdfplumber
from bs4 import BeautifulSoup

def fetch_us_data():
    """Fallback aggregator if jhu_us_summary.csv is missing."""
    jhu_url = "https://raw.githubusercontent.com/CSSEGISandData/measles_data/main/measles_county_all_updates.csv"
    try:
        df = pd.read_csv(jhu_url, dtype={'location_id': str})
        df['date'] = df['date'].astype(str).str.strip()
        df_2026 = df[df['date'].str.contains('^2026', regex=True)].copy()
        df_2026 = df_2026[df_2026['outcome_type'] == 'case_lab-confirmed']
        
        if df_2026.empty: return pd.DataFrame()

        us_agg = df_2026.groupby(['location_id', 'location_name']).agg({'value': 'sum'}).reset_index()
        us_agg = us_agg.rename(columns={'location_id': 'ISO3166_2', 'location_name': 'Combined_Key', 'value': 'Confirmed'})
        us_agg['ISO3166_2'] = us_agg['ISO3166_2'].str.split('.').str[0].str.zfill(5)
        us_agg['Province_State'] = us_agg['Combined_Key'].str.split(',').str[-1].str.strip()
        us_agg['Country_Region'] = 'US'
        us_agg['Last_Update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for col in ['Deaths', 'Recovered', 'Active']: us_agg[col] = 0
        return us_agg
    except Exception as e:
        print(f"US Fallback Error: {e}")
        return pd.DataFrame()

def fetch_canada_data():
    """Uses Playwright to render the dynamic PHAC table."""
    url = "https://health-infobase.canada.ca/measles-rubella/"
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="networkidle")
            
            # Wait specifically for the table ID seen in your inspection
            page.wait_for_selector("table#geoTable")
            html_content = page.content()
            browser.close()
            
        # Parse the rendered HTML
        tables = pd.read_html(io.StringIO(html_content), attrs={"id": "geoTable"})
        df = tables[0]
        
        # Column 0: Province, Column 2: Total cases 2026
        df = df.iloc[:, [0, 2]]
        df.columns = ['Province_State', 'Confirmed']
        
        # Cleanup: Remove 'Canada' summary and footnotes
        df = df[~df['Province_State'].str.contains('Canada', case=False)].copy()
        df['Province_State'] = df['Province_State'].str.replace(r'\[.*?\]|\d+', '', regex=True).str.strip()
        df['Confirmed'] = pd.to_numeric(df['Confirmed'].astype(str).str.extract('(\d+)', expand=False), errors='coerce').fillna(0).astype(int)

        canada_meta = {
            'Alberta': {'ISO': 'CA-AB', 'Lat': 53.9333, 'Long': -116.5765},
            'British Columbia': {'ISO': 'CA-BC', 'Lat': 53.7267, 'Long': -127.6476},
            'Manitoba': {'ISO': 'CA-MB', 'Lat': 53.7609, 'Long': -98.8139},
            'New Brunswick': {'ISO': 'CA-NB', 'Lat': 46.5653, 'Long': -66.4619},
            'Newfoundland and Labrador': {'ISO': 'CA-NL', 'Lat': 53.1355, 'Long': -57.6604},
            'Nova Scotia': {'ISO': 'CA-NS', 'Lat': 44.6820, 'Long': -63.7443},
            'Ontario': {'ISO': 'CA-ON', 'Lat': 51.2538, 'Long': -85.3232},
            'Prince Edward Island': {'ISO': 'CA-PE', 'Lat': 46.5107, 'Long': -63.4168},
            'Quebec': {'ISO': 'CA-QC', 'Lat': 52.9399, 'Long': -73.5491},
            'Saskatchewan': {'ISO': 'CA-SK', 'Lat': 52.9399, 'Long': -106.4509},
            'Northwest Territories': {'ISO': 'CA-NT', 'Lat': 64.8255, 'Long': -124.8457},
            'Nunavut': {'ISO': 'CA-NU', 'Lat': 70.2998, 'Long': -83.1076},
            'Yukon': {'ISO': 'CA-YT', 'Lat': 64.2823, 'Long': -135.0000}
        }

        df = df[df['Province_State'].isin(canada_meta.keys())].copy()
        df['Country_Region'] = 'Canada'
        df['Last_Update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['Deaths'], df['Recovered'] = 0, 0
        df['Active'] = df['Confirmed']
        df['Combined_Key'] = df['Province_State'] + ", Canada"
        df['ISO3166_2'] = df['Province_State'].map(lambda x: canada_meta[x]['ISO'])
        df['Lat'] = df['Province_State'].map(lambda x: canada_meta[x]['Lat'])
        df['Long_'] = df['Province_State'].map(lambda x: canada_meta[x]['Long'])

        return df[['Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_', 
                   'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key', 'ISO3166_2']]
    except Exception as e:
        print(f"Canada Playwright Error: {e}")
        return pd.DataFrame()

def fetch_mexico_data():
    """
    Retrieves confirmed measles case data by dynamically scraping and parsing 
    Table 1 from the official Mexican government epidemiological PDF report.
    """
    landing_url = "https://www.gob.mx/salud/documentos/informe-diario-del-brote-de-sarampion-en-mexico-2026"
    
    # 1. Scrape the landing page for the current PDF link
    try:
        resp = requests.get(landing_url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        pdf_tag = soup.find('a', href=lambda href: href and href.endswith('.pdf'))
        if not pdf_tag:
            print("Error: No PDF link identified on the landing page.")
            return pd.DataFrame()
            
        pdf_url = pdf_tag['href']
        if pdf_url.startswith('/'):
            pdf_url = "https://www.gob.mx" + pdf_url
            
    except requests.RequestException as e:
        print(f"Network error during link extraction: {e}")
        return pd.DataFrame()

    # 2. Download and extract the PDF Table in-memory
    try:
        pdf_resp = requests.get(pdf_url, timeout=15)
        pdf_resp.raise_for_status()
        
        raw_data = []
        with pdfplumber.open(io.BytesIO(pdf_resp.content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text and "Situación actual de sarampión en México" in text:
                    table = page.extract_table()
                    if table:
                        raw_data = table
                        break
                        
        if not raw_data:
            print("Error: Target epidemiological table not found in the parsed PDF.")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"PDF stream parsing error: {e}")
        return pd.DataFrame()

# 3. Clean and format the extracted Dataframe via positional indexing
    df = pd.DataFrame(raw_data[2:])
    
    # Isolate State (index 0) and Confirmed 2026 (index 7)
    df = df.rename(columns={0: 'Estado', 7: 'Confirmados_2026'})
    df = df[['Estado', 'Confirmados_2026']].copy()
    
    # Drop empty rows and aggregate totals
    df = df[df['Estado'].notna()]
    df = df[~df['Estado'].astype(str).str.contains("Total|Estado", case=False, na=False)]
    df['Estado'] = df['Estado'].astype(str).str.replace('\n', ' ').str.strip()
    
    # Coerce OCR numeric artifacts
    df['Confirmados_2026'] = pd.to_numeric(
        df['Confirmados_2026'].astype(str).str.replace(',', '').str.strip(), 
        errors='coerce'
    ).fillna(0)

    # 4. Map spatial metadata matching the PDF's specific diacritics
    mexico_meta = {
        'Aguascalientes': {'ISO': 'MX-AGU', 'Lat': 21.8853, 'Long': -102.2916},
        'Baja California': {'ISO': 'MX-BCN', 'Lat': 30.8406, 'Long': -115.2838},
        'Baja California Sur': {'ISO': 'MX-BCS', 'Lat': 26.0444, 'Long': -111.6661},
        'Campeche': {'ISO': 'MX-CAM', 'Lat': 19.8301, 'Long': -90.5349},
        'Coahuila': {'ISO': 'MX-COA', 'Lat': 27.0587, 'Long': -101.7068},
        'Colima': {'ISO': 'MX-COL', 'Lat': 19.1223, 'Long': -104.0028},
        'Chiapas': {'ISO': 'MX-CHP', 'Lat': 16.7569, 'Long': -93.1292},
        'Chihuahua': {'ISO': 'MX-CHH', 'Lat': 28.6330, 'Long': -106.0691},
        'Ciudad de México': {'ISO': 'MX-CMX', 'Lat': 19.4326, 'Long': -99.1332},
        'Durango': {'ISO': 'MX-DUR', 'Lat': 24.0277, 'Long': -104.6532},
        'Guanajuato': {'ISO': 'MX-GUA', 'Lat': 21.0190, 'Long': -101.2574},
        'Guerrero': {'ISO': 'MX-GRO', 'Lat': 17.5809, 'Long': -99.8237},
        'Hidalgo': {'ISO': 'MX-HID', 'Lat': 20.0911, 'Long': -98.7624},
        'Jalisco': {'ISO': 'MX-JAL', 'Lat': 20.6595, 'Long': -103.3490},
        'México': {'ISO': 'MX-MEX', 'Lat': 19.3202, 'Long': -99.5694},
        'Michoacán': {'ISO': 'MX-MIC', 'Lat': 19.5665, 'Long': -101.7068},
        'Morelos': {'ISO': 'MX-MOR', 'Lat': 18.6813, 'Long': -99.1013},
        'Nayarit': {'ISO': 'MX-NAY', 'Lat': 21.7514, 'Long': -104.8455},
        'Nuevo León': {'ISO': 'MX-NLE', 'Lat': 25.5922, 'Long': -99.9962},
        'Oaxaca': {'ISO': 'MX-OAX', 'Lat': 17.0732, 'Long': -96.7266},
        'Puebla': {'ISO': 'MX-PUE', 'Lat': 19.0414, 'Long': -98.2063},
        'Querétaro': {'ISO': 'MX-QUE', 'Lat': 20.5888, 'Long': -100.3899},
        'Quintana Roo': {'ISO': 'MX-ROO', 'Lat': 19.1817, 'Long': -88.4791},
        'San Luis Potosí': {'ISO': 'MX-SLP', 'Lat': 22.1565, 'Long': -100.9855},
        'Sinaloa': {'ISO': 'MX-SIN', 'Lat': 25.1721, 'Long': -107.4795},
        'Sonora': {'ISO': 'MX-SON', 'Lat': 29.2972, 'Long': -110.3309},
        'Tabasco': {'ISO': 'MX-TAB', 'Lat': 17.8409, 'Long': -92.6180},
        'Tamaulipas': {'ISO': 'MX-TAM', 'Lat': 23.7369, 'Long': -99.1411},
        'Tlaxcala': {'ISO': 'MX-TLA', 'Lat': 19.3182, 'Long': -98.2375},
        'Veracruz': {'ISO': 'MX-VER', 'Lat': 19.1738, 'Long': -96.1342},
        'Yucatán': {'ISO': 'MX-YUC', 'Lat': 20.7099, 'Long': -89.0943},
        'Zacatecas': {'ISO': 'MX-ZAC', 'Lat': 22.7709, 'Long': -102.5832}
    }
    
    out_df = pd.DataFrame()
    out_df['Province_State'] = df['Estado']
    out_df['Country_Region'] = 'Mexico'
    out_df['Last_Update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out_df['Lat'] = df['Estado'].map(lambda x: mexico_meta.get(x, {}).get('Lat', 0.0))
    out_df['Long_'] = df['Estado'].map(lambda x: mexico_meta.get(x, {}).get('Long', 0.0))
    
    # Assign the correct 2026 column
    out_df['Confirmed'] = df['Confirmados_2026'] 
    
    out_df['Deaths'] = 0
    out_df['Recovered'] = 0
    out_df['Active'] = df['Confirmados_2026']
    # Normalize strings for the Combined_Key
    out_df['Combined_Key'] = df['Estado'].str.replace('á', 'a').str.replace('é', 'e').str.replace('í', 'i').str.replace('ó', 'o') + ", Mexico"
    out_df['ISO3166_2'] = df['Estado'].map(lambda x: mexico_meta.get(x, {}).get('ISO', ''))

    return out_df

# To verify output sum:
# print(fetch_mexico_data()['Confirmed'].sum())

def main():
    print("--- Starting North American Master Merge ---")
    
    # 1. Fetch International Data
    df_can = fetch_canada_data()
    df_mex = fetch_mexico_data()
    print(f"Collected Canada: {len(df_can)} rows | Mexico: {len(df_mex)} rows")
    
    # 2. Load Local JHU Summary with persistence check
    if os.path.exists("jhu_us_summary.csv"):
        df_usa = pd.read_csv("jhu_us_summary.csv", dtype={'ISO3166_2': str})
        print(f"Merge Sync: Successfully loaded {len(df_usa)} US rows from local file.")
    else:
        print("Warning: jhu_us_summary.csv not found in workspace. Running Fallback fetcher...")
        df_usa = fetch_us_data()

    # 3. Defensive Concatenation
    # Ensures columns align even if one source has extra metadata
    master_df = pd.concat([df_can, df_mex, df_usa], ignore_index=True, sort=False)
    
    if not master_df.empty:
        # Standardize FIPS/ISO strings for D3 mapping
        if 'ISO3166_2' in master_df.columns:
            master_df['ISO3166_2'] = master_df['ISO3166_2'].astype(str).str.split('.').str[0].str.zfill(5)
            
        master_df.to_csv("measles_na_update.csv", index=False)
        print(f"Success: Master file contains {len(master_df)} total records.")
    else:
        print("Fatal Error: All data sources returned empty sets.")

if __name__ == "__main__":
    main()
