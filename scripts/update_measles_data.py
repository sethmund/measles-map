import pandas as pd
import requests
import io
import zipfile
import json
import re
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright

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
    """Retrieves case data via reverse-chronological URL iteration."""
    base_url = "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/efe/historicos/2026/datos_abiertos_efe_{}.zip"
    current_date = datetime.now()
    
    for i in range(30):
        test_url = base_url.format((current_date - timedelta(days=i)).strftime("%d%m%y"))
        try:
            if requests.head(test_url, timeout=10).status_code == 200:
                r = requests.get(test_url)
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
                    with z.open(csv_name) as f:
                        df = pd.read_csv(f, encoding='ISO-8859-1')
                
                confirmed = df[df['DIAGNOSTICO'] == 1]
                counts = confirmed.groupby('ENTIDAD_RES').size().reset_index(name='Confirmed')
                
                mexico_meta = {
                    1: {'State': 'Aguascalientes', 'ISO': 'MX-AGU', 'Lat': 21.8853, 'Long': -102.2916},
                    2: {'State': 'Baja California', 'ISO': 'MX-BCN', 'Lat': 30.8406, 'Long': -115.2838},
                    3: {'State': 'Baja California Sur', 'ISO': 'MX-BCS', 'Lat': 26.0444, 'Long': -111.6661},
                    4: {'State': 'Campeche', 'ISO': 'MX-CAM', 'Lat': 19.8301, 'Long': -90.5349},
                    5: {'State': 'Coahuila', 'ISO': 'MX-COA', 'Lat': 27.0587, 'Long': -101.7068},
                    6: {'State': 'Colima', 'ISO': 'MX-COL', 'Lat': 19.1223, 'Long': -104.0028},
                    7: {'State': 'Chiapas', 'ISO': 'MX-CHP', 'Lat': 16.7569, 'Long': -93.1292},
                    8: {'State': 'Chihuahua', 'ISO': 'MX-CHH', 'Lat': 28.6330, 'Long': -106.0691},
                    9: {'State': 'Ciudad de Mexico', 'ISO': 'MX-CMX', 'Lat': 19.4326, 'Long': -99.1332},
                    10: {'State': 'Durango', 'ISO': 'MX-DUR', 'Lat': 24.0277, 'Long': -104.6532},
                    11: {'State': 'Guanajuato', 'ISO': 'MX-GUA', 'Lat': 21.0190, 'Long': -101.2574},
                    12: {'State': 'Guerrero', 'ISO': 'MX-GRO', 'Lat': 17.5809, 'Long': -99.8237},
                    13: {'State': 'Hidalgo', 'ISO': 'MX-HID', 'Lat': 20.0911, 'Long': -98.7624},
                    14: {'State': 'Jalisco', 'ISO': 'MX-JAL', 'Lat': 20.6595, 'Long': -103.3490},
                    15: {'State': 'Mexico', 'ISO': 'MX-MEX', 'Lat': 19.3202, 'Long': -99.5694},
                    16: {'State': 'Michoacan', 'ISO': 'MX-MIC', 'Lat': 19.5665, 'Long': -101.7068},
                    17: {'State': 'Morelos', 'ISO': 'MX-MOR', 'Lat': 18.6813, 'Long': -99.1013},
                    18: {'State': 'Nayarit', 'ISO': 'MX-NAY', 'Lat': 21.7514, 'Long': -104.8455},
                    19: {'State': 'Nuevo Leon', 'ISO': 'MX-NLE', 'Lat': 25.5922, 'Long': -99.9962},
                    20: {'State': 'Oaxaca', 'ISO': 'MX-OAX', 'Lat': 17.0732, 'Long': -96.7266},
                    21: {'State': 'Puebla', 'ISO': 'MX-PUE', 'Lat': 19.0414, 'Long': -98.2063},
                    22: {'State': 'Queretaro', 'ISO': 'MX-QUE', 'Lat': 20.5888, 'Long': -100.3899},
                    23: {'State': 'Quintana Roo', 'ISO': 'MX-ROO', 'Lat': 19.1817, 'Long': -88.4791},
                    24: {'State': 'San Luis Potosi', 'ISO': 'MX-SLP', 'Lat': 22.1565, 'Long': -100.9855},
                    25: {'State': 'Sinaloa', 'ISO': 'MX-SIN', 'Lat': 25.1721, 'Long': -107.4795},
                    26: {'State': 'Sonora', 'ISO': 'MX-SON', 'Lat': 29.2972, 'Long': -110.3309},
                    27: {'State': 'Tabasco', 'ISO': 'MX-TAB', 'Lat': 17.8409, 'Long': -92.6180},
                    28: {'State': 'Tamaulipas', 'ISO': 'MX-TAM', 'Lat': 23.7369, 'Long': -99.1411},
                    29: {'State': 'Tlaxcala', 'ISO': 'MX-TLA', 'Lat': 19.3182, 'Long': -98.2375},
                    30: {'State': 'Veracruz', 'ISO': 'MX-VER', 'Lat': 19.1738, 'Long': -96.1342},
                    31: {'State': 'Yucatan', 'ISO': 'MX-YUC', 'Lat': 20.7099, 'Long': -89.0943},
                    32: {'State': 'Zacatecas', 'ISO': 'MX-ZAC', 'Lat': 22.7709, 'Long': -102.5832}
                }
                
                counts['Province_State'] = counts['ENTIDAD_RES'].map(lambda x: mexico_meta.get(x, {}).get('State', 'Unknown'))
                counts['ISO3166_2'] = counts['ENTIDAD_RES'].map(lambda x: mexico_meta.get(x, {}).get('ISO', ''))
                counts['Lat'] = counts['ENTIDAD_RES'].map(lambda x: mexico_meta.get(x, {}).get('Lat', ''))
                counts['Long_'] = counts['ENTIDAD_RES'].map(lambda x: mexico_meta.get(x, {}).get('Long', ''))
                counts['Country_Region'] = 'Mexico'
                counts['Last_Update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                counts['Deaths'], counts['Recovered'] = 0, 0
                counts['Active'] = counts['Confirmed']
                counts['Combined_Key'] = counts['Province_State'] + ", Mexico"
                
                return counts[['Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_', 
                               'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key', 'ISO3166_2']]
        except:
            continue
    return pd.DataFrame()

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
