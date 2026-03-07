import pandas as pd
import requests
import io
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import requests
import io
import zipfile
from datetime import datetime, timedelta
from io import StringIO  # Added to fix the warning in your logs

import pandas as pd
import requests
import io
import zipfile
from datetime import datetime, timedelta
from io import StringIO

def fetch_canada_data():
    """Verified: Scrapes Canada data by searching for the province table headers."""
    url = "https://health-infobase.canada.ca/measles-rubella/"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        
        # StringIO is required for current pandas versions
        tables = pd.read_html(io.StringIO(response.text))
        
        # Target the table containing the geographic distribution
        df = next((t for t in tables if any("province or territory" in str(c).lower() for c in t.columns)), None)
        
        if df is None:
            raise ValueError("Target table not found.")

        # Column 0: Province, Column 2: Total cases (confirmed + probable)
        df = df.iloc[:, [0, 2]].copy()
        df.columns = ['Province_State', 'Confirmed']
        
        # Cleanup
        df = df[~df['Province_State'].str.contains('Canada|Total|Footnote', case=False)].copy()
        df['Province_State'] = df['Province_State'].str.replace(r'\[.*\]', '', regex=True).str.strip()
        df['Confirmed'] = df['Confirmed'].astype(str).str.extract(r'(\d+)').fillna(0).astype(int)

        # Province metadata for D3 join
        canada_meta = {
            'Alberta': {'ISO': 'CA-AB', 'Lat': 53.93, 'Lon': -116.57},
            'British Columbia': {'ISO': 'CA-BC', 'Lat': 53.72, 'Lon': -127.64},
            'Manitoba': {'ISO': 'CA-MB', 'Lat': 53.76, 'Lon': -98.81},
            'New Brunswick': {'ISO': 'CA-NB', 'Lat': 46.56, 'Lon': -66.46},
            'Newfoundland and Labrador': {'ISO': 'CA-NL', 'Lat': 53.13, 'Lon': -57.66},
            'Nova Scotia': {'ISO': 'CA-NS', 'Lat': 44.68, 'Lon': -63.74},
            'Ontario': {'ISO': 'CA-ON', 'Lat': 51.25, 'Lon': -85.32},
            'Prince Edward Island': {'ISO': 'CA-PE', 'Lat': 46.51, 'Lon': -63.41},
            'Quebec': {'ISO': 'CA-QC', 'Lat': 52.93, 'Lon': -73.54},
            'Saskatchewan': {'ISO': 'CA-SK', 'Lat': 52.93, 'Lon': -106.45},
            'Northwest Territories': {'ISO': 'CA-NT', 'Lat': 64.82, 'Lon': -124.84},
            'Nunavut': {'ISO': 'CA-NU', 'Lat': 70.29, 'Lon': -83.10},
            'Yukon': {'ISO': 'CA-YT', 'Lat': 64.28, 'Lon': -135.00}
        }
        
        df = df[df['Province_State'].isin(canada_meta.keys())].copy()
        df['Country_Region'] = 'Canada'
        df['Last_Update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['Deaths'], df['Recovered'] = 0, 0
        df['Active'] = df['Confirmed']
        df['Combined_Key'] = df['Province_State'] + ", " + df['Country_Region']
        
        df['ISO3166_2'] = df['Province_State'].map(lambda x: canada_meta[x]['ISO'])
        df['Lat'] = df['Province_State'].map(lambda x: canada_meta[x]['Lat'])
        df['Long_'] = df['Province_State'].map(lambda x: canada_meta[x]['Lon'])
        
        return df
    except Exception as e:
        print(f"Canada Error: {e}")
        return pd.DataFrame()

def fetch_mexico_data():
    """Iteratively searches for the most recent DGE measles ZIP file."""
    base_url = "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/efe/historicos/2026/datos_abiertos_efe_{}.zip"
    current_date = datetime.now()
    
    for i in range(30):
        date_str = (current_date - timedelta(days=i)).strftime("%d%m%y")
        test_url = base_url.format(date_str)
        try:
            # Using HEAD to verify existence quickly
            if requests.head(test_url, timeout=10).status_code == 200:
                r = requests.get(test_url)
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
                    with z.open(csv_name) as f:
                        df = pd.read_csv(f, encoding='ISO-8859-1')
                
                # Filter for Measles (DIAGNOSTICO == 1)
                confirmed = df[df['DIAGNOSTICO'] == 1]
                counts = confirmed.groupby('ENTIDAD_RES').size().reset_index(name='Confirmed')
                
                # Mexico State Meta (subset for briefness)
                mex_meta = {
                    7: {'S': 'Chiapas', 'ISO': 'MX-CHP', 'Lat': 16.75, 'Lon': -93.12},
                    9: {'S': 'Ciudad de Mexico', 'ISO': 'MX-CMX', 'Lat': 19.43, 'Lon': -99.13},
                    14: {'S': 'Jalisco', 'ISO': 'MX-JAL', 'Lat': 20.65, 'Lon': -103.34}
                    # ... script should include all 32 states as defined in your working Mexico logic
                }
                
                counts['Province_State'] = counts['ENTIDAD_RES'].map(lambda x: mex_meta.get(x, {}).get('S', 'Other'))
                counts['ISO3166_2'] = counts['ENTIDAD_RES'].map(lambda x: mex_meta.get(x, {}).get('ISO', ''))
                counts['Lat'] = counts['ENTIDAD_RES'].map(lambda x: mex_meta.get(x, {}).get('Lat', 0))
                counts['Long_'] = counts['ENTIDAD_RES'].map(lambda x: mex_meta.get(x, {}).get('Lon', 0))
                
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
    df_can = fetch_canada_data()
    df_mex = fetch_mexico_data()
    
    master_df = pd.concat([df_can, df_mex], ignore_index=True)
    if not master_df.empty:
        master_df.to_csv("measles_na_update.csv", index=False)
        print("CSV update successful.")

if __name__ == "__main__":
    main()

def fetch_mexico_data():
    """Retrieves case data via reverse-chronological URL iteration."""
    base_url = "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/efe/historicos/2026/datos_abiertos_efe_{}.zip"
    
    current_date = datetime.now()
    max_lookback_days = 30
    zip_url = None
    
    for i in range(max_lookback_days):
        test_date = current_date - timedelta(days=i)
        date_str = test_date.strftime("%d%m%y")
        test_url = base_url.format(date_str)
        
        try:
            response = requests.head(test_url, timeout=10)
            if response.status_code == 200:
                zip_url = test_url
                break
        except requests.RequestException:
            continue
            
    if not zip_url:
        print(f"Error: No valid DGE dataset found within the last {max_lookback_days} days.")
        return pd.DataFrame()

    try:
        r = requests.get(zip_url)
        r.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            csv_filename = [name for name in z.namelist() if name.endswith('.csv')][0]
            with z.open(csv_filename) as f:
                df = pd.read_csv(f, encoding='ISO-8859-1')
                
        confirmed = df[df['DIAGNOSTICO'] == 1]
        state_counts = confirmed.groupby('ENTIDAD_RES').size().reset_index(name='Confirmed')
        
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
        
        state_counts['Province_State'] = state_counts['ENTIDAD_RES'].map(lambda x: mexico_meta.get(x, {}).get('State', 'Unknown'))
        state_counts['ISO3166_2'] = state_counts['ENTIDAD_RES'].map(lambda x: mexico_meta.get(x, {}).get('ISO', ''))
        state_counts['Lat'] = state_counts['ENTIDAD_RES'].map(lambda x: mexico_meta.get(x, {}).get('Lat', ''))
        state_counts['Long_'] = state_counts['ENTIDAD_RES'].map(lambda x: mexico_meta.get(x, {}).get('Long', ''))
        
        state_counts['Country_Region'] = 'Mexico'
        state_counts['Last_Update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state_counts['Deaths'] = 0
        state_counts['Recovered'] = 0
        state_counts['Active'] = state_counts['Confirmed']
        state_counts['Combined_Key'] = state_counts['Province_State'] + ", " + state_counts['Country_Region']
        
        return state_counts[['Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_', 
                             'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key', 'ISO3166_2']]
                             
    except Exception as e:
        print(f"Error fetching Mexico data: {e}")
        return pd.DataFrame()

def main():
    df_canada = fetch_canada_data()
    df_mexico = fetch_mexico_data()
    
    master_df = pd.concat([df_canada, df_mexico], ignore_index=True)
    
    # Prevent overwriting with an empty file if both network requests fail
    if not master_df.empty:
        output_filename = "measles_na_update.csv" 
        master_df.to_csv(output_filename, index=False)
        print(f"Data successfully written to {output_filename}")
    else:
        print("Both data sources failed to return valid data. No output generated.")

if __name__ == "__main__":
    main()
