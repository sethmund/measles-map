import pandas as pd
import requests
import io
import zipfile
from datetime import datetime, timedelta

def fetch_canada_data():
    """Scrapes weekly provincial measles data from PHAC Health Infobase."""
    url = "https://health-infobase.canada.ca/measles-rubella/"
    try:
        # Target the static column header rather than the table title
        tables = pd.read_html(url, match="Province or territory")
        df = tables[0]
        
        df = df.rename(columns={
            df.columns[0]: 'Province_State',
            df.columns[2]: 'Confirmed' 
        })
        df = df[df['Province_State'] != 'Canada'].copy()
        
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
        
        df['Country_Region'] = 'Canada'
        df['Last_Update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['Deaths'] = 0
        df['Recovered'] = 0
        df['Active'] = df['Confirmed']
        df['Combined_Key'] = df['Province_State'] + ", " + df['Country_Region']
        
        df['ISO3166_2'] = df['Province_State'].map(lambda x: canada_meta.get(x, {}).get('ISO', ''))
        df['Lat'] = df['Province_State'].map(lambda x: canada_meta.get(x, {}).get('Lat', ''))
        df['Long_'] = df['Province_State'].map(lambda x: canada_meta.get(x, {}).get('Long', ''))
        
        return df[['Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_', 
                   'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key', 'ISO3166_2']]
                   
    except Exception as e:
        print(f"Error fetching Canada data: {e}")
        return pd.DataFrame()

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
