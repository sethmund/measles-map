import pandas as pd
from datetime import datetime

def process_jhu_us_data():
    """Aggregates raw JHU daily line lists into a mapped YTD summary."""
    jhu_url = "https://raw.githubusercontent.com/CSSEGISandData/measles_data/main/measles_county_all_updates.csv"
    
    try:
        # Force location_id to string to prevent loss of leading zeros (e.g., 08001)
        df = pd.read_csv(jhu_url, dtype={'location_id': str})
        
        # 1. Strict 2026 Filter
        df['date'] = df['date'].astype(str).str.strip()
        df_2026 = df[df['date'].str.contains('^2026', regex=True)].copy()
        
        # 2. Filter for Lab-Confirmed cases specifically
        df_2026 = df_2026[df_2026['outcome_type'] == 'case_lab-confirmed']
        
        if df_2026.empty:
            print("Warning: No US 2026 records found. Check JHU source date format.")
            return None

        # 3. Aggregate by location_id (FIPS) to get YTD totals
        us_agg = df_2026.groupby(['location_id', 'location_name']).agg({'value': 'sum'}).reset_index()
        
        # 4. Standardize Columns to match your CA/MX schema
        us_agg = us_agg.rename(columns={
            'location_id': 'ISO3166_2',
            'location_name': 'Combined_Key',
            'value': 'Confirmed'
        })
        
        # Reformat FIPS: split any trailing decimals and pad to 5 digits
        us_agg['ISO3166_2'] = us_agg['ISO3166_2'].str.split('.').str[0].str.zfill(5)
        
        # Extract Province_State (e.g., 'Colorado') from 'Adams, Colorado'
        us_agg['Province_State'] = us_agg['Combined_Key'].str.split(',').str[-1].str.strip()
        us_agg['Country_Region'] = 'US'
        us_agg['Last_Update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Add placeholder columns to maintain a consistent CSV structure
        for col in ['Deaths', 'Recovered', 'Active', 'Lat', 'Long_']:
            us_agg[col] = 0
            
        # Select final column order for the merge
        final_cols = ['Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_', 
                      'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key', 'ISO3166_2']
        
        us_summary = us_agg[final_cols]
        us_summary.to_csv("jhu_us_summary.csv", index=False)
        print(f"JHU Update Successful: {len(us_summary)} counties processed.")
        return us_summary

    except Exception as e:
        print(f"Critical error processing JHU data: {e}")
        return None

if __name__ == "__main__":
    process_jhu_us_data()
