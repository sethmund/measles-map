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
        df['Confirmed'] = pd.to_numeric(df['Confirmed'].astype(str).str.extract(r'(\d+)', expand=False), errors='coerce').fillna(0).astype(int)

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

# ---------------------------------------------------------------------------
# MEXICO: SSA / DGE open data, Enfermedades Febriles Exantemáticas (EFE)
# Landing page lists one PDF per year; each PDF links the weekly ZIP snapshots:
#   https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/efe/historicos/YYYY/datos_abiertos_efe_DDMMYY.zip
# Each ZIP holds one case-level CSV. Dictionary (20201123_Descriptores/Catálogos_EFEs):
#   DIAGNOSTICO: 0 sin diagnóstico, 1 SARAMPIÓN, 2 rubéola, 3 descartado
#   ENTIDAD_RES: INEGI state code (01-32) of residence; DEFUNCION: 1 = sí
# ---------------------------------------------------------------------------
MX_LANDING_URL = "https://www.gob.mx/salud/documentos/datos-abiertos-bases-historicas-de-enfermedades-febriles-exantematicas?idiom=es"
MX_ZIP_BASE = "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/efe/historicos"
MX_REPORT_YEAR = 2026
MX_HEADERS = {"User-Agent": "Mozilla/5.0 (measles-map ETL; +https://github.com/sethmund/measles-map)"}
MX_ZIP_RE = re.compile(r"https?://[^\s\"'<>()]+?datos_abiertos_efe_(\d{6})\.zip", re.I)

# Keyed by INEGI CLAVE_ENTIDAD. Names must match mx.json feature names exactly
# (index.html joins on Province_State), hence 'Ciudad de Mexico' / 'Estado de México'.
MEXICO_META = {
    '01': ('Aguascalientes', 'MX-AGU', 21.8853, -102.2916),
    '02': ('Baja California', 'MX-BCN', 30.8406, -115.2838),
    '03': ('Baja California Sur', 'MX-BCS', 26.0444, -111.6661),
    '04': ('Campeche', 'MX-CAM', 19.8301, -90.5349),
    '05': ('Coahuila', 'MX-COA', 27.0587, -101.7068),
    '06': ('Colima', 'MX-COL', 19.1223, -104.0028),
    '07': ('Chiapas', 'MX-CHP', 16.7569, -93.1292),
    '08': ('Chihuahua', 'MX-CHH', 28.6330, -106.0691),
    '09': ('Ciudad de Mexico', 'MX-CMX', 19.4326, -99.1332),
    '10': ('Durango', 'MX-DUR', 24.0277, -104.6532),
    '11': ('Guanajuato', 'MX-GUA', 21.0190, -101.2574),
    '12': ('Guerrero', 'MX-GRO', 17.5809, -99.8237),
    '13': ('Hidalgo', 'MX-HID', 20.0911, -98.7624),
    '14': ('Jalisco', 'MX-JAL', 20.6595, -103.3490),
    '15': ('Estado de México', 'MX-MEX', 19.3202, -99.5694),
    '16': ('Michoacán', 'MX-MIC', 19.5665, -101.7068),
    '17': ('Morelos', 'MX-MOR', 18.6813, -99.1013),
    '18': ('Nayarit', 'MX-NAY', 21.7514, -104.8455),
    '19': ('Nuevo León', 'MX-NLE', 25.5922, -99.9962),
    '20': ('Oaxaca', 'MX-OAX', 17.0732, -96.7266),
    '21': ('Puebla', 'MX-PUE', 19.0414, -98.2063),
    '22': ('Querétaro', 'MX-QUE', 20.5888, -100.3899),
    '23': ('Quintana Roo', 'MX-ROO', 19.1817, -88.4791),
    '24': ('San Luis Potosí', 'MX-SLP', 22.1565, -100.9855),
    '25': ('Sinaloa', 'MX-SIN', 25.1721, -107.4795),
    '26': ('Sonora', 'MX-SON', 29.2972, -110.3309),
    '27': ('Tabasco', 'MX-TAB', 17.8409, -92.6180),
    '28': ('Tamaulipas', 'MX-TAM', 23.7369, -99.1411),
    '29': ('Tlaxcala', 'MX-TLA', 19.3182, -98.2375),
    '30': ('Veracruz', 'MX-VER', 19.1738, -96.1342),
    '31': ('Yucatán', 'MX-YUC', 20.7099, -89.0943),
    '32': ('Zacatecas', 'MX-ZAC', 22.7709, -102.5832),
}


def _mx_zip_date(url):
    m = MX_ZIP_RE.search(url)
    return datetime.strptime(m.group(1), "%d%m%y") if m else datetime.min


def _find_mexico_zip_links():
    """Collect weekly ZIP URLs from the landing page and the yearly PDF it links to."""
    urls = set()
    resp = requests.get(MX_LANDING_URL, headers=MX_HEADERS, timeout=30)
    resp.raise_for_status()
    urls.update(m.group(0) for m in MX_ZIP_RE.finditer(resp.text))

    soup = BeautifulSoup(resp.content, 'html.parser')
    pdf_links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '.pdf' in href.lower() and 'efe' in href.lower():
            if href.startswith('/'):
                href = "https://www.gob.mx" + href
            year = re.search(r'(20\d{2})', href)
            pdf_links.append((int(year.group(1)) if year else 0, href))

    # Newest year's PDF first; stop as soon as one yields ZIP links
    for _, pdf_url in sorted(pdf_links, reverse=True):
        try:
            pdf_resp = requests.get(pdf_url, headers=MX_HEADERS, timeout=30)
            pdf_resp.raise_for_status()
            found = set(m.group(0) for m in MX_ZIP_RE.finditer(pdf_resp.content.decode('latin-1')))
            with pdfplumber.open(io.BytesIO(pdf_resp.content)) as pdf:
                for page in pdf.pages:
                    for link in (page.hyperlinks or []):
                        uri = link.get('uri') or ''
                        if MX_ZIP_RE.search(uri):
                            found.add(uri)
            if found:
                urls.update(found)
                break
        except Exception as e:
            print(f"Mexico: could not read yearly PDF {pdf_url}: {e}")
    return urls


def _probe_mexico_zip(days_back=35):
    """Fallback: walk back day by day testing the predictable ZIP filename."""
    today = datetime.now()
    for i in range(days_back):
        d = today - timedelta(days=i)
        url = f"{MX_ZIP_BASE}/{d.year}/datos_abiertos_efe_{d:%d%m%y}.zip"
        try:
            r = requests.head(url, headers=MX_HEADERS, timeout=15, allow_redirects=True)
            if r.status_code == 200:
                return url
        except requests.RequestException:
            continue
    return None


def _read_efe_zip(content):
    """Return the case-level CSV inside an EFE ZIP as an all-string DataFrame."""
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        name = next(n for n in zf.namelist() if n.lower().endswith('.csv'))
        raw = zf.read(name)
    for enc in ('utf-8-sig', 'latin-1'):
        try:
            return pd.read_csv(io.BytesIO(raw), dtype=str, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise ValueError("Unreadable CSV encoding in EFE ZIP")


def summarize_mexico_efe(df):
    """Aggregate case-level EFE records to state totals of confirmed measles."""
    df = df.copy()
    df.columns = df.columns.str.strip().str.upper()
    for col in ('DIAGNOSTICO', 'ENTIDAD_RES', 'DEFUNCION', 'FECHA_DIAGNOSTICO'):
        df[col] = df[col].astype(str).str.strip()

    measles = df[df['DIAGNOSTICO'] == '1'].copy()
    # FECHA_DIAGNOSTICO appears as DD/MM/YYYY or YYYY-MM-DD (9999-99-99 = no data)
    year = measles['FECHA_DIAGNOSTICO'].str.extract(r'((?:19|20)\d{2})', expand=False)
    measles = measles[year == str(MX_REPORT_YEAR)]
    measles['ENTIDAD_RES'] = measles['ENTIDAD_RES'].str.split('.').str[0].str.zfill(2)

    unmapped = measles[~measles['ENTIDAD_RES'].isin(MEXICO_META)]
    if len(unmapped):
        print(f"Mexico: {len(unmapped)} confirmed cases with non-state ENTIDAD_RES "
              f"{sorted(unmapped['ENTIDAD_RES'].unique())} excluded from map")

    grouped = measles.groupby('ENTIDAD_RES').agg(
        Confirmed=('DIAGNOSTICO', 'size'),
        Deaths=('DEFUNCION', lambda s: int((s == '1').sum())),
    )

    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for code, (name, iso, lat, lon) in MEXICO_META.items():
        conf = int(grouped['Confirmed'].get(code, 0))
        deaths = int(grouped['Deaths'].get(code, 0))
        rows.append({
            'Province_State': name, 'Country_Region': 'Mexico', 'Last_Update': stamp,
            'Lat': lat, 'Long_': lon, 'Confirmed': conf, 'Deaths': deaths,
            'Recovered': 0, 'Active': conf, 'Combined_Key': f"{name}, Mexico",
            'ISO3166_2': iso,
        })
    return pd.DataFrame(rows)


def fetch_mexico_data():
    """Confirmed measles by state of residence from the SSA EFE open-data ZIP."""
    try:
        zip_url = None
        try:
            links = _find_mexico_zip_links()
            if links:
                zip_url = max(links, key=_mx_zip_date)
        except Exception as e:
            print(f"Mexico: landing page discovery failed: {e}")
        if not zip_url:
            print("Mexico: no ZIP link found on landing page; probing filename pattern...")
            zip_url = _probe_mexico_zip()
        if not zip_url:
            print("Mexico Error: could not locate an EFE open-data ZIP.")
            return pd.DataFrame()

        print(f"Mexico: downloading {zip_url}")
        resp = requests.get(zip_url, headers=MX_HEADERS, timeout=120)
        resp.raise_for_status()
        df = _read_efe_zip(resp.content)
        if 'FECHA_ACTUALIZACION' in df.columns:
            print(f"Mexico: dataset FECHA_ACTUALIZACION = {df['FECHA_ACTUALIZACION'].iloc[0]}")

        out = summarize_mexico_efe(df)
        print(f"Mexico: {out['Confirmed'].sum()} confirmed {MX_REPORT_YEAR} cases, "
              f"{out['Deaths'].sum()} deaths")
        return out
    except Exception as e:
        print(f"Mexico Error: {e}")
        return pd.DataFrame()


def load_previous_rows(country):
    """Last committed rows for a country, so one failed source doesn't blank it on the map."""
    try:
        prev = pd.read_csv("measles_na_update.csv", dtype={'ISO3166_2': str})
        prev = prev[prev['Country_Region'] == country]
        if country == 'Mexico':  # carry forward with current mx.json names
            prev['Province_State'] = prev['Province_State'].replace(
                {'Ciudad de México': 'Ciudad de Mexico', 'México': 'Estado de México'})
        return prev
    except Exception:
        return pd.DataFrame()

def main():
    print("--- Starting North American Master Merge ---")
    
    # 1. Fetch International Data
    df_can = fetch_canada_data()
    df_mex = fetch_mexico_data()
    if df_can.empty:
        df_can = load_previous_rows('Canada')
        print(f"Canada fetch failed; carrying forward {len(df_can)} previous rows.")
    if df_mex.empty:
        df_mex = load_previous_rows('Mexico')
        print(f"Mexico fetch failed; carrying forward {len(df_mex)} previous rows.")
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
