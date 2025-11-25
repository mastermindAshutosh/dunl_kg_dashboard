import pandas as pd
import os
import json

# Configuration
RAW_DIR = 'data/raw'
PROC_DIR = 'data/processed'
os.makedirs(PROC_DIR, exist_ok=True)

# Mock Coordinate Map (Since DUNL CSVs provided don't have Lat/Lon)
# In a real scenario, you would use a Geocoding API here.
PORT_COORDINATES = {
    'TAIPA00': {'lat': 5.3600, 'lng': -4.0083},   # Abidjan
    'ALEDC00': {'lat': 31.2001, 'lng': 29.9187},  # Alexandria
    'ALGDC00': {'lat': 36.1408, 'lng': -5.4562},  # Algeciras
    'TYNPA00': {'lat': 24.0891, 'lng': 38.0637},  # Yanbu
    'WILDC00': {'lat': 53.5167, 'lng': 8.1333},   # Wilhelmshaven
    'TVAPC00': {'lat': 17.6868, 'lng': 83.2185},  # Vishakhapatnam
    'ROT':     {'lat': 51.9225, 'lng': 4.4792},   # Rotterdam
    'SIN':     {'lat': 1.3521, 'lng': 103.8198},  # Singapore
    'HOU':     {'lat': 29.7604, 'lng': -95.3698}  # Houston
}

def clean_dunl_id(url_id):
    """Extracts ID from DUNL URL string"""
    if pd.isna(url_id): return ""
    return url_id.split('/')[-1]

def ingest_ports():
    print("... Ingesting Ports")
    # Load your specific CSV filename
    df = pd.read_csv(os.path.join(RAW_DIR, 'port_Port Charges Location Data.csv'))
    
    ports = []
    for _, row in df.iterrows():
        pid = clean_dunl_id(row['ID'])
        # Only keep ports we have coordinates for (for the MVP)
        if pid in PORT_COORDINATES:
            ports.append({
                'id': pid,
                'name': row.get('port', 'Unknown Port'),
                'region': row.get('region', 'Global'),
                'lat': PORT_COORDINATES[pid]['lat'],
                'lng': PORT_COORDINATES[pid]['lng'],
                'dunl_uri': row['ID']
            })
    
    with open(os.path.join(PROC_DIR, 'ports.json'), 'w') as f:
        json.dump(ports, f, indent=2)

def ingest_benchmarks():
    print("... Ingesting Benchmarks")
    df = pd.read_csv(os.path.join(RAW_DIR, 'symbols _Platts Benchmarks.csv'))
    
    benchmarks = []
    for _, row in df.iterrows():
        # Clean data
        benchmarks.append({
            'id': clean_dunl_id(row['ID']),
            'symbol': row.get('symbol', 'N/A'),
            'description': row.get('description', ''),
            'commodity': row.get('commodity', 'General'),
            'currency': row.get('currency', 'USD'),
            'uom': row.get('uom', 'N/A'),
            'dunl_uri': row['ID']
        })
    
    with open(os.path.join(PROC_DIR, 'benchmarks.json'), 'w') as f:
        json.dump(benchmarks, f, indent=2)

def ingest_currencies():
    print("... Ingesting Currencies")
    df = pd.read_csv(os.path.join(RAW_DIR, 'currency.csv'))
    
    currencies = []
    for _, row in df.iterrows():
        currencies.append({
            'code': row.get('currencyCode', 'N/A'),
            'label': row.get('currencyLabel', ''),
            'dunl_uri': row['ID']
        })
        
    with open(os.path.join(PROC_DIR, 'currencies.json'), 'w') as f:
        json.dump(currencies, f, indent=2)

if __name__ == "__main__":
    # Ensure you have put the CSV files in data/raw/ before running
    try:
        ingest_ports()
        ingest_benchmarks()
        ingest_currencies()
        print("✅ Ingestion Complete. Data saved to data/processed/")
    except FileNotFoundError as e:
        print(f"❌ Error: {e}. Please ensure CSV files are in data/raw/")
