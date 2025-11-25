import json
import os
import yfinance as yf
import pandas as pd
from rapidfuzz import process, fuzz

PROC_DIR = 'data/processed'
ENRICH_DIR = 'data/enriched'
os.makedirs(ENRICH_DIR, exist_ok=True)

# Yahoo Finance Proxy Map (Still needed for prices as DUNL is ref data only)
SYMBOL_MAP = {
    'AAGZU00': 'CL=F',   # Crude
    'TS01021': 'TI=F',   # Iron Ore
    'AAIDC00': 'HO=F',   # Fuel Oil
    'AAGJA00': 'RB=F',   # Gasoline
    'TS01034': 'MTF=F',  # Coal
    'WAUSA00': 'ZW=F',   # Wheat
    'SP500': '^GSPC'     # Index
}

def clean_text(text):
    """Removes noise words to improve matching accuracy"""
    noise_words = ['FOB', 'CIF', 'CFR', 'DES', 'Port Charge', 'Disport Charge', 'Cargo', 'Blend', 'Strip', 'vs']
    text = text.replace('.', '')
    for word in noise_words:
        text = text.replace(word, '')
    return text.strip()

def resolve_logistics_links(benchmarks, ports):
    """
    Dynamically links Benchmarks to Ports using Fuzzy Matching.
    Returns a list of edge dictionaries.
    """
    print("... 🧠 Running Entity Resolution on Locations")
    links = []
    
    # Create a lookup dictionary for ports: {Clean_Name: Port_ID}
    port_lookup = {clean_text(p['name']): p['id'] for p in ports}
    port_names = list(port_lookup.keys())

    for b in benchmarks:
        # 1. Get the Benchmark Description (e.g., "Gasoil FOB Spore Cargo")
        desc_clean = clean_text(b['description'])
        
        # 2. Fuzzy Match against all Port Names
        # We use partial_token_sort_ratio to handle mixed word orders
        match = process.extractOne(
            desc_clean, 
            port_names, 
            scorer=fuzz.partial_token_sort_ratio
        )
        
        if match:
            best_match_name, score, index = match
            
            # Threshold: Only link if we are >80% confident
            if score > 80:
                port_id = port_lookup[best_match_name]
                print(f"   🔗 Linked '{b['symbol']}' <--> '{best_match_name}' (Score: {score})")
                
                links.append({
                    'from': b['id'],
                    'to': port_id,
                    'label': 'Pricing Location', # Visible label on line
                    'title': f"Match Logic: Found '{best_match_name}' in '{b['description']}'", # Tooltip logic
                    'arrows': 'to',
                    'color': {'color': '#64748b', 'opacity': 0.6},
                    'dashes': True
                })

    return links

def fetch_market_data():
    print("... 📈 Fetching Market Data")
    tickers = list(SYMBOL_MAP.values())
    try:
        data = yf.download(tickers, period="3mo", interval="1d", progress=False)['Close']
        data.index = data.index.strftime('%b %d')
        market_history = {"dates": data.index.tolist(), "datasets": {}}
        
        for dunl_id, yf_ticker in SYMBOL_MAP.items():
            if yf_ticker in data.columns:
                clean_data = data[yf_ticker].fillna(method='ffill').fillna(0).round(2).tolist()
                market_history["datasets"][dunl_id] = clean_data
        return market_history
    except Exception as e:
        print(f"Warning: Market data fetch failed ({e}). Using mock data.")
        return {"dates": [], "datasets": {}}

def build_knowledge_graph(ports, benchmarks, currencies, dynamic_links):
    print("... 🕸️  Assembling Graph")
    nodes = []
    edges = [] # Start with dynamic links
    edges.extend(dynamic_links) 
    
    existing_nodes = set()

    # Add Nodes
    for b in benchmarks:
        if b['id'] in SYMBOL_MAP:
            # Benchmark Node
            nodes.append({
                'id': b['id'],
                'label': b['symbol'],
                'group': 'benchmark',
                'title': f"<b>{b['description']}</b><br>ID: {b['id']}",
                'value': 25
            })
            existing_nodes.add(b['id'])

            # Commodity Family Node
            comm_id = b['commodity']
            if comm_id not in existing_nodes:
                nodes.append({'id': comm_id, 'label': comm_id, 'group': 'commodity', 'value': 40})
                existing_nodes.add(comm_id)
            edges.append({'from': comm_id, 'to': b['id'], 'color': '#f97316'})

            # Currency Link
            if b['currency'] not in existing_nodes:
                nodes.append({'id': b['currency'], 'label': b['currency'], 'group': 'currency', 'value': 15})
                existing_nodes.add(b['currency'])
            edges.append({'from': b['id'], 'to': b['currency'], 'color': '#10b981', 'length': 50})

    # Add Port Nodes (Only if they were linked dynamically)
    linked_port_ids = set([link['to'] for link in dynamic_links])
    
    for p in ports:
        if p['id'] in linked_port_ids:
            nodes.append({
                'id': p['id'],
                'label': p['name'].replace(' Port Charge','').replace(' Disport Charge',''),
                'group': 'port',
                'value': 20,
                'title': f"<b>{p['name']}</b><br>Region: {p['region']}"
            })

    return {"nodes": nodes, "edges": edges}

if __name__ == "__main__":
    # Load Processed Data
    with open(os.path.join(PROC_DIR, 'ports.json')) as f: ports = json.load(f)
    with open(os.path.join(PROC_DIR, 'benchmarks.json')) as f: benchmarks = json.load(f)
    with open(os.path.join(PROC_DIR, 'currencies.json')) as f: currencies = json.load(f)

    # 1. Resolve Links Dynamically
    dynamic_links = resolve_logistics_links(benchmarks, ports)
    
    # 2. Fetch Data
    market_data = fetch_market_data()
    
    # 3. Build Graph
    graph_data = build_knowledge_graph(ports, benchmarks, currencies, dynamic_links)

    final_payload = {
        "ports": ports,
        "benchmarks": [b for b in benchmarks if b['id'] in SYMBOL_MAP],
        "market_data": market_data,
        "graph": graph_data
    }

    with open(os.path.join(ENRICH_DIR, 'dashboard_payload.json'), 'w') as f:
        json.dump(final_payload, f, indent=2)
    
    print("✅ Enrichment Complete.")
