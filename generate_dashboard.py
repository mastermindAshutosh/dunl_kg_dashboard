import json
import os
from jinja2 import Environment, FileSystemLoader

PROC_DIR = 'data/processed'
TEMPLATE_DIR = 'templates'
OUTPUT_FILE = 'index.html'

def generate():
    print("🎨 Step 3: Rendering Dashboard...")
    
    with open(os.path.join(PROC_DIR, 'dashboard_data.json')) as f:
        payload = json.load(f)
        
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    template = env.get_template('dashboard.html')
    
    html = template.render(payload=payload)
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"✅ Dashboard Ready: {os.path.abspath(OUTPUT_FILE)}")

if __name__ == "__main__":
    generate()
