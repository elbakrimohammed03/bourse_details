import requests
import pandas as pd
import re
import time
import os
from datetime import datetime
from bs4 import BeautifulSoup
from supabase import create_client

# ============================================
# CONFIGURATION SUPABASE
# ============================================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}
base_url = "https://www.casablancabourse.com"

def split_two_numbers(raw):
    raw = str(raw).strip()
    if ' ' in raw:
        nums = re.findall(r'[\d ]+\.\d+', raw)
        if len(nums) >= 2:
            return [float(nums[0].replace(' ', '')), float(nums[1].replace(' ', ''))]
    candidates = []
    for pos in range(2, len(raw) - 1):
        left, right = raw[:pos], raw[pos:]
        try:
            if '.' in left and '.' in right:
                candidates.append((float(left), float(right), len(left.split('.')[1]), len(right.split('.')[1])))
        except: continue
    if candidates:
        for n1, n2, d1, d2 in candidates:
            if d1 == 2 and d2 == 2: return [n1, n2]
        best = min(candidates, key=lambda x: abs(x[2] - x[3]))
        return [best[0], best[1]]
    try: return [float(raw), None]
    except: return [None, None]

# --- ÉTAPE 1 : COLLECTE DES COURS ---
resp = requests.get(base_url, headers=headers, timeout=15)
soup = BeautifulSoup(resp.text, 'lxml')

all_tickers = {}
for a in soup.find_all('a', href=True):
    if '/action/capitalisation' in a['href']:
        ticker = a['href'].split('/')[1]
        name = a.get_text(strip=True)
        if name and ticker: all_tickers[name] = ticker

rows_base = []
for row in soup.find_all('tr'):
    cols = row.find_all('td')
    if len(cols) == 9:
        rows_base.append({
            'entreprise': cols[1].get_text(strip=True),
            'volume_actions': float(cols[3].get_text(strip=True).replace(',','').strip() or 0),
            'prix_mad': float(cols[6].get_text(strip=True).replace('DH','').replace(',','').strip() or 0),
            'variation_pct': float(cols[7].get_text(strip=True).replace('%','').replace('↓','').replace('↑','').strip() or 0)
        })

# --- ÉTAPE 2 : SCRAPING PROFOND ---
all_details = []
for name, ticker in all_tickers.items():
    try:
        url = f"{base_url}/{ticker}/action/capitalisation"
        r = requests.get(url, headers=headers, timeout=15)
        text = BeautifulSoup(r.text, 'lxml').get_text()
        
        data = {'entreprise': name, 'ticker': ticker}
        sec = re.search(r'(\w[\w\s]+?)\s*\n\s*Secteur', text)
        data['secteur'] = sec.group(1).replace('actions','').strip() if sec else None

        pe = re.search(r'P/E Ratio\s*([\d.]+)', text)
        data['pe_actuel'] = float(pe.group(1)) if pe else None
        div = re.search(r'Rendement Dividende\s*([\d.]+)%', text)
        data['div_yield_pct'] = float(div.group(1)) if div else None

        pluri = re.search(r'Évolution Pluriannuelle(.+?)Capitalisation Boursière', text, re.DOTALL)
        if pluri:
            sec_text = pluri.group(1)
            mapping_keys = {'PE': 'pe', 'BPA': 'bpa', 'DPA': 'dpa', 'RN': 'rn'}
            patterns = {
                'PE': r'Price earning ratio \(x\)[↑↓]?\s*(.+?)(?:\n)',
                'BPA': r'Bénéfice par action \(DH\)[↑↓]?\s*(.+?)(?:\n)',
                'DPA': r'Dividende par action \(DH\)[↑↓]?\s*(.+?)(?:\n)',
                'RN': r'Résultat net \(MDH\)[↑↓]?\s*(.+?)(?:\n)'
            }
            for key, pat in patterns.items():
                match = re.search(pat, sec_text)
                if match:
                    vals = split_two_numbers(match.group(1))
                    data[f'{mapping_keys[key]}_2023'], data[f'{mapping_keys[key]}_2022'] = vals[0], vals[1]

        all_details.append(data)
        time.sleep(0.3)
    except: continue

# --- ÉTAPE 3 : MERGE ET INSERTION SUPABASE ---
df_base = pd.DataFrame(rows_base)
df_details = pd.DataFrame(all_details)
df_final = df_base.merge(df_details, on='entreprise', how='left')
df_final['date_collecte'] = datetime.now().strftime("%Y-%m-%d")

# Conversion en dictionnaire pour Supabase
records = df_final.to_dict(orient='records')

try:
    supabase.table("bourse_details").insert(records).execute()
    print(f"✅ {len(records)} lignes insérées dans Supabase.")
except Exception as e:
    print(f"❌ Erreur Supabase : {e}")
