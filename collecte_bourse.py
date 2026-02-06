import requests
import pandas as pd
import re
import time
import os
from datetime import datetime
from bs4 import BeautifulSoup
from supabase import create_client

# CONFIGURATION
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

headers = {"User-Agent": "Mozilla/5.0"}
base_url = "https://www.casablancabourse.com"

def clean_val(val):
    """Force la conversion en nombre pur ou None pour SQL."""
    try:
        if val is None or str(val).strip() in ['', '-', 'N/A']: return None
        res = re.sub(r'[^\d.-]', '', str(val).replace(',', '.'))
        return float(res)
    except:
        return None

def split_two_numbers(raw):
    raw = str(raw).strip()
    if ' ' in raw:
        nums = re.findall(r'[\d ]+\.\d+', raw)
        if len(nums) >= 2:
            return [clean_val(nums[0]), clean_val(nums[1])]
    return [clean_val(raw), None]

# --- ÉTAPE 1 : COURS ---
resp = requests.get(base_url, headers=headers, timeout=15)
soup = BeautifulSoup(resp.text, 'html.parser')

all_tickers = {}
for a in soup.find_all('a', href=True):
    if '/action/capitalisation' in a['href']:
        ticker = a['href'].split('/')[1]
        name = a.get_text(strip=True)
        all_tickers[name] = ticker

rows_base = []
for row in soup.find_all('tr'):
    cols = row.find_all('td')
    if len(cols) == 9:
        rows_base.append({
            'entreprise': cols[1].get_text(strip=True),
            'volume_actions': clean_val(cols[3].get_text(strip=True)),
            'prix_mad': clean_val(cols[6].get_text(strip=True)),
            'variation_pct': clean_val(cols[7].get_text(strip=True))
        })

# --- ÉTAPE 2 : DÉTAILS ---
all_details = []
for name, ticker in all_tickers.items():
    try:
        r = requests.get(f"{base_url}/{ticker}/action/capitalisation", headers=headers, timeout=15)
        text = BeautifulSoup(r.text, 'html.parser').get_text()
        
        data = {'entreprise': name, 'ticker': ticker}
        
        # Secteur
        sec = re.search(r'(\w[\w\s]+?)\s*\n\s*Secteur', text)
        data['secteur'] = sec.group(1).replace('actions','').strip() if sec else None

        # Ratios
        data['pe_actuel'] = clean_val(re.search(r'P/E Ratio\s*([\d.,]+)', text).group(1)) if re.search(r'P/E Ratio\s*([\d.,]+)', text) else None
        data['div_yield_pct'] = clean_val(re.search(r'Rendement Dividende\s*([\d.,]+)%', text).group(1)) if re.search(r'Rendement Dividende\s*([\d.,]+)%', text) else None

        # Pluriannuel
        pluri = re.search(r'Évolution Pluriannuelle(.+?)Capitalisation Boursière', text, re.DOTALL)
        if pluri:
            st = pluri.group(1)
            pats = {'pe': r'Price earning ratio \(x\).*?\s+([\d.\s]+)', 'bpa': r'Bénéfice par action \(DH\).*?\s+([\d.\s]+)', 
                    'dpa': r'Dividende par action \(DH\).*?\s+([\d.\s]+)', 'rn': r'Résultat net \(MDH\).*?\s+([\d.\s]+)'}
            for k, p in pats.items():
                m = re.search(p, st)
                if m:
                    v = split_two_numbers(m.group(1))
                    data[f'{k}_2023'], data[f'{k}_2022'] = v[0], v[1]
        
        all_details.append(data)
        time.sleep(0.2)
    except: continue

# --- ÉTAPE 3 : MERGE & INSERT ---
df_final = pd.DataFrame(rows_base).merge(pd.DataFrame(all_details), on='entreprise', how='left')
df_final['date_collecte'] = datetime.now().strftime("%Y-%m-%d")

# Nettoyage final pour Supabase : remplacer NaN par None
records = df_final.where(pd.notnull(df_final), None).to_dict(orient='records')

try:
    supabase.table("bourse_details").insert(records).execute()
    print(f"✅ Insertion réussie : {len(records)} entreprises.")
except Exception as e:
    print(f"❌ Erreur d'insertion : {e}")
