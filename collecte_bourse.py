import requests
import pandas as pd
import re
import time
import os
from datetime import datetime
from bs4 import BeautifulSoup
from supabase import create_client

# ============================================
# CONFIGURATION SUPABASE AVEC CLES INTEGREES
# ============================================
SUPABASE_URL = "https://nbgpxasdgucltfcygqua.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5iZ3B4YXNkZ3VjbHRmY3lncXVhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAzMzc1NDAsImV4cCI6MjA4NTkxMzU0MH0.EpLaGobOZxa_VI-_cOBXoDBiB7J-5QaC9vNV4lyNNKc"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}
base_url = "https://www.casablancabourse.com"

def clean_val(val):
    """Force la conversion en nombre pur ou None pour PostgreSQL."""
    try:
        if val is None or str(val).strip() in ['', '-', 'N/A']: 
            return None
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

# --- ETAPE 1 : COLLECTE DES COURS ---
print("Demarrage de la collecte des cours...")
resp = requests.get(base_url, headers=headers, timeout=15)
soup = BeautifulSoup(resp.text, 'html.parser')

all_tickers = {}
for a in soup.find_all('a', href=True):
    if '/action/capitalisation' in a['href']:
        ticker = a['href'].split('/')[1]
        name = a.get_text(strip=True)
        if name and ticker: 
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

# --- ETAPE 2 : SCRAPING DES DONNEES FONDAMENTALES ---
print(f"Traitement de {len(all_tickers)} entreprises...")
all_details = []
for i, (name, ticker) in enumerate(all_tickers.items(), 1):
    try:
        r = requests.get(f"{base_url}/{ticker}/action/capitalisation", headers=headers, timeout=15)
        text = BeautifulSoup(r.text, 'html.parser').get_text()
        
        data = {'entreprise': name, 'ticker': ticker}
        
        # Extraction du secteur
        sec = re.search(r'(\w[\w\s]+?)\s*\n\s*Secteur', text)
        data['secteur'] = sec.group(1).replace('actions','').strip() if sec else None

        # Ratios financiers
        pe_match = re.search(r'P/E Ratio\s*([\d.,]+)', text)
        data['pe_actuel'] = clean_val(pe_match.group(1)) if pe_match else None
        
        div_match = re.search(r'Rendement Dividende\s*([\d.,]+)%', text)
        data['div_yield_pct'] = clean_val(div_match.group(1)) if div_match else None

        # Evolution Pluriannuelle
        pluri = re.search(r'Évolution Pluriannuelle(.+?)Capitalisation Boursière', text, re.DOTALL)
        if pluri:
            st = pluri.group(1)
            pats = {
                'pe': r'Price earning ratio \(x\).*?\s+([\d.\s]+)', 
                'bpa': r'Bénéfice par action \(DH\).*?\s+([\d.\s]+)', 
                'dpa': r'Dividende par action \(DH\).*?\s+([\d.\s]+)', 
                'rn': r'Résultat net \(MDH\).*?\s+([\d.\s]+)'
            }
            for k, p in pats.items():
                m = re.search(p, st)
                if m:
                    v = split_two_numbers(m.group(1))
                    data[f'{k}_2023'], data[f'{k}_2022'] = v[0], v[1]
        
        all_details.append(data)
        if i % 10 == 0:
            print(f"Progression : {i}/{len(all_tickers)}")
        time.sleep(0.2)
    except Exception as e:
        print(f"Erreur sur {name} : {e}")
        continue

# --- ETAPE 3 : FUSION ET INSERTION ---
df_base = pd.DataFrame(rows_base)
df_details = pd.DataFrame(all_details)
df_final = df_base.merge(df_details, on='entreprise', how='left')
df_final['date_collecte'] = datetime.now().strftime("%Y-%m-%d")

# Conversion des NaN en None pour la compatibilite SQL
records = df_final.where(pd.notnull(df_final), None).to_dict(orient='records')

try:
    supabase.table("bourse_details").insert(records).execute()
    print(f"Operation terminee avec succes : {len(records)} lignes inserees.")
except Exception as e:
    print(f"Erreur lors de l'insertion Supabase : {e}")
