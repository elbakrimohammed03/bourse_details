import requests
import pandas as pd
import re
import time
import os
from datetime import datetime
from bs4 import BeautifulSoup

# ============================================
# CONFIGURATION & UTILS
# ============================================
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}
base_url = "https://www.casablancabourse.com"

def split_two_numbers(raw):
    """Sépare deux nombres collés (ex: 15.4012.30 -> 15.40, 12.30)"""
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

# ============================================
# ÉTAPE 1 : COURS DU JOUR
# ============================================
print("🚀 Démarrage de la collecte...")
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
            'Entreprise': cols[1].get_text(strip=True),
            'Volume_Actions': cols[3].get_text(strip=True).replace(',','').strip(),
            'Prix_MAD': cols[6].get_text(strip=True).replace('DH','').replace(',','').strip(),
            'Variation_Pct': cols[7].get_text(strip=True).replace('%','').replace('↓','').replace('↑','').strip()
        })

df_base = pd.DataFrame(rows_base)
df_base['Prix_MAD'] = pd.to_numeric(df_base['Prix_MAD'], errors='coerce')
df_base['Variation_Pct'] = pd.to_numeric(df_base['Variation_Pct'], errors='coerce')

# ============================================
# ÉTAPE 2 : SCRAPING PROFOND (80 PAGES)
# ============================================
all_details = []
for i, (name, ticker) in enumerate(all_tickers.items(), 1):
    try:
        url = f"{base_url}/{ticker}/action/capitalisation"
        r = requests.get(url, headers=headers, timeout=15)
        text = BeautifulSoup(r.text, 'lxml').get_text()
        
        data = {'Entreprise': name, 'Ticker': ticker}
        
        # Secteur (Nettoyage)
        sec = re.search(r'(\w[\w\s]+?)\s*\n\s*Secteur', text)
        data['Secteur'] = sec.group(1).replace('actions','').strip() if sec else None

        # Ratios actuels
        pe = re.search(r'P/E Ratio\s*([\d.]+)', text)
        data['PE_Actuel'] = float(pe.group(1)) if pe else None
        div = re.search(r'Rendement Dividende\s*([\d.]+)%', text)
        data['Div_Yield_Pct'] = float(div.group(1)) if div else None

        # Pluriannuel (2023 vs 2022)
        pluri = re.search(r'Évolution Pluriannuelle(.+?)Capitalisation Boursière', text, re.DOTALL)
        if pluri:
            sec_text = pluri.group(1)
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
                    data[f'{key}_2023'], data[f'{key}_2022'] = vals[0], vals[1]

        all_details.append(data)
        time.sleep(0.4)
    except: continue

# ============================================
# ÉTAPE 3 : MERGE & SAVE
# ============================================
df_details = pd.DataFrame(all_details)
df_final = df_base.merge(df_details, on='Entreprise', how='left')
df_final['Date_Collecte'] = datetime.now().strftime("%Y-%m-%d")

nom_fichier = 'bourse_casablanca_historique.csv'
if os.path.exists(nom_fichier):
    # On garde les mêmes colonnes pour l'append
    df_final.to_csv(nom_fichier, mode='a', index=False, header=False, encoding='utf-8')
else:
    df_final.to_csv(nom_fichier, index=False, header=True, encoding='utf-8')

print(f"✅ Terminé ! {len(df_final)} entreprises sauvegardées.")
