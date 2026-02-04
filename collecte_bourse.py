import requests
import pandas as pd
import re
import time
import os
from datetime import datetime
from bs4 import BeautifulSoup

# Configuration
headers = {"User-Agent": "Mozilla/5.0"}
base_url = "https://www.casablancabourse.com"

def split_two_numbers(raw):
    raw = str(raw).strip()
    if ' ' in raw:
        nums = re.findall(r'[\d ]+\.\d+', raw)
        if len(nums) >= 2:
            return [float(nums[0].replace(' ', '')), float(nums[1].replace(' ', ''))]
    return [None, None]

# 1. Extraction des cours
resp = requests.get(base_url, headers=headers, timeout=10)
soup = BeautifulSoup(resp.text, 'lxml')
all_tickers = {}
for a in soup.find_all('a', href=True):
    if '/action/capitalisation' in a['href']:
        ticker = a['href'].split('/')[1]
        all_tickers[a.get_text(strip=True)] = ticker

rows = []
for row in soup.find_all('tr'):
    cols = row.find_all('td')
    if len(cols) == 9:
        rows.append({
            'Entreprise': cols[1].get_text(strip=True),
            'Prix (MAD)': cols[6].get_text(strip=True).replace('DH','').replace(',','').strip(),
            'Volume_num': cols[3].get_text(strip=True).replace(',','').strip()
        })

df_base = pd.DataFrame(rows)
df_base['Prix (MAD)'] = pd.to_numeric(df_base['Prix (MAD)'], errors='coerce')

# 2. Collecte des détails
all_details = []
for name, ticker in list(all_tickers.items()):
    try:
        url = f"{base_url}/{ticker}/action/capitalisation"
        r = requests.get(url, headers=headers, timeout=10)
        text = BeautifulSoup(r.text, 'lxml').get_text()
        pe = re.search(r'P/E Ratio\s*([\d.]+)', text)
        div = re.search(r'Rendement Dividende\s*([\d.]+)%', text)
        all_details.append({
            'Entreprise': name, 
            'P/E Ratio': float(pe.group(1)) if pe else None,
            'Dividende %': float(div.group(1)) if div else None
        })
        time.sleep(0.5)
    except: continue

# 3. Fusion et Sauvegarde
df_final = df_base.merge(pd.DataFrame(all_details), on='Entreprise', how='left')
df_final['Date_Collecte'] = datetime.now().strftime("%Y-%m-%d")

nom_fichier = 'bourse_casablanca_historique.csv'
if os.path.exists(nom_fichier):
    df_final.to_csv(nom_fichier, mode='a', index=False, header=False, encoding='utf-8')
else:
    df_final.to_csv(nom_fichier, index=False, header=True, encoding='utf-8')
