import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
import os

def clean_price(text):
    """Mengubah 'Rp 2,5 Miliar' menjadi 2500000000."""
    if not text: return 0
    text = text.lower().replace('rp', '').replace('.', '').replace(',', '').strip()
    try:
        numbers = re.findall(r'\d+', text)
        if not numbers: return 0
        val = float(numbers[0])
        if 'miliar' in text: val *= 1_000_000_000
        elif 'juta' in text: val *= 1_000_000
        return val
    except:
        return 0

def run_etl():
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    kecamatans = ['tebet', 'cilandak', 'kebayoran-baru', 'jagakarsa', 'mampang-prapatan']
    new_listings = []

    for kec in kecamatans:
        # Contoh URL (Sesuaikan dengan target situs properti yang Anda pilih)
        url = f"https://www.rumah123.com/jual/jakarta-selatan/{kec}/"
        try:
            res = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Selector di bawah ini adalah ilustrasi, sesuaikan dengan web target saat inspect element
            cards = soup.find_all('div', class_='ui-organism-intersection-observer-wrapper')
            
            for card in cards:
                try:
                    judul = card.find('h2').text.strip()
                    harga_raw = card.find('div', class_='ui-atomic-text--bold').text.strip()
                    lt_raw = card.find('span', text=re.compile(r'm²')).text.strip()
                    
                    harga = clean_price(harga_raw)
                    luas_tanah = int(re.findall(r'\d+', lt_raw)[0])
                    
                    if harga > 0:
                        new_listings.append({
                            'tanggal_ambil': datetime.now().strftime('%Y-%m-%d'),
                            'judul': judul,
                            'kecamatan': kec.replace('-', ' ').title(),
                            'harga': harga,
                            'luas_tanah': luas_tanah,
                            'harga_per_m2': harga / luas_tanah
                        })
                except: continue
        except Exception as e:
            print(f"Gagal akses {kec}: {e}")

    if not new_listings: return

    df_new = pd.DataFrame(new_listings)
    file_path = 'data/property_data.csv'

    # Buat folder data jika belum ada
    os.makedirs('data', exist_ok=True)

    # Logika Append: Menggabungkan data lama dan baru, hapus duplikat
    if os.path.exists(file_path):
        df_old = pd.read_csv(file_path)
        df_final = pd.concat([df_old, df_new]).drop_duplicates(subset=['judul', 'harga'], keep='first')
    else:
        df_final = df_new

    df_final.to_csv(file_path, index=False)
    print(f"Berhasil memperbarui data. Total baris sekarang: {len(df_final)}")

if __name__ == "__main__":
    run_etl()