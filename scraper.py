
import argparse
import re
import sys
from pathlib import Path

import pandas as pd

# ──────────────────────────────────────────────────────────────
# Konfigurasi
# ──────────────────────────────────────────────────────────────
DEFAULT_INPUT  = "dekoruma_jakarta_selatan.csv"
DEFAULT_OUTPUT = "dekoruma_jakarta_selatan_clean.csv"

# Daftar nama kecamatan & kelurahan di Jakarta Selatan
# (urut dari nama paling panjang agar lebih spesifik duluan)
KECAMATAN_JAKSEL = sorted([
    "Mampang Prapatan", "Pondok Pinang", "Kebayoran Baru", "Kebayoran Lama",
    "Pasar Minggu", "Cilandak Barat", "Jati Padang", "Pondok Labu",
    "Pondok Jaya", "Duren Tiga", "Lebak Bulus", "Pesanggrahan",
    "Petukangan", "Cilandak", "Jagakarsa", "Gandaria", "Setiabudi",
    "Pejaten", "Cipulir", "Kalibata", "Kuningan", "Kemang", "Fatmawati",
    "Pancoran", "Cipete", "Cawang", "Bangka", "Bintaro", "Cinere", "Tebet",
], key=len, reverse=True)


# ──────────────────────────────────────────────────────────────
# Fungsi utilitas
# ──────────────────────────────────────────────────────────────
def _parse_harga(raw: str) -> float | None:
    """
    Konversi string harga ke nilai numerik (Rupiah penuh).
    Contoh:
      "Rp3M"    → 3_000_000_000
      "Rp890jt" → 890_000_000
      "Rp2,5M"  → 2_500_000_000
    """
    if not raw or pd.isna(raw):
        return None
    raw = str(raw).strip().replace(",", ".")
    m = re.match(r"Rp([\d.]+)(M|jt)", raw, re.IGNORECASE)
    if not m:
        return None
    angka = float(m.group(1))
    satuan = m.group(2).lower()
    if satuan == "m":
        return angka * 1_000_000_000
    else:  # jt
        return angka * 1_000_000


def _format_harga(nilai: float | None) -> str:
    """Format nilai numerik kembali ke string yang mudah dibaca."""
    if nilai is None or pd.isna(nilai):
        return ""
    if nilai >= 1_000_000_000:
        v = nilai / 1_000_000_000
        return f"Rp {v:g} M"
    else:
        v = nilai / 1_000_000
        return f"Rp {v:g} jt"


def _extract_kecamatan(teks: str) -> str:
    """
    Cari nama kecamatan yang dikenal di dalam teks.
    Strategi utama: cari pola '<Kecamatan>,Jakarta' agar tidak salah ambil
    nama kecamatan yang kebetulan ada di dalam nama properti
    (mis. "Pondok Pinang Residence" vs "Pondok Pinang,Jakarta Selatan").
    Fallback: pencarian bebas jika pola anchor tidak ditemukan.
    """
    # Strategi 1: anchor ke ',Jakarta' — lebih presisi
    for kec in KECAMATAN_JAKSEL:
        pattern = rf"{re.escape(kec)},Jakarta"
        if re.search(pattern, teks, re.IGNORECASE):
            return kec
    # Strategi 2: fallback pencarian bebas
    for kec in KECAMATAN_JAKSEL:
        if kec.lower() in teks.lower():
            return kec
    return ""


def _extract_nama_bersih(nama_raw: str, kecamatan: str) -> str:
    """
    Pisahkan nama properti dari teks gabungan.
    Nama properti ada di awal, sebelum blok '<Kecamatan>,Jakarta' dimulai.
    Dengan pola ini, kecamatan yang merupakan bagian dari nama properti
    (mis. "Pondok Pinang Residence") tidak ikut terpotong.
    """
    if not kecamatan:
        m = re.split(r",Jakarta|Rp\d", nama_raw)
        return m[0].strip() if m else nama_raw.strip()

    # Cari posisi '<Kecamatan>,Jakarta' dalam string
    pattern = rf"{re.escape(kecamatan)},Jakarta"
    m = re.search(pattern, nama_raw, re.IGNORECASE)
    if m and m.start() > 0:
        return nama_raw[:m.start()].strip()
    # Fallback: potong di awal kemunculan kecamatan
    idx = nama_raw.lower().find(kecamatan.lower())
    if idx > 0:
        return nama_raw[:idx].strip()
    return nama_raw.strip()


def _extract_cicilan(nama_raw: str) -> str:
    """Ambil nilai cicilan per bulan dari teks nama gabungan."""
    m = re.search(r"Cicilan\s*(\d+)\s*jt/bulan", nama_raw, re.IGNORECASE)
    if m:
        return f"{m.group(1)} jt/bulan"
    return ""


def _extract_luas(nama_raw: str, prefix: str) -> tuple[str, str]:
    """
    Ekstrak luas tanah (LT) atau luas bangunan (LB) dari teks gabungan.
    Mengembalikan (min, max); max bisa kosong jika tunggal.
    Contoh:
      "LT82-162m²"  → ("82 m²", "162 m²")
      "LT128-130m²" → ("128 m²", "130 m²")
      "LT128m²"     → ("128 m²", "")
    """
    # Pola: LT atau LB diikuti angka, opsional "-angka", lalu m²
    pattern = rf"{prefix}\s*([\d]+)\s*(?:-\s*([\d]+))?\s*m²"
    m = re.search(pattern, nama_raw, re.IGNORECASE)
    if not m:
        return ("", "")
    lo = f"{m.group(1)} m²"
    hi = f"{m.group(2)} m²" if m.group(2) else ""
    return (lo, hi)


def _fix_status(status_raw: str) -> str:
    """
    Normalisasi kolom status.
    Jika bukan nilai yang dikenal, kembalikan 'Properti Baru' sebagai default.
    """
    valid = {"properti baru", "properti bekas", "second"}
    if str(status_raw).strip().lower() in valid:
        return status_raw.strip()
    # Anomali: status berisi nama properti (mis. "Precium Kebayoran Baru")
    return "Properti Baru"


# ──────────────────────────────────────────────────────────────
# Pipeline utama
# ──────────────────────────────────────────────────────────────
def extract(filepath: str) -> pd.DataFrame:
    """Baca CSV hasil scraping."""
    print(f"[EXTRACT] Membaca: {filepath}")
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    print(f"          {len(df)} baris, {len(df.columns)} kolom ditemukan.")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Bersihkan dan perkaya semua kolom."""
    print("[TRANSFORM] Memulai proses pembersihan data...")
    df = df.copy()
    total = len(df)

    # ── 1. Kecamatan ─────────────────────────────────────────
    print("  [1/7] Mengekstrak kolom kecamatan dari nama...")
    df["kecamatan"] = df["nama"].apply(_extract_kecamatan)
    found = df["kecamatan"].ne("").sum()
    print(f"        {found}/{total} baris berhasil diisi.")

    # ── 2. Nama bersih ────────────────────────────────────────
    print("  [2/7] Membersihkan kolom nama (memisahkan dari teks gabungan)...")
    df["nama"] = df.apply(
        lambda r: _extract_nama_bersih(r["nama"], r["kecamatan"]), axis=1
    )

    # ── 3. Lokasi ─────────────────────────────────────────────
    print("  [3/7] Memperbaiki kolom lokasi...")
    df["lokasi"] = df["kecamatan"].apply(
        lambda k: f"{k}, Jakarta Selatan" if k else "Jakarta Selatan"
    )

    # ── 4. LT & LB ───────────────────────────────────────────
    print("  [4/7] Mengekstrak Luas Tanah (lt) dan Luas Bangunan (lb)...")
    # Simpan nama_raw dulu (sebelum dibersihkan — sudah terlanjur diubah di step 2)
    # Kita perlu re-baca dari file, tapi kita sudah punya datanya di kolom nama.
    # Trik: gabungkan kembali kecamatan ke nama untuk pola lengkap — atau
    # lebih mudah, kita simpan nama_raw dari awal sebelum transform step 2.
    # Karena urutan transform, kita ekstrak LT/LB dari df asli (pakai original_nama).
    pass  # ditangani di bawah dengan original_nama

    # ── 5. Cicilan ────────────────────────────────────────────
    print("  [5/7] Mengekstrak nilai cicilan per bulan...")
    pass  # sama, ditangani di bawah

    # ── 6. Harga ──────────────────────────────────────────────
    print("  [6/7] Menormalisasi format harga (min & max)...")
    df["harga_min_num"] = df["harga_min"].apply(_parse_harga)
    df["harga_max_num"] = df["harga_max"].apply(_parse_harga)
    df["harga_min"] = df["harga_min_num"].apply(_format_harga)
    df["harga_max"] = df["harga_max_num"].apply(_format_harga)

    # ── 7. Status ─────────────────────────────────────────────
    print("  [7/7] Memperbaiki kolom status...")
    anomali = (df["status"].str.strip().str.lower()
               .isin({"properti baru", "properti bekas", "second"}))
    n_anomali = (~anomali).sum()
    df["status"] = df["status"].apply(_fix_status)
    if n_anomali:
        print(f"        {n_anomali} baris anomali status diperbaiki ke 'Properti Baru'.")

    return df


def transform_with_raw(raw_df: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """
    Gunakan nama_raw (sebelum dibersihkan) untuk ekstrak LT/LB & cicilan.
    raw_df = DataFrame asli dari extract()
    df     = DataFrame setelah transform()
    """
    print("  [LT/LB] Mengekstrak luas tanah & bangunan dari sumber asli...")
    lt_min_list, lt_max_list, lb_min_list, lb_max_list, cicilan_list = [], [], [], [], []

    for nama_raw in raw_df["nama"]:
        lo, hi = _extract_luas(nama_raw, "LT")
        lt_min_list.append(lo)
        lt_max_list.append(hi)

        lo, hi = _extract_luas(nama_raw, "LB")
        lb_min_list.append(lo)
        lb_max_list.append(hi)

        cicilan_list.append(_extract_cicilan(nama_raw))

    df["lt_min"] = lt_min_list
    df["lt_max"] = lt_max_list
    df["lb_min"] = lb_min_list
    df["lb_max"] = lb_max_list
    df["cicilan"] = cicilan_list

    lt_found = sum(1 for v in lt_min_list if v)
    lb_found = sum(1 for v in lb_min_list if v)
    cic_found = sum(1 for v in cicilan_list if v)
    total = len(raw_df)
    print(f"          LT: {lt_found}/{total} | LB: {lb_found}/{total} | Cicilan: {cic_found}/{total}")
    return df


def load(df: pd.DataFrame, filepath: str):
    """Simpan DataFrame hasil ETL ke CSV."""
    # Kolom final (hapus kolom numerik sementara)
    cols = [
        "nama", "lokasi", "kecamatan",
        "harga_min", "harga_max", "cicilan",
        "lt_min", "lt_max", "lb_min", "lb_max",
        "status", "url",
    ]
    # Tambahkan kolom numerik harga sebagai bonus
    cols_bonus = ["harga_min_num", "harga_max_num"]
    final_cols = cols + [c for c in cols_bonus if c in df.columns]

    df[final_cols].to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"\n[LOAD] {len(df)} baris berhasil disimpan ke: {filepath}")


def report(raw_df: pd.DataFrame, clean_df: pd.DataFrame):
    """Tampilkan ringkasan perubahan data."""
    print("\n" + "=" * 60)
    print("  LAPORAN ETL")
    print("=" * 60)

    checks = [
        ("nama",      "Nama properti bersih (bukan teks gabungan)",
         raw_df["nama"].str.contains(r"Jakarta Selatan").sum(),
         clean_df["nama"].str.contains(r"Jakarta Selatan").sum()),
        ("kecamatan", "Kecamatan terisi",
         raw_df["kecamatan"].isna().sum(),
         clean_df["kecamatan"].eq("").sum()),
        ("cicilan",   "Cicilan berisi angka",
         (raw_df["cicilan"] == "Cicilan").sum(),
         clean_df["cicilan"].eq("").sum()),
        ("lt_min",    "Luas Tanah min terisi",
         raw_df["lt_min"].isna().sum(),
         clean_df["lt_min"].eq("").sum()),
        ("lb_min",    "Luas Bangunan min terisi",
         raw_df["lb_min"].isna().sum(),
         clean_df["lb_min"].eq("").sum()),
    ]

    for col, label, sebelum_error, sesudah_error in checks:
        status = "✅" if sesudah_error < sebelum_error else "⚠️ "
        print(f"  {status} [{col:10}] {label}")
        print(f"            Sebelum bermasalah: {sebelum_error} baris  →  "
              f"Sesudah bermasalah: {sesudah_error} baris")

    print()
    print("  Contoh data bersih (5 baris pertama):")
    preview = clean_df[["nama", "kecamatan", "harga_min", "harga_max",
                         "cicilan", "lt_min", "lb_min", "status"]].head(5)
    print(preview.to_string(index=False))
    print("=" * 60)


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="ETL pipeline untuk membersihkan data scraping Dekoruma"
    )
    parser.add_argument(
        "--input", default=DEFAULT_INPUT,
        help=f"File CSV input (default: {DEFAULT_INPUT})"
    )
    parser.add_argument(
        "--output", default=DEFAULT_OUTPUT,
        help=f"File CSV output (default: {DEFAULT_OUTPUT})"
    )
    args = parser.parse_args()

    if not Path(args.input).exists():
        print(f"[ERROR] File tidak ditemukan: {args.input}")
        sys.exit(1)

    print("=" * 60)
    print("  ETL Pipeline — Dekoruma Jakarta Selatan")
    print("=" * 60)

    # Extract
    raw_df = extract(args.input)

    # Transform (nama asli diperlukan untuk ekstraksi LT/LB & cicilan)
    clean_df = transform(raw_df)
    clean_df = transform_with_raw(raw_df, clean_df)

    # Report
    report(raw_df, clean_df)

    # Load
    load(clean_df, args.output)


if __name__ == "__main__":
    main()