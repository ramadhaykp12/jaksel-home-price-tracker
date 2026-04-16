import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Jaksel Property Tracker", layout="wide")

st.title("🏙️ Jakarta Selatan Property Market Dashboard")

try:
    df = pd.read_csv('data/property_data.csv')
    
    # Sidebar Filter
    st.sidebar.header("Filter")
    kec_filter = st.sidebar.multiselect("Kecamatan", options=df['kecamatan'].unique(), default=df['kecamatan'].unique())
    df_filtered = df[df['kecamatan'].isin(kec_filter)]

    # Metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Iklan", len(df_filtered))
    c2.metric("Rata-rata Harga", f"Rp {df_filtered['harga'].mean()/1e9:.1f} M")
    c3.metric("Rata-rata m²", f"Rp {df_filtered['harga_per_m2'].mean()/1e6:.1f} Juta")

    # Chart 1: Harga per m2 tiap Kecamatan
    st.subheader("Perbandingan Harga per m² per Kecamatan")
    fig1 = px.box(df_filtered, x='kecamatan', y='harga_per_m2', color='kecamatan')
    st.plotly_chart(fig1, use_container_width=True)

    # Chart 2: Hubungan Luas vs Harga
    st.subheader("Analisis Luas Tanah vs Harga Total")
    fig2 = px.scatter(df_filtered, x='luas_tanah', y='harga', color='kecamatan', 
                     hover_data=['judul'], size='harga_per_m2')
    st.plotly_chart(fig2, use_container_width=True)

    # Data Table
    st.subheader("Data Properti Terbaru")
    st.dataframe(df_filtered.sort_values(by='tanggal_ambil', ascending=False))

except Exception as e:
    st.warning("Data belum tersedia. Tunggu bot scraper berjalan pertama kali di GitHub.")