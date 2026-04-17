import streamlit as st
import pandas as pd
import plotly.express as px

st.title("🏙️ Jakarta Selatan Property Analytics")

try:
    df = pd.read_csv('data/property_data.csv')
    
    # Simple Metrics
    col1, col2 = st.columns(2)
    col1.metric("Rerata Harga/m²", f"Rp {df['harga_per_m2'].mean():,.0f}")
    col2.metric("Total Iklan Terdata", len(df))

    # Grafik Harga per Wilayah
    st.subheader("Distribusi Harga per m²")
    fig = px.box(df, x='lokasi', y='harga_per_m2', color='lokasi')
    st.plotly_chart(fig, use_container_width=True)

    # List Data
    st.subheader("Daftar Properti")
    st.dataframe(df)
except:
    st.info("Belum ada data. Jalankan scraper.py di GitHub Actions.")