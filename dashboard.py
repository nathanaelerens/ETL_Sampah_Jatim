import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# 🚨 Configuration: Use the Docker service name for the database host
# If your PostgreSQL service is named 'postgres', use that.
DB_URI = 'postgresql://airflow:airflow@postgres:5432/proyek'

st.set_page_config(page_title="Jatim Waste Tracker", layout="wide")

# Function to load data from the final table
@st.cache_data
def load_data():
    engine = create_engine(DB_URI)
    # Query the single joined table with lowercase column names
    query = """
    SELECT 
        tahun,
        kabupaten_kota as kota, 
        timbulan_sampah_tahunan as total_timbulan_ton,
        sisa_makanan,
        kayu_ranting,
        kertas_karton,
        plastik,
        logam
        
    FROM sampah_joined
    ORDER BY timbulan_sampah_tahunan DESC
    """
    df = pd.read_sql(query, engine)
    return df

# --- Dashboard Layout ---

st.title("🗑️ Dashboard Monitoring Sampah Jawa Timur")

try:
    df = load_data()
except Exception as e:
    st.error("Gagal koneksi ke database. Pastikan Airflow DAG sudah berjalan dan database service aktif.")
    st.exception(e)
    st.stop()

# --- SIDEBAR FILTER ---
st.sidebar.header("Filter Data")
tahun_list = sorted(df['tahun'].unique(), reverse=True)
pilih_tahun = st.sidebar.selectbox("Pilih Tahun Laporan", tahun_list)

df_filtered = df[df['tahun'] == pilih_tahun].copy()

# --- KPI METRICS ---
col1, col2, col3 = st.columns(3)
total_sampah = df_filtered['total_timbulan_ton'].sum()
top_city_row = df_filtered.sort_values(by='total_timbulan_ton', ascending=False).iloc[0]
avg_sampah = df_filtered['total_timbulan_ton'].mean()

col1.metric("Total Sampah (Tahunan)", f"{total_sampah:,.0f} Ton")
col2.metric("Penyumbang Terbesar", top_city_row['kota'])
col3.metric("Rata-rata per Kota", f"{avg_sampah:,.0f} Ton")

st.divider()

# --- GRAFIK (Top 10 Timbulan) ---
col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.subheader("🏆 Top 10 Kota Penghasil Sampah")
    top_10 = df_filtered.sort_values(by='total_timbulan_ton', ascending=False).head(10)
    fig_top10 = px.bar(top_10, 
                       x='kota', 
                       y='total_timbulan_ton', 
                       color='total_timbulan_ton')
    st.plotly_chart(fig_top10, use_container_width=True)

with col_chart2:
    st.subheader("♻️ Komposisi Sampah (Top 5 Kota)")
    komposisi_cols = ['sisa_makanan', 'kayu_ranting', 'kertas_karton', 'plastik', 'logam']
    top_5_composition = df_filtered.sort_values(by='total_timbulan_ton', ascending=False).head(5)
    
    # Melt data for stacked bar visualization
    df_melted = top_5_composition.melt(id_vars=['kota'], value_vars=komposisi_cols,
                                       var_name='Jenis Sampah', value_name='Persentase')
    
    fig_comp = px.bar(df_melted, x='kota', y='Persentase', color='Jenis Sampah',
                      title="Komposisi Sampah (Top 5 Penyumbang)")
    st.plotly_chart(fig_comp, use_container_width=True)

# --- TABEL DATA ---
st.divider()
st.subheader(f"📋 Detail Data ({pilih_tahun})")
st.dataframe(df_filtered, use_container_width=True)