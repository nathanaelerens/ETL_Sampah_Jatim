import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# --- KONFIGURASI DATABASE ---
# Pastikan passwordnya benar (misal: 12345)
# Gunakan 127.0.0.1 agar aman
DB_URI = 'postgresql://postgres:12345@127.0.0.1:5432/waste_management_db'

st.set_page_config(page_title="Jatim Waste Tracker", layout="wide")

# Fungsi Load Data
def load_data():
    engine = create_engine(DB_URI)
    # Query join tabel Fakta dan Dimensi
    # Kita ambil nama kota dari tabel dimensi
    query = """
    SELECT 
        d.nama_kabkota_clean as kota,
        f.tahun,
        f.total_timbulan_ton,
        f.vol_rumah_tangga_ton,
        f.vol_perkantoran_ton,
        f.vol_pasar_ton,
        f.vol_perniagaan_ton,
        f.vol_fasilitas_publik_ton,
        f.vol_kawasan_ton,
        f.vol_lainnya_ton
    FROM fact_sampah_jatim f
    JOIN dim_lokasi_jatim d ON f.id_lokasi = d.id_lokasi
    ORDER BY f.total_timbulan_ton DESC
    """
    df = pd.read_sql(query, engine)
    return df

# --- JUDUL DASHBOARD ---
st.title("🗑️ Dashboard Monitoring Sampah Jawa Timur")
st.markdown("Sistem pemantauan volume dan komposisi sampah berbasis **Data Warehouse**.")

# --- LOAD DATA ---
try:
    df = load_data()
except Exception as e:
    st.error(f"Gagal koneksi ke database: {e}")
    st.stop()

# --- SIDEBAR FILTER ---
st.sidebar.header("Filter Data")
tahun_list = sorted(df['tahun'].unique(), reverse=True)
pilih_tahun = st.sidebar.selectbox("Pilih Tahun Laporan", tahun_list)

# Filter dataframe berdasarkan tahun yang dipilih
df_filtered = df[df['tahun'] == pilih_tahun]

# --- KPI METRICS (Kotak Angka di Atas) ---
col1, col2, col3 = st.columns(3)
total_sampah = df_filtered['total_timbulan_ton'].sum()
# Cari kota dengan sampah terbanyak
top_city_row = df_filtered.sort_values(by='total_timbulan_ton', ascending=False).iloc[0]
avg_sampah = df_filtered['total_timbulan_ton'].mean()

col1.metric("Total Sampah se-Jatim", f"{total_sampah:,.0f} Ton")
col2.metric("Penyumbang Terbesar", top_city_row['kota'])
col3.metric("Rata-rata per Kota", f"{avg_sampah:,.0f} Ton")

st.divider()

# --- LAYOUT GRAFIK (2 Kolom) ---
col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.subheader("🏆 Top 10 Kota Penghasil Sampah")
    # Ambil 10 teratas
    top_10 = df_filtered.sort_values(by='total_timbulan_ton', ascending=False).head(10)
    st.bar_chart(top_10.set_index('kota')['total_timbulan_ton'], color="#FF4B4B")

with col_chart2:
    st.subheader("📊 Profil Sumber Sampah (Top 5 Kota)")
    top_5 = df_filtered.head(5)
    # Siapkan data untuk Stacked Bar
    data_sumber = top_5[['kota', 'vol_rumah_tangga_ton', 'vol_pasar_ton', 'vol_perniagaan_ton', 'vol_fasilitas_publik_ton']]
    data_sumber = data_sumber.set_index('kota')
    # Tampilkan grafik
    st.bar_chart(data_sumber)

# --- TABEL DATA ---
st.divider()
st.subheader(f"📋 Detail Data ({pilih_tahun})")
st.dataframe(df_filtered, use_container_width=True)