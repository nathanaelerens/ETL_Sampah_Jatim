from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

import pandas as pd
import glob
import time
import os
import shutil


# ======================================================================
# Helper: tunggu file Excel selesai di-download
# ======================================================================

def wait_for_download(download_path, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        files = glob.glob(os.path.join(download_path, "*.xlsx"))
        if files:
            if all(not f.endswith(".crdownload") for f in files):
                return files
        time.sleep(1)
    raise TimeoutError("Download tidak selesai dalam waktu yang ditentukan.")


# ======================================================================
# Selenium downloader yang sudah stabil (dipakai untuk Timbulan & Komposisi)
# ======================================================================

def download_sipsn(url, output_dir):
    download_dir = output_dir 

    # 🚨 CHANGE 2: Ensure the directory exists before starting the download
    # if not os.path.exists(download_dir):
    #     os.makedirs(download_dir)

    os.makedirs(download_dir, exist_ok=True)

    chrome_options = Options()
    chrome_options.binary_location = "/usr/bin/chromium"
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")

    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(
        service=Service("/usr/bin/chromedriver"),
        options=chrome_options
    )

    wait = WebDriverWait(driver, 40)

    try:
        driver.get(url)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # ========================
        # Pilih Tahun (2024)
        # ========================
        # select_tahun = Select(driver.find_element(By.ID, "filter_id_tahun"))
        # select_tahun.select_by_visible_text("2024")
        # time.sleep(1)
        # ==================================
        # 🚨 NEW STEP: PILIH JUMLAH ENTRI ("SEMUA")
        # ==================================
        # Locate the dropdown using its NAME attribute confirmed from HTML
        select_entries_element = driver.find_element(By.NAME, "tabeldata_length")
        select_entries = Select(select_entries_element)
        
        # Select by value "-1", which corresponds to the "SEMUA" option
        select_entries.select_by_value("-1")
        
        # Wait for the table data to fully load (this takes longer for all entries)
        time.sleep(5)
        # ========================
        # PILIH PROVINSI (HANYA INI YANG DIGUNAKAN)
        # ========================
        select_prov = Select(driver.find_element(By.ID, "filter_id_propinsi"))
        select_prov.select_by_visible_text("Jawa Timur")
        time.sleep(1)

        # ========================
        # PILIH KAB/KOTA
        # ========================
        # select_kabkot = Select(driver.find_element(By.ID, "filter_id_kabkot"))
        # select_kabkot.select_by_visible_text("Semua Kabupaten/Kota")
        # time.sleep(1)

        # ========================
        # KLIK TOMBOL DOWNLOAD EXCEL
        # ========================
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.dropdown-toggle"))).click()
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.buttons-excel"))).click()

        # Tunggu file selesai download
        wait_for_download(download_dir)

    finally:
        driver.quit()



def download_timbulan():
    download_sipsn("https://sipsn.kemenlh.go.id/sipsn/public/data/timbulan", "/opt/airflow/downloads/timbulan_data")


def download_komposisi():
    download_sipsn("https://sipsn.kemenlh.go.id/sipsn/public/data/komposisi", "/opt/airflow/downloads/komposisi_data")


# ======================================================================
# Convert Excel → CSV
# ======================================================================

def convert_timbulan():
    files = glob.glob("/opt/airflow/downloads/timbulan_data/*Timbulan*.xlsx")
    if not files:
        raise ValueError("Timbulan tidak ditemukan!")

    df = pd.read_excel(files[-1], header=1)
    df = df[df["Tahun"].astype(str).str.isdigit()]
    df.to_csv("/opt/airflow/csv/timbulan.csv", index=False)


def convert_komposisi():
    files = glob.glob("/opt/airflow/downloads/komposisi_data/*Komposisi*.xlsx")
    if not files:
        raise ValueError("Komposisi tidak ditemukan!")

    df = pd.read_excel(files[-1], header=1)
    df = df[df["Tahun"].astype(str).str.isdigit()]
    df.to_csv("/opt/airflow/csv/komposisi.csv", index=False)


# ======================================================================
# Insert CSV → PostgreSQL
# ======================================================================

def insert_csv():
    hook = PostgresHook(postgres_conn_id="Sampah_jatim")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE timbulan_sampah;")
    cur.execute("TRUNCATE komposisi_sampah;")

    with open("/opt/airflow/csv/timbulan.csv", "r") as f:
        cur.copy_expert("COPY timbulan_sampah FROM STDIN WITH CSV HEADER", f)

    with open("/opt/airflow/csv/komposisi.csv", "r") as f:
        cur.copy_expert("COPY komposisi_sampah FROM STDIN WITH CSV HEADER", f)

    conn.commit()


# ======================================================================
# DAG
# ======================================================================

default_args = {
    "owner": "nathan",
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="pipeline_sampah_jatim_final_beneran",
    start_date=datetime(2025, 12, 1),
    schedule="@daily",
    default_args=default_args,
    catchup=False
) as dag:

    task_download_timbulan = PythonOperator(
        task_id="download_timbulan",
        python_callable=download_timbulan
    )

    task_download_komposisi = PythonOperator(
        task_id="download_komposisi",
        python_callable=download_komposisi
    )

    task_convert_timbulan = PythonOperator(
        task_id="convert_timbulan",
        python_callable=convert_timbulan
    )

    task_convert_komposisi = PythonOperator(
        task_id="convert_komposisi",
        python_callable=convert_komposisi
    )

    task_create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id="Sampah_jatim",
        sql="""
        CREATE TABLE IF NOT EXISTS timbulan_sampah (
            Tahun INT,
            Provinsi VARCHAR(50),
            Kabupaten_Kota VARCHAR(100),
            Timbulan_sampah_harian FLOAT,
            Timbulan_sampah_tahunan FLOAT
        );

        CREATE TABLE IF NOT EXISTS komposisi_sampah (
            Tahun INT,
            Provinsi VARCHAR(50),
            Kabupaten_Kota VARCHAR(100),
            Sisa_Makanan FLOAT,
            Kayu_Ranting FLOAT,
            Kertas_Karton FLOAT,
            Plastik FLOAT,
            Logam FLOAT,
            Kain FLOAT,
            Karet_Kulit FLOAT,
            Kaca FLOAT,
            Lainnya FLOAT
        );
        """
    )

    task_insert = PythonOperator(
        task_id="insert_csv",
        python_callable=insert_csv
    )

    task_clean = SQLExecuteQueryOperator(
        task_id="clean_tables",
        conn_id="Sampah_jatim",
        sql="""
        -- 1. CLEAN sisa_makanan
        UPDATE komposisi_sampah
        SET sisa_makanan = subquery.avg_value
        FROM (
            SELECT AVG(sisa_makanan) AS avg_value
            FROM komposisi_sampah
            WHERE sisa_makanan IS NOT NULL
        ) AS subquery
        WHERE sisa_makanan IS NULL;

        -- 2. CLEAN kayu_ranting
        UPDATE komposisi_sampah
        SET kayu_ranting = subquery.avg_value
        FROM (
            SELECT AVG(kayu_ranting) AS avg_value
            FROM komposisi_sampah
            WHERE kayu_ranting IS NOT NULL
        ) AS subquery
        WHERE kayu_ranting IS NULL;
        
        -- 3. CLEAN kertas_karton
        UPDATE komposisi_sampah
        SET kertas_karton = subquery.avg_value
        FROM (
            SELECT AVG(kertas_karton) AS avg_value
            FROM komposisi_sampah
            WHERE kertas_karton IS NOT NULL
        ) AS subquery
        WHERE kertas_karton IS NULL;
        
        -- 4. CLEAN plastik
        UPDATE komposisi_sampah
        SET plastik = subquery.avg_value
        FROM (
            SELECT AVG(plastik) AS avg_value
            FROM komposisi_sampah
            WHERE plastik IS NOT NULL
        ) AS subquery
        WHERE plastik IS NULL;

        -- 5. CLEAN logam
        UPDATE komposisi_sampah
        SET logam = subquery.avg_value
        FROM (
            SELECT AVG(logam) AS avg_value
            FROM komposisi_sampah
            WHERE logam IS NOT NULL
        ) AS subquery
        WHERE logam IS NULL;

        -- 6. CLEAN kain
        UPDATE komposisi_sampah
        SET kain = subquery.avg_value
        FROM (
            SELECT AVG(kain) AS avg_value
            FROM komposisi_sampah
            WHERE kain IS NOT NULL
        ) AS subquery
        WHERE kain IS NULL;

        -- 7. CLEAN karet_kulit
        UPDATE komposisi_sampah
        SET karet_kulit = subquery.avg_value
        FROM (
            SELECT AVG(karet_kulit) AS avg_value
            FROM komposisi_sampah
            WHERE karet_kulit IS NOT NULL
        ) AS subquery
        WHERE karet_kulit IS NULL;

        -- 8. CLEAN kaca
        UPDATE komposisi_sampah
        SET kaca = subquery.avg_value
        FROM (
            SELECT AVG(kaca) AS avg_value
            FROM komposisi_sampah
            WHERE kaca IS NOT NULL
        ) AS subquery
        WHERE kaca IS NULL;

        -- 9. CLEAN lainnya
        UPDATE komposisi_sampah
        SET lainnya = subquery.avg_value
        FROM (
            SELECT AVG(lainnya) AS avg_value
            FROM komposisi_sampah
            WHERE lainnya IS NOT NULL
        ) AS subquery
        WHERE lainnya IS NULL;
    """
    )

    task_join = SQLExecuteQueryOperator(
    task_id="join_tables",
    conn_id="Sampah_jatim",
    sql="""
    DROP TABLE IF EXISTS sampah_joined;

    CREATE TABLE sampah_joined AS
    SELECT 
        t.Tahun,
        t.Provinsi,
        t.Kabupaten_Kota,
        t.Timbulan_sampah_harian,
        t.Timbulan_sampah_tahunan,
        
        -- 🚨 CHANGE: Use lowercase column names without quotes
        k.sisa_makanan,
        k.kayu_ranting,
        k.kertas_karton,
        k.plastik,
        k.logam,
        k.kain,
        k.karet_kulit,
        k.kaca,
        k.lainnya
        
    FROM timbulan_sampah t
    LEFT JOIN komposisi_sampah k 
        ON t.Kabupaten_Kota = k.Kabupaten_Kota
        AND t.Tahun = k.Tahun;
    """
)

    # FLOW
    task_download_timbulan >> task_download_komposisi \
        >> task_convert_timbulan >> task_convert_komposisi \
        >> task_create_tables >> task_insert >> task_clean >> task_join
