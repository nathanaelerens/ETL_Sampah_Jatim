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


# ======================================================================
# Selenium Helper
# ======================================================================

def wait_for_download(download_path, timeout=60):
    """Wait until a new .xlsx file appears and is fully downloaded."""
    start = time.time()
    while time.time() - start < timeout:
        files = glob.glob(os.path.join(download_path, "*.xlsx"))
        if files:
            # Ensure file is not still writing
            if all(not f.endswith(".crdownload") for f in files):
                return files
        time.sleep(1)
    raise TimeoutError("Download did not finish within timeout.")


# ======================================================================
# 1. Selenium Downloader (Reusable)
# ======================================================================

def download_sipsn(url):
    download_dir = "/opt/airflow/downloads"

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

        # ======================================================
        # HANDLE NATIVE <select> DROPDOWNS
        # ======================================================

        # --- Select Tahun = 2024 ---
        from selenium.webdriver.support.ui import Select
        select_tahun = Select(driver.find_element(By.ID, "filter_id_tahun"))
        select_tahun.select_by_visible_text("2024")

        time.sleep(1)

        # --- Select Provinsi = Jawa Timur ---
        select_prov = Select(driver.find_element(By.ID, "filter_id_propinsi"))
        select_prov.select_by_visible_text("Jawa Timur")

        time.sleep(1)

        # --- Select Kabupaten/Kota = All ---
        select_kabkot = Select(driver.find_element(By.ID, "filter_id_kabkot"))
        select_kabkot.select_by_visible_text("Semua Kabupaten/Kota")

        time.sleep(1)

        # ======================================================
        # CLICK EXCEL DOWNLOAD BUTTON
        # ======================================================

        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.dropdown-toggle"))).click()
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.buttons-excel"))).click()

        # Wait for download to finish
        wait_for_download(download_dir)

    finally:
        driver.quit()



def download_timbulan():
    download_sipsn("https://sipsn.kemenlh.go.id/sipsn/public/data/timbulan")


def download_komposisi():
    download_sipsn("https://sipsn.kemenlh.go.id/sipsn/public/data/komposisi")


# ======================================================================
# 2. Convert Excel → Clean CSV
# ======================================================================

def convert_timbulan():
    files = glob.glob("/opt/airflow/downloads/*Timbulan*.xlsx")
    if not files:
        raise ValueError("No Timbulan files downloaded!")

    df = pd.read_excel(files[-1])
    df = df[df["Tahun"].astype(str).str.isdigit()]
    df.to_csv("/opt/airflow/csv/timbulan.csv", index=False)


def convert_komposisi():
    files = glob.glob("/opt/airflow/downloads/*Komposisi*.xlsx")
    if not files:
        raise ValueError("No Komposisi files downloaded!")

    df = pd.read_excel(files[-1])
    df = df[df["Tahun"].astype(str).str.isdigit()]
    df.to_csv("/opt/airflow/csv/komposisi.csv", index=False)


# ======================================================================
# 3. Insert CSV into PostgreSQL
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
# DAG Definition
# ======================================================================

default_args = {
    "owner": "nathan",
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="pipeline_sampah_jatim_final",
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

    task_join = SQLExecuteQueryOperator(
        task_id="join_tables",
        conn_id="Sampah_jatim",
        sql="""
        CREATE TABLE IF NOT EXISTS sampah_joined AS
        SELECT *
        FROM timbulan_sampah t
        LEFT JOIN komposisi_sampah k USING (Kabupaten_Kota);
        """
    )

    # DAG FLOW
    task_download_timbulan >> task_download_komposisi \
        >> task_convert_timbulan >> task_convert_komposisi \
        >> task_create_tables >> task_insert >> task_join
