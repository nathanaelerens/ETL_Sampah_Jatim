from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pandas as pd
import glob
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os

def insert_csv():
    hook = PostgresHook(postgres_conn_id='Sampah_jatim')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE timbulan_sampah;")
    cursor.execute("TRUNCATE TABLE komposisi_sampah;")

    with open('/opt/airflow/csv/timbulan_sampah_jatim.csv', 'r') as f:
        cursor.copy_expert(
            "COPY timbulan_sampah FROM STDIN WITH CSV HEADER",
            f
        )

    with open('/opt/airflow/csv/komposisi_sampah_jatim.csv', 'r') as f:
        cursor.copy_expert(
            "COPY komposisi_sampah FROM STDIN WITH CSV HEADER",
            f
        )

    conn.commit()


def convert_excel_to_csv():
    import os
    
    files = glob.glob("/opt/airflow/downloads/*[Tt]imbulan*.xlsx")
    if not files:
        raise ValueError("No timbulan Excel files found.")

    latest = max(files, key=lambda f: os.path.getmtime(f))

    # Read Excel normally
    df = pd.read_excel(latest)

    # The real header starts at row index 1 → drop row 0
    df.columns = df.iloc[1]     # row index 1 contains the correct header
    df = df[2:]                 # remove the first two rows (metadata + header row)

    # Clean column names (strip spaces)
    df.columns = [str(c).strip().replace(" ", "_") for c in df.columns]

    df.to_csv("/opt/airflow/csv/timbulan_sampah_jatim.csv", index=False)



def convert_komposisi_excel_to_csv():
    import os
    
    files = glob.glob("/opt/airflow/downloads/*[Kk]omposisi*.xlsx")
    if not files:
        raise ValueError("No komposisi Excel files found.")

    latest = max(files, key=lambda f: os.path.getmtime(f))

    df = pd.read_excel(latest)

    df.columns = df.iloc[1]   # header is row 1
    df = df[2:]               # remove extra rows

    df.columns = [str(c).strip().replace(" ", "_") for c in df.columns]

    df.to_csv("/opt/airflow/csv/komposisi_sampah_jatim.csv", index=False)



def download_file():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import time
    import os

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

    service = Service("/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)

    wait = WebDriverWait(driver, 30)

    try:
        url = "https://sipsn.kemenlh.go.id/sipsn/public/data/timbulan"
        driver.get(url)

        # Wait for JS
        time.sleep(3)

        # ----------------------------------------------------------------------
        # SELECT TAHUN (Select2)
        # ----------------------------------------------------------------------
        tahun_dropdown = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#select2-filter_id_tahun-container"))
        )
        tahun_dropdown.click()

        option_2024 = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//li[contains(text(), '2024')]"))
        )
        option_2024.click()

        time.sleep(1)

        # ----------------------------------------------------------------------
        # SELECT PROVINSI = Jawa Timur (value = 35)
        # ----------------------------------------------------------------------
        prov_dropdown = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#select2-filter_id_propinsi-container"))
        )
        prov_dropdown.click()

        jatim_option = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//li[contains(text(), 'Jawa Timur')]"))
        )
        jatim_option.click()

        time.sleep(2)

        # ----------------------------------------------------------------------
        # SELECT KABUPATEN = ALL
        # ----------------------------------------------------------------------
        kab_dropdown = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#select2-filter_id_kabkot-container"))
        )
        kab_dropdown.click()

        kab_all = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//li[contains(text(), 'Semua Kabupaten/Kota')]"))
        )
        kab_all.click()

        time.sleep(2)

        # ----------------------------------------------------------------------
        # CLICK EXCEL DOWNLOAD
        # ----------------------------------------------------------------------
        tools_btn = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.dropdown-toggle"))
        )
        tools_btn.click()

        excel_btn = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a.buttons-excel"))
        )
        excel_btn.click()

        # wait for download
        time.sleep(8)

    except Exception as e:
        # Save debug screenshot
        screenshot_path = "/opt/airflow/debug_download_error.png"
        driver.save_screenshot(screenshot_path)
        print(f"DEBUG screenshot saved at: {screenshot_path}")
        driver.quit()
        raise e

    driver.quit()
    print("Download complete:", download_dir)



def download_file2():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.support.ui import Select, WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import time

    download_dir = "/opt/airflow/downloads"

    chrome_options = Options()
    chrome_options.binary_location = "/usr/bin/chromium"
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    service = Service("/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)

    wait = WebDriverWait(driver, 25)

    # Open URL
    driver.get("https://sipsn.kemenlh.go.id/sipsn/public/data/komposisi")

    # 1️⃣ WAIT for Tahun dropdown
    tahun_dropdown = wait.until(
        EC.presence_of_element_located((By.ID, "filter_id_tahun"))
    )
    Select(tahun_dropdown).select_by_visible_text("2024")

    # 2️⃣ WAIT for Provinsi dropdown
    prov_dropdown = wait.until(
        EC.presence_of_element_located((By.ID, "filter_id_propinsi"))
    )
    Select(prov_dropdown).select_by_value("35")   # Jawa Timur

    # 3️⃣ WAIT for Kabupaten dropdown to refresh
    kab_dropdown = wait.until(
        EC.presence_of_element_located((By.ID, "filter_id_kabkot"))
    )
    Select(kab_dropdown).select_by_index(0)

    # 4️⃣ WAIT for Tools button
    tools_btn = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "button.dropdown-toggle"))
    )
    tools_btn.click()

    # 5️⃣ WAIT for Excel button
    excel_btn = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "a.buttons-excel"))
    )
    excel_btn.click()

    # 6️⃣ WAIT for file to finish downloading
    time.sleep(6)

    driver.quit()
    print("Download complete:", download_dir)





my_default_args = {
    'owner': 'nathan', 
    'retries': 5, 
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='pipeline_data_sampah_jatim_postgres_v10',
    default_args=my_default_args,
    description='membuat dag sederhana',
    start_date=datetime(2025, 11, 26), 
    schedule='@daily'
) as dag:
    download_task = PythonOperator(
        task_id='download_file',
        python_callable= download_file)
    
    download_task2 = PythonOperator(
        task_id='download_file2',
        python_callable= download_file2)
    
    convert = PythonOperator(
        task_id='convert_excel_to_csv',
        python_callable= convert_excel_to_csv)
    
    convert_komposisi = PythonOperator(
    task_id='convert_komposisi_excel_to_csv',
    python_callable=convert_komposisi_excel_to_csv
)

    

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='Sampah_jatim',
        sql="""
        CREATE TABLE IF NOT EXISTS timbulan_sampah (
            
            Tahun INT,
            Provinsi VARCHAR(50),
            kabupaten_kota VARCHAR(50) PRIMARY KEY,
            Timbulan_sampah_harian FLOAT,
            Timbulan_sampah_tahunan FLOAT
            );
        
        CREATE TABLE IF NOT EXISTS komposisi_sampah (
            
            Tahun INT,
            Provinsi VARCHAR(50),
            kabupaten_kota VARCHAR(50) PRIMARY KEY,
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

    fill_table = PythonOperator(
    task_id='fill_table',
    python_callable=insert_csv
)
    clean_timbulan = SQLExecuteQueryOperator(
        task_id='clean_timbulan_table',
        conn_id='Sampah_jatim',
        sql="""
        UPDATE timbulan_sampah
        SET 
            Tahun = COALESCE(Tahun, 0),
            Timbulan_sampah_harian = COALESCE(Timbulan_sampah_harian, 0),
            Timbulan_sampah_tahunan = COALESCE(Timbulan_sampah_tahunan, 0)
        WHERE 
            Tahun IS NULL
            OR Timbulan_sampah_harian IS NULL
            OR Timbulan_sampah_tahunan IS NULL;
        """
    )

    clean_komposisi = SQLExecuteQueryOperator(
        task_id='clean_komposisi_table',
        conn_id='Sampah_jatim',
        sql="""
        UPDATE komposisi_sampah
        SET 
            Sisa_Makanan   = COALESCE(Sisa_Makanan, 0),
            Kayu_Ranting   = COALESCE(Kayu_Ranting, 0),
            Kertas_Karton  = COALESCE(Kertas_Karton, 0),
            Plastik        = COALESCE(Plastik, 0),
            Logam          = COALESCE(Logam, 0),
            Kain           = COALESCE(Kain, 0),
            Karet_Kulit    = COALESCE(Karet_Kulit, 0),
            Kaca           = COALESCE(Kaca, 0),
            Lainnya        = COALESCE(Lainnya, 0)
        WHERE 
            Sisa_Makanan IS NULL
            OR Kayu_Ranting IS NULL
            OR Kertas_Karton IS NULL
            OR Plastik IS NULL
            OR Logam IS NULL
            OR Kain IS NULL
            OR Karet_Kulit IS NULL
            OR Kaca IS NULL
            OR Lainnya IS NULL;
        """
    )
    join_tables = SQLExecuteQueryOperator(
    task_id='join_tables',
    conn_id='Sampah_jatim',
    sql="""
    DROP TABLE IF EXISTS sampah_joined;

    CREATE TABLE sampah_joined AS
    SELECT
        t.Tahun AS tahun_timbulan,
        t.Provinsi AS provinsi_timbulan,
        t.kabupaten_kota,
        t.Timbulan_sampah_harian,
        t.Timbulan_sampah_tahunan,
        k.Tahun AS tahun_komposisi,
        k.Provinsi AS provinsi_komposisi,
        k.Sisa_Makanan,
        k.Kayu_Ranting,
        k.Kertas_Karton,
        k.Plastik,
        k.Logam,
        k.Kain,
        k.Karet_Kulit,
        k.Kaca,
        k.Lainnya
    FROM timbulan_sampah t
    LEFT JOIN komposisi_sampah k
        ON t.kabupaten_kota = k.kabupaten_kota;
    """
)




    download_task >> download_task2 >> convert >> convert_komposisi >> create_table >> fill_table >> clean_timbulan >> clean_komposisi >> join_tables
