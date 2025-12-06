from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
import time
import os
import glob
import pandas as pd

# ======================================================================
# Helper: tunggu file Excel selesai di-download (same as in DAG)
# ======================================================================

def wait_for_download(download_path, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        # Look for *.xlsx files
        files = glob.glob(os.path.join(download_path, "*.xlsx"))
        if files:
            # Ensure the download is complete (no .crdownload extension)
            if all(not f.endswith(".crdownload") for f in files):
                return files
        time.sleep(1)
    raise TimeoutError("Download tidak selesai dalam waktu yang ditentukan.")


# ======================================================================
# Local Test Function
# ======================================================================

def download_sipsn_local(url):
    # 🚨 HARDCODED LOCAL PATH: Use a simple local directory
    # NOTE: You must create this directory before running the script.
    download_dir = os.path.join(os.getcwd(), "test_downloads")
    
    # Ensure the directory exists
    os.makedirs(download_dir, exist_ok=True)
    print(f"Downloads will go to: {download_dir}")
    
    # -----------------------------------------------------------
    # 🚨 LOCAL SETUP NOTES: 
    # 1. If running outside Docker, you usually DON'T need binary_location.
    # 2. You might need to change the service path if chromedriver is not in your PATH.
    # 3. We remove --headless=new so you can watch the browser action.
    # -----------------------------------------------------------

    chrome_options = Options()
    # chrome_options.binary_location = "/usr/bin/chromium" # Remove for local test
    # chrome_options.add_argument("--headless=new") # Comment out to see the browser
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")

    # Set Chrome preferences for download path
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    # Initialize the WebDriver
    # NOTE: If you get an error here, you might need to install/update chromedriver
    driver = webdriver.Chrome(options=chrome_options)

    wait = WebDriverWait(driver, 40)

    try:
        driver.get(url)
        
        # 🚨 FIX: Explicitly wait for the Province dropdown to be present
        wait.until(EC.presence_of_element_located((By.ID, "filter_id_propinsi")))
        print("Page loaded successfully.")
        
        # ==================================
        # PILIH JUMLAH ENTRI ("SEMUA")
        # ==================================
        select_entries_element = driver.find_element(By.NAME, "tabeldata_length")
        select_entries = Select(select_entries_element)
        select_entries.select_by_value("-1")
        print("Selected 'SEMUA' entries.")
        
        time.sleep(5) # Wait for the table data to fully load

        # ========================
        # PILIH PROVINSI (HANYA INI YANG DIGUNAKAN)
        # ========================
        select_prov = Select(driver.find_element(By.ID, "filter_id_propinsi"))
        select_prov.select_by_visible_text("Jawa Timur")
        time.sleep(1)
        print("Selected Jawa Timur.")

        # ========================
        # KLIK TOMBOL DOWNLOAD EXCEL
        # ========================
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.dropdown-toggle"))).click()
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.buttons-excel"))).click()
        print("Clicked download button.")

        # Tunggu file selesai download
        files = wait_for_download(download_dir)
        print(f"Download complete: {files[-1]}")

    finally:
        driver.quit()
        print("Browser closed.")


if __name__ == "__main__":
    # Clean up old download directory before running
    download_dir = os.path.join(os.getcwd(), "test_downloads")
    if os.path.exists(download_dir):
        import shutil
        shutil.rmtree(download_dir)

    # Test the Timbulan URL
    print("--- Starting Timbulan Download Test ---")
    try:
        download_sipsn_local("https://sipsn.kemenlh.go.id/sipsn/public/data/timbulan")
    except Exception as e:
        print(f"Timbulan Test FAILED: {e}")
    
    # Optional: Test the Komposisi URL
    print("\n--- Starting Komposisi Download Test ---")
    try:
        download_sipsn_local("https://sipsn.kemenlh.go.id/sipsn/public/data/komposisi")
    except Exception as e:
        print(f"Komposisi Test FAILED: {e}")