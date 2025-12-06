from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import time
import os

# --- Configure download folder ---
download_dir = os.path.join(os.getcwd(), "downloads")
os.makedirs(download_dir, exist_ok=True)

chrome_options = Options()
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

driver = webdriver.Chrome(options=chrome_options)
wait = WebDriverWait(driver, 20)

url = "https://sipsn.kemenlh.go.id/sipsn/public/data/timbulan"
driver.get(url)

# Wait for page load
time.sleep(2)

# =====================================================
# SELECT PROVINSI ONLY — DO NOT TOUCH TAHUN/KABUPATEN
# =====================================================

print("Waiting for provinsi dropdown...")

provinsi_dropdown = wait.until(
    EC.presence_of_element_located((By.ID, "filter_id_propinsi"))
)

provinsi = Select(provinsi_dropdown)

TARGET_PROVINSI = "Jawa Timur"
print("Selecting provinsi:", TARGET_PROVINSI)

provinsi.select_by_visible_text(TARGET_PROVINSI)

# Wait for the table to reload after selecting provinsi
time.sleep(2)

# =====================================================
# CLICK 'TOOLS' DROPDOWN
# =====================================================

print("Clicking Tools dropdown...")

tools_btn = wait.until(
    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.dropdown-toggle"))
)
tools_btn.click()

time.sleep(1)

# =====================================================
# CLICK EXCEL DOWNLOAD BUTTON
# =====================================================

print("Clicking Excel button...")

excel_btn = wait.until(
    EC.element_to_be_clickable((By.CSS_SELECTOR, "a.buttons-excel"))
)
excel_btn.click()

# Wait for download to complete
time.sleep(5)

driver.quit()

print("Downloaded files in:", download_dir)
