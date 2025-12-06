from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import os

# --- Configure download folder ---
download_dir = os.path.join(os.getcwd(), "downloads")

chrome_options = Options()
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

driver = webdriver.Chrome(options=chrome_options)

url = "https://sipsn.kemenlh.go.id/sipsn/public/data/timbulan"
driver.get(url)

time.sleep(2)   # wait for table to load

# 1. Click the Tools dropdown
tools_btn = driver.find_element(By.CSS_SELECTOR, "button.dropdown-toggle")
tools_btn.click()

time.sleep(1)

# 2. Click the Excel download button
excel_btn = driver.find_element(By.CSS_SELECTOR, "a.buttons-excel")
excel_btn.click()

# wait for download
time.sleep(5)

driver.quit()

print("Downloaded files in:", download_dir)
