import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

EDGEDRIVER_PATH = "C:/webdriver/edgedriver_win64/msedgedriver.exe"
EDGE_BINARY_PATH = "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe"

service = Service(EDGEDRIVER_PATH)
options = webdriver.EdgeOptions()
options.binary_location = EDGE_BINARY_PATH

driver = webdriver.Edge(service=service, options=options)
driver.get("https://www.jw.org/en/")
time.sleep(3)

# Click the language button
lang_button = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.jsChooseSiteLanguage")))
driver.execute_script("arguments[0].click();", lang_button)

# Wait for the list
WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.hasWebContent")))
time.sleep(5)

# Iterate through each <li> including downloadOnly
rows = driver.find_elements(By.CSS_SELECTOR, "li.hasWebContent, li.downloadOnly")

languages = []
for row in rows:
    try:
        eng = row.find_element(By.CSS_SELECTOR, "div.altLabel").text.strip()
    except:
        eng = ""  # sometimes missing
    try:
        native = row.find_element(By.CSS_SELECTOR, "div.optionLabel").text.strip()
    except:
        native = ""  # sometimes missing
    if eng or native:
        languages.append({"English Name": eng, "Local Name": native})
    else:
        # For downloadOnly items with no visible text
        languages.append({"English Name": "[Download Only]", "Local Name": ""})

# Create a DataFrame
df = pd.DataFrame(languages)

# Print DataFrame
print(df)

# Optional: save to CSV
df.to_csv("./data/jw_languages.csv", index=False)

print(f"\nTotal pairs found: {len(df)}")

driver.quit()
