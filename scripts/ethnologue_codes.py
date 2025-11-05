from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
import time
import os

# ========================
# Configuration
# ========================
output_file_path = "./data/ethnologue_code_list.txt"
EDGEDRIVER_PATH = r"C:\webdriver\msedgedriver.exe"
EDGE_BINARY_PATH = r"C:\Program Files (x86)\Microsoft/Edge/Application\msedge.exe"

os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

# ========================
# Start WebDriver
# ========================
service = Service(EDGEDRIVER_PATH)
options = webdriver.EdgeOptions()
options.binary_location = EDGE_BINARY_PATH
# options.add_argument("--headless")  # uncomment if you don’t need a browser window

driver = webdriver.Edge(service=service, options=options)
driver.get("https://www.ethnologue.com/browse/codes/")
time.sleep(3)  # allow main page to load

codes_list = []

# ========================
# Loop through all A–Z letters
# ========================
for letter_index in range(1, 27):  # A–Z
    letter = chr(64 + letter_index)
    print(f"Processing letter {letter}...")

    try:
        # Click the letter tab (A=1, B=2, etc.)
        letter_xpath = f"/html/body/main/div/div/div/div[3]/div/nav/div/ul/li[{letter_index}]/a"
        driver.find_element(By.XPATH, letter_xpath).click()
        time.sleep(2)  # let content load

        # Table container (starts at div[4] for A, 5 for B, etc.)
        table_xpath = f"/html/body/main/div/div/div/div[{letter_index + 3}]"

        # There are multiple columns per letter
        for col in range(1, 9):
            try:
                column_xpath = f"{table_xpath}/div[{col}]"
                column = driver.find_element(By.XPATH, column_xpath)

                code_elements = column.find_elements(By.TAG_NAME, "a")
                codes = [code.text.strip() for code in code_elements if code.text.strip()]
                codes_list.extend(codes)
            except Exception:
                # skip empty/missing columns
                continue

        print(f"✓ Found {len([c for c in codes_list if c.startswith(letter.lower())])} codes for {letter}")

    except Exception as e:
        print(f"⚠️ Error processing letter {letter}: {e}")

# ========================
# Save results
# ========================
with open(output_file_path, "w", encoding="utf-8") as f:
    for code in codes_list:
        f.write(code + "\n")

print(f"\n✅ Scraping completed. {len(codes_list)} codes saved to {output_file_path}")

driver.quit()
