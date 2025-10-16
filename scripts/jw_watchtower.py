import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
import time
import random
import pandas as pd

# URL of the ISO 639-2 code list
url = "https://www.loc.gov/standards/iso639-2/php/code_list.php"

# Fetch the HTML content
response = requests.get(url)
response.raise_for_status()  # ensure the request succeeded

# Parse the HTML with BeautifulSoup
soup = BeautifulSoup(response.text, "html.parser")

# Find the main table
table = soup.find("table")

# Extract column headers
headers = [th.get_text(strip=True) for th in table.find_all("th")]

# Extract table rows
rows = []
for tr in table.find_all("tr")[1:]:  # skip header row
    cells = [td.get_text(strip=True) for td in tr.find_all("td")]
    if cells:
        rows.append(cells)

# Convert to DataFrame
df = pd.DataFrame(rows, columns=headers)

# Show first few rows
print(df.head())

# (Optional) Save to CSV
df.to_csv("./data/iso_639_1_2_codes.csv", index=False, encoding="utf-8")
print("✅ Saved as iso_639_1_2_codes.csv")



# ====== User settings ======
EDGE_DRIVER_PATH = "/usr/local/bin/msedgedriver"  # path to msedgedriver
WAIT_BETWEEN = (1, 2)  # random wait between requests

# ====== Selenium setup ======
options = Options()
# Run actual browser
# options.add_argument("--headless")  # <-- Do NOT use headless
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/118.0.5993.90 Safari/537.36 Edg/118.0.2088.61")

service = Service(EDGE_DRIVER_PATH)
driver = webdriver.Edge(service=service, options=options)

# ====== Function to check site ======
def check_site_selenium(code, iso1=None, iso2=None):
    url = f"https://wol.jw.org/{code}"
    visited_urls = []

    try:
        driver.get(url)
        time.sleep(random.uniform(1, 2))  # let page load
        visited_urls.append(driver.current_url)

        # Build possible valid prefixes
        valid_prefixes = [f"https://wol.jw.org/{code.lower()}/"]
        if iso1 and iso1.strip():
            valid_prefixes.append(f"https://wol.jw.org/{iso1.strip().lower()}/")
        if iso2 and iso2.strip():
            valid_prefixes.append(f"https://wol.jw.org/{iso2.strip().lower()}/")

        final_url = driver.current_url.lower()
        if any(final_url.startswith(v) for v in valid_prefixes):
            status = "Exists"
        elif "/en/" in final_url or final_url.rstrip("/") == "https://wol.jw.org/en":
            status = "Redirected to EN"
        else:
            status = f"Redirected to {final_url}"

        return status, visited_urls

    except Exception as e:
        return f"Error: {e}", visited_urls

# ====== Test ISO codes ======
def test_iso_codes_selenium(df):
    df = df.copy()
    df["JW_for_iso1_status"] = None
    # df["JW_for_iso1_urls"] = None
    df["JW_for_iso2_status"] = None
    # df["JW_for_iso2_urls"] = None

    for idx, row in df.iterrows():
        iso1 = str(row.get("ISO 639-1 Code", "")).strip()
        iso2 = str(row.get("ISO 639-2 Code", "")).strip()

        # Test 2-letter code
        if iso1 and iso1 != "-":
            status, urls = check_site_selenium(iso1, iso1=iso1, iso2=iso2)
            df.at[idx, "JW_for_iso1_status"] = status
            # df.at[idx, "JW_for_iso1_urls"] = " -> ".join(urls)
        else:
            df.at[idx, "JW_for_iso1_status"] = "N/A"
            # df.at[idx, "JW_for_iso1_urls"] = ""

        # Test 3-letter code
        if iso2 and iso2 != "-":
            status, urls = check_site_selenium(iso2, iso1=iso1, iso2=iso2)
            df.at[idx, "JW_for_iso2_status"] = status
           #  df.at[idx, "JW_for_iso2_urls"] = " -> ".join(urls)
        else:
            df.at[idx, "JW_for_iso2_status"] = "N/A"
            # df.at[idx, "JW_for_iso2_urls"] = ""

        time.sleep(random.uniform(*WAIT_BETWEEN))

    return df

# ====== Example usage ======
# df = pd.read_csv("iso_639_codes.csv", encoding="utf-8")
# df_cut = df.head(20)  # test first 20 rows
results_df = test_iso_codes_selenium(df)

results_df["Does_have_bible"] = results_df.apply(
    lambda row: 1 if ("Exists" in str(row["JW_for_iso1_status"]) or "Exists" in str(row["JW_for_iso2_status"])) else 0,
    axis=1
)

results_df.to_csv("./data/jw_availability_by_iso.csv", index=False, encoding="utf-8")
print("✅ Completed. See jw_availability_by_iso.csv")

# Close browser after completion
driver.quit()
