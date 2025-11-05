import pandas as pd
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time

update_existing = True

# ---------- CONFIG ----------
EDGEDRIVER_PATH = r"C:\webdriver\msedgedriver.exe"
EDGE_BINARY_PATH = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
output_folder = "./data"
file_path = os.path.join(output_folder, "ethnologue_code_list.txt")
csv_path = os.path.join(output_folder, "ethnologue_language_data.csv")
macro_csv_path = os.path.join(output_folder, "ethnologue_macrolanguages.csv")
base_url = "https://www.ethnologue.com/language/"
# ----------------------------

# --- Setup Edge ---
service = Service(EDGEDRIVER_PATH)
options = webdriver.EdgeOptions()
options.binary_location = EDGE_BINARY_PATH
driver = webdriver.Edge(service=service, options=options)

# --- Load language codes ---
with open(file_path, "r", encoding="utf-8") as f:
    language_codes = [line.strip() for line in f.readlines()]

# --- Initialize dataframe in correct column order ---
df_existing = pd.DataFrame(columns=[
    "ISO Code",
    "Language Name",
    "Summary",
    "Population Size",
    "Institutional (%)",
    "Stable (%)",
    "Endangered (%)",
    "Extinct (%)",
    "Digital Support",
])

existing_codes = set()

# --- Load existing data ---
if update_existing:
    try:
        # Load base language data (prevent interpreting 'nan' as missing)
        if os.path.exists(csv_path):
            df_original = pd.read_csv(csv_path, dtype=str, keep_default_na=False, na_values=[])
            # Ensure ISO Code column is string
            df_original["ISO Code"] = df_original["ISO Code"].astype(str)
            existing_codes.update(df_original["ISO Code"].dropna().astype(str).str.strip().tolist())
            df_existing = pd.concat([df_existing, df_original], ignore_index=True)

        # Load macrolanguage data (prevent interpreting 'nan' as missing)
        if os.path.exists(macro_csv_path):
            df_macro_existing = pd.read_csv(macro_csv_path, dtype=str, keep_default_na=False, na_values=[])
            df_macro_existing["ISO Code"] = df_macro_existing["ISO Code"].astype(str)
            existing_codes.update(df_macro_existing["ISO Code"].dropna().astype(str).str.strip().tolist())

        # Remove duplicates and filter remaining codes
        df_existing = df_existing.drop_duplicates(subset=["ISO Code"], keep="last").reset_index(drop=True)
        language_codes = [c for c in language_codes if c not in existing_codes]

        print(f"Loaded {len(existing_codes)} existing codes (original + macrolanguages); {len(language_codes)} codes remaining to scrape.")
    except Exception as e:
        print(f"Error reading existing CSVs: {e}")
else:
    print("update_existing is False; scraping all codes.")

# --- Containers ---
data_macro = []
updated_rows = 0

# --- Main scraping loop ---
for code in language_codes:
    time.sleep(1)
    try:
        url = base_url + code
        driver.get(url)

        # Wait for summary section
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='summary']/p"))
        )
        summary = driver.find_element(By.XPATH, "//*[@id='summary']/p").text.strip()

        # --- MACROLANGUAGE DETECTION ---
        if any(q in summary for q in ['"macrolanguage"', '“macrolanguage”']):
            print(f"'{code}' is a macrolanguage — extracting member languages...")

            macro_langs = [
                e.text.strip() for e in driver.find_elements(
                    By.XPATH, "//*[@id='typology']//ul/li/a/span"
                ) if e.text.strip()
            ]

            if macro_langs:
                print(f"  → Found {len(macro_langs)} member languages: {macro_langs}")
                lang_list = " ".join(macro_langs)
            else:
                print(f"  → No member languages found for {code}")
                lang_list = ""  # empty second column

            # ensure ISO code stored as string literal
            data_macro.append({
                "ISO Code": str(code),
                "Languages in the macro": lang_list
            })
            continue  # skip regular scrape

        # --- REGULAR LANGUAGE SCRAPE ---
        print(f"{code} is not a macrolanguage — scraping standard fields.")

        try:
            language_name = driver.find_element(By.XPATH, "//h1").text.strip()
        except:
            language_name = ""

        try:
            summary_text = driver.find_element(By.XPATH, "//*[@id='summary']/p").text.strip()
        except:
            summary_text = ""

        try:
            population_size = driver.find_element(
                By.XPATH, "/html/body/main/article/section[2]/ul/li[2]/article/div[1]/div/div[1]/div"
            ).text.strip()
        except:
            population_size = ""

        # Vitality distribution
        vitality_data = {"Institutional": "0%", "Stable": "0%", "Endangered": "0%", "Extinct": "0%"}
        try:
            vitality_ol = driver.find_element(By.XPATH, "//ol[@class='histogram-vitality']")
            for li in vitality_ol.find_elements(By.XPATH, ".//li[@class='histogram__datum tooltip']"):
                label = li.find_element(By.XPATH, ".//div[@class='histogram__datum-label']/label").text.strip()
                bar = li.find_element(By.XPATH, ".//div[@class='histogram__bar']").get_attribute("style")
                if "--bar-width" in bar:
                    vitality_data[label] = bar.split("--bar-width:")[1].split(";")[0].strip()
        except:
            pass

        # Digital Support indicator
        try:
            img_src = driver.find_element(
                By.XPATH, "/html/body/main/article/section[2]/ul/li[4]/article/p/img"
            ).get_attribute("src")
            digital_support = img_src.split("/")[-1].split(".")[0]
        except:
            digital_support = ""

        # Force ISO code to string explicitly (this prevents pandas from making it NaN)
        code_str = str(code)

        new_row = pd.DataFrame([{
            "ISO Code": code_str,
            "Language Name": language_name,
            "Summary": summary_text,
            "Population Size": population_size,
            "Institutional (%)": vitality_data.get("Institutional", "0%"),
            "Stable (%)": vitality_data.get("Stable", "0%"),
            "Endangered (%)": vitality_data.get("Endangered", "0%"),
            "Extinct (%)": vitality_data.get("Extinct", "0%"),
            "Digital Support": digital_support,
        }])

        df_existing = pd.concat([df_existing, new_row], ignore_index=True)

        # Make sure ISO Code column is string for all rows (converts any internal NaN -> 'nan' string)
        df_existing["ISO Code"] = df_existing["ISO Code"].astype(str)

        df_existing = df_existing.drop_duplicates(subset=["ISO Code"], keep="last")
        df_existing = df_existing.sort_values(by=["ISO Code"], ignore_index=True)
        updated_rows += 1

        print(f"  → Added '{language_name}' ({code_str}) successfully.")

        time.sleep(0.5)

    except Exception as e:
        print(f"Error scraping data for {code}: {e}")

driver.quit()

# --- SAVE FINAL LANGUAGE DATA (overwrite original) ---
if updated_rows > 0:
    # Ensure ISO Code column contains strings (and not NaN)
    df_existing["ISO Code"] = df_existing["ISO Code"].astype(str)
    df_existing.to_csv(csv_path, index=False)
    print(f"\n✅ Updated and overwritten {csv_path}")
else:
    print("\nNo updates were made to the language data.")

# --- SAVE MACROLANGUAGE DATA ---
if data_macro:
    df_macro = pd.DataFrame(data_macro, columns=["ISO Code", "Languages in the macro"])
    if os.path.exists(macro_csv_path):
        df_existing_macro = pd.read_csv(macro_csv_path, dtype=str, keep_default_na=False, na_values=[])
        df_macro = pd.concat([df_existing_macro, df_macro], ignore_index=True)
        df_macro = df_macro.drop_duplicates(subset=["ISO Code"], keep="last")
    df_macro.to_csv(macro_csv_path, index=False)
    print(f"✅ Macrolanguage data saved to {macro_csv_path}")
else:
    print("No macrolanguages found in this run.")

# --- SUMMARY ---
print(f"\nSummary:")
print(f"  • New/updated regular languages: {updated_rows}")
print(f"  • New macrolanguages: {len(data_macro)}")
print(f"  • Total ISO codes skipped (already scraped): {len(existing_codes)}")
