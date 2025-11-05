import os
import requests
from bs4 import BeautifulSoup
import json
import pycountry
import pandas as pd
import jellyfish
from itertools import permutations

def max_word_order_similarity(name1, name2):
    """Compute max Jaro similarity allowing only full-word order swaps."""
    words1 = name1.replace(",", "").split()
    words2 = name2.replace(",", "").split()
    max_sim = 0.0

    for perm1 in set(permutations(words1)):
        perm1_str = " ".join(perm1)
        sim = jellyfish.jaro_similarity(perm1_str, name2)
        max_sim = max(max_sim, sim)
        if max_sim >= 0.95:  # early exit for near-perfect match
            break

    for perm2 in set(permutations(words2)):
        perm2_str = " ".join(perm2)
        sim = jellyfish.jaro_similarity(name1, perm2_str)
        max_sim = max(max_sim, sim)
        if max_sim >= 0.95:
            break

    return max_sim

# === CONFIG ===
output_folder = "./data"
os.makedirs(output_folder, exist_ok=True)
ethnologue_file = os.path.join(output_folder, "ethnologue_code_list.txt")

# === Load language codes ===
with open(ethnologue_file, "r", encoding="utf-8") as f:
    language_codes = [line.strip() for line in f.readlines()]


# === For checking language names to scraped codes ===
ethnologue_csv = os.path.join(output_folder, "ethnologue_language_data.csv")
ethno_df = pd.read_csv(ethnologue_csv, encoding="utf-8")

# Normalize columns
ethno_df["ISO Code"] = ethno_df["ISO Code"].str.strip()
ethno_df["Language Name"] = ethno_df["Language Name"].str.strip()

# === Scrape the Incubator Wikis page ===
url = "https://incubator.wikimedia.org/wiki/Incubator:Wikis"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.content, "html.parser")

# Find the main table (more flexible class search)
table = soup.find("table", {"class": lambda c: c and "wikitable" in c})
if not table:
    raise RuntimeError("Could not find the Wikitable. The page structure may have changed.")

# === Extract only Wikipedia rows ===
scraped_data = []
for row in table.find_all("tr"):
    first_td = row.find("td")
    if not first_td:
        continue

    # Only include rows that start with "Wikipedia"
    if "Wikipedia" not in first_td.get_text():
        continue

    # Extract language name (from <a class="extiw">)
    lang_name_tag = first_td.find("a", class_="extiw")
    lang_name = None
    if lang_name_tag:
        lang_name = lang_name_tag.find("b").text.strip() if lang_name_tag.find("b") else lang_name_tag.text.strip()

    # Extract incubator language code (e.g., wp/aa -> aa)
    incubator_link = first_td.find("a", href=lambda x: x and "Wp/" in x)
    if incubator_link and incubator_link["href"]:
        lang_code = incubator_link["href"].split("/")[-1].strip()
    else:
        lang_code = None

    if lang_name and lang_code:
        scraped_data.append((lang_code, lang_name))

print(f"✅ Found {len(scraped_data)} Wikipedia test wikis.")

# === Build dictionary ===
language_dict = {code: 0 for code in language_codes}

for scraped_code, scraped_name in scraped_data:
    # Try to find ISO 639-3 code using pycountry
    language = pycountry.languages.get(name=scraped_name)
    if language:
        correct_code = language.alpha_3
        # Ignore 2-letter scraped codes, prefer correct ISO 639-3
        if len(scraped_code) == 2:
            print(f"Ignoring 2-letter code '{scraped_code}' for '{scraped_name}', using '{correct_code}'.")
        elif len(scraped_code) == 3 and scraped_code != correct_code:
            print(f"Warning: mismatch '{scraped_code}' ({scraped_name}) should be '{correct_code}'.")
        if len(scraped_code) == 3 and scraped_code in language_codes:
            ethno_match = ethno_df[ethno_df["ISO Code"] == scraped_code]
            if not ethno_match.empty:
                ethno_name = ethno_match.iloc[0]["Language Name"]
                similarity = max_word_order_similarity(scraped_name, ethno_name)
                if similarity >= 0.90:
                    print(f"✅ {scraped_code} likely correct (name similarity {similarity:.2f}) — '{scraped_name}' ≈ '{ethno_name}'")
                else:
                    print(f"⚠️ {scraped_code} mismatch (name similarity {similarity:.2f}) — scraped '{scraped_name}' vs Ethnologue '{ethno_name}'")
            else:
                print(f"❌ {scraped_code} not found in Ethnologue dataset.")


            language_dict[scraped_code] = 1

        if correct_code in language_codes:
            language_dict[correct_code] = 1
    else:
        print(f"Warning: No ISO 639-3 code found for '{scraped_name}'.")

# === Save JSON ===
output_file = os.path.join(output_folder, "WPincubatornew.json")
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(language_dict, f, ensure_ascii=False, indent=4)

print(f"✅ Dictionary saved to {output_file}")
