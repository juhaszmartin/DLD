import os
import requests
from bs4 import BeautifulSoup
import json
import pycountry

# NOTE maybe check how many rows in the incubator page and how many 1-s in the output


# Load the language codes from the .txt file
output_folder = "./data"
file_path = os.path.join(output_folder, "ethnologue_code_list.txt")
with open(file_path, "r", encoding="utf-8") as f:
    language_codes = [line.strip() for line in f.readlines()]

# Scrape the data from the URL
url = "https://incubator.wikimedia.org/wiki/Incubator:Wikis"
response = requests.get(url)
soup = BeautifulSoup(response.content, "html.parser")

# Find the table with class "wikitable plainlinks attable"
table = soup.find("table", {"class": "wikitable plainlinks attable"})

# Extract the 3-letter language codes and names from the table
scraped_data = []
for row in table.find_all("tr"):
    lang_attr = row.find("td", {"lang": True})
    if lang_attr:
        lang_code = lang_attr.get("lang")
        lang_name_tag = row.find("a", {"class": "extiw"})
        if lang_name_tag:
            lang_name = lang_name_tag.find("b").text.strip() if lang_name_tag.find("b") else lang_name_tag.text.strip()
            scraped_data.append((lang_code, lang_name))

# Create the dictionary and check for mismatches
language_dict = {code: 0 for code in language_codes}  # Initialize all codes to 0

for scraped_code, scraped_name in scraped_data:
    # Use pycountry to find the correct ISO 639-3 code for the scraped name
    language = pycountry.languages.get(name=scraped_name)
    if language:
        correct_code = language.alpha_3
        # If the scraped code is 2 letters, ignore it and use the correct 3-letter code
        if len(scraped_code) == 2:
            print(f"Ignoring 2-letter code '{scraped_code}' for '{scraped_name}'. Using correct ISO 639-3 code '{correct_code}'.")
        # Compare the scraped code with the correct code (only if scraped code is 3 letters)
        elif len(scraped_code) == 3 and scraped_code != correct_code:
            print(f"Warning: Mismatch detected! Scraped code '{scraped_code}' ({scraped_name}) should be '{correct_code}'.")
        # Update the dictionary value to 1 if the correct code is in the Ethnologue list
        if correct_code in language_codes:
            language_dict[correct_code] = 1
    else:
        print(f"Warning: No ISO 639-3 code found for '{scraped_name}'.")

# Save the dictionary as WPincubatornew.JSON in the /data folder
output_file_path = os.path.join(output_folder, "WPincubatornew.json")
with open(output_file_path, "w", encoding="utf-8") as json_file:
    json.dump(language_dict, json_file, ensure_ascii=False, indent=4)

print(f"Dictionary saved to {output_file_path}")
