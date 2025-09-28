import requests
from bs4 import BeautifulSoup
import pycountry
import csv
import json

# TODO: pycountry can't find all language codes, maybe check with the wals or ethnologue data

# URL of the ISO 639-3 table
iso_639_3_url = "https://iso639-3.sil.org/sites/iso639-3/files/downloads/iso-639-3.tab"

# Download and save the table
response = requests.get(iso_639_3_url)
with open("./data/iso-639-3.tab", "wb") as f:
    f.write(response.content)

print("ISO 639-3 table downloaded and saved as 'iso-639-3.tab'")


# Load ISO 639-3 data from the iso-639-3.tab file
def load_iso_639_3_table(file_path="./data/iso-639-3.tab"):
    iso_639_3_codes = set()
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            iso_639_3_codes.add(row["Id"])  # Add the ISO 639-3 code (ID column)
    return iso_639_3_codes


# Load the ISO 639-3 code set
iso_639_3_codes = load_iso_639_3_table()


def get_iso_code(code, name):
    # First, try using ISO 639-1 alpha-2 code if available
    try:
        lang = pycountry.languages.get(alpha_2=code)
        if lang and lang.alpha_3:
            return lang.alpha_3
    except LookupError:
        pass

    # If alpha-2 lookup fails, try looking up by language name
    try:
        lang = pycountry.languages.lookup(name)
        return lang.alpha_3
    except LookupError:
        pass

    # If previous checks fail, directly check if the 3-letter code exists in ISO 639-3
    if code in iso_639_3_codes:
        return code  # Map directly if it exists in ISO 639-3

    return "Unknown"


# URL of the Wikipedia page containing the table
url = "https://meta.wikimedia.org/wiki/List_of_Wikipedias"

# Fetch the page content
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/117.0.0.0 Safari/537.36"
}

response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.content, "html.parser")

# Find the first table on the page
table = soup.find("table")

# Check if the table was found
if not table:
    print("Table not found on the page.")
    exit()

# Get all the rows in the table
rows = table.find_all("tr")

# Create the dictionary to map wiki codes to ISO codes
wiki_code_to_iso_code = {}

# Open the output file with UTF-8 encoding
with open("./data/iso_codes_of_wikis.txt", "w", encoding="utf-8") as f:
    # Iterate over the rows, skipping the header rows
    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if len(cells) >= 7:
            # Extract the language code from the fourth cell
            code_cell = cells[3]
            code_link = code_cell.find("a")
            if code_link and code_link.text.strip():
                code = code_link.text.strip()
            else:
                code = code_cell.text.strip()

            # Extract the English language name from the third cell
            language_name = cells[2].text.strip()

            # Get the ISO code using the main and fallback methods
            iso_code = get_iso_code(code, language_name)

            # Add the mapping to the dictionary
            wiki_code_to_iso_code[code] = iso_code

            # Write the output to the file
            f.write(f"{language_name} ({code}): {iso_code}\n")

# Save the dictionary
with open("./data/wiki_code_to_iso_code.json", "w", encoding="utf-8") as json_file:
    json.dump(wiki_code_to_iso_code, json_file, ensure_ascii=False, indent=4)
