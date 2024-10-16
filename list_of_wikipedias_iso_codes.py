import requests
from bs4 import BeautifulSoup
import pycountry
import json


def get_iso_code(code, name):
    # Try to get ISO code using the code
    try:
        lang = pycountry.languages.lookup(code)
        return lang.alpha_3
    except LookupError:
        pass
    # Try to get ISO code using the language name
    try:
        lang = pycountry.languages.lookup(name)
        return lang.alpha_3
    except LookupError:
        pass
    return "Unknown"


# URL of the Wikipedia page containing the table
url = "https://meta.wikimedia.org/wiki/List_of_Wikipedias"

# Fetch the page content
response = requests.get(url)
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
with open("iso_codes_of_wikis.txt", "w", encoding="utf-8") as f:
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

            # Get the ISO 639-2 code
            iso_code = get_iso_code(code, language_name)

            # Add the mapping to the dictionary
            wiki_code_to_iso_code[code] = iso_code

            # Write the output to the file
            f.write(f"{language_name} ({code}): {iso_code}\n")

# Save the dictionary
with open("./dicts/wiki_code_to_iso_code.json", "w", encoding="utf-8") as json_file:
    json.dump(wiki_code_to_iso_code, json_file, ensure_ascii=False, indent=4)
