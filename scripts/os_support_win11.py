import requests
from bs4 import BeautifulSoup
import pandas as pd
import pycountry

# NOTE: Valencian does not have ISO 639-3 code

# Custom mapping for languages that pycountry can't find with these names (used Wals data for codes)
custom_mapping = {
    "Nepali": "nep",
    "Scottish": "gae",
    "Greek": "grk",
    "Konkani": "kkn",
    "Malay": "mly",
    "Punjabi": "pan",
    "Uyghur": "uyg",
}


def get_iso_code(language_name):
    # Check if the language name is in the macrolanguages dictionary
    if language_name in custom_mapping:
        return custom_mapping[language_name]

    # Try looking it up in pycountry if not a macrolanguage
    try:
        lang = pycountry.languages.lookup(language_name)
        return lang.alpha_3  # Return the ISO 639-3 code
    except LookupError:
        return None  # If not found, return None


# URL of the Microsoft support page
url = (
    "https://support.microsoft.com/en-us/windows/language-packs-for-windows-a5094319-a92d-18de-5b53-1cfc697cfca8#windowsversion=windows_11"
)

# Send a GET request
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the HTML content
    soup = BeautifulSoup(response.text, "html.parser")

    # Find the table
    table = soup.select_one("article section div div:nth-of-type(2) div:nth-of-type(1) table")

    if table:
        # Extract table headers
        headers = [th.text.strip() for th in table.find_all("th")]
        headers.append("ISO 639-3 Code")  # Add new column header

        # Extract table rows
        rows = []
        for tr in table.find_all("tr")[1:]:  # Skip header row
            cells = [td.text.strip() for td in tr.find_all("td")]

            if cells:
                language_name = cells[0].split()[0]  # First column: Language Name
                iso_code = get_iso_code(language_name)  # Get ISO code using the custom function

                cells.append(iso_code)  # Append the ISO code to the row
                rows.append(cells)

        # Create a DataFrame
        df = pd.DataFrame(rows, columns=headers)

        # Print the table
        print(df)
    else:
        print("Table not found on the page.")
else:
    print(f"Failed to retrieve the webpage. Status code: {response.status_code}")

df.to_csv("./data/os_support_windows.csv", index=False)
