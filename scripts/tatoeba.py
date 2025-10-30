import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://tatoeba.org/en/stats/sentences_by_language"
resp = requests.get(url)
resp.raise_for_status()

soup = BeautifulSoup(resp.text, "html.parser")

# find the table
table = soup.find("table", class_="languages-stats")
if table is None:
    raise RuntimeError("Could not find table with class 'languages-stats'")

# find all rows
rows = table.find_all("tr")

# first row is headers
headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]

# remaining rows are data
data = []
for tr in rows[1:]:
    cells = [td.get_text(strip=True) for td in tr.find_all("td")]
    # skip empty rows
    if cells:
        data.append(cells)

# create DataFrame
df = pd.DataFrame(data, columns=headers)
cols = list(df.columns)
cols[2] = "ISO 639-3"
df.columns = cols
df.drop(df.columns[:1], axis=1, inplace=True)
df.reset_index(drop=True, inplace=True)
df["Sentences"] = pd.to_numeric(df["Sentences"].str.replace(",", "", regex=False), errors="coerce").fillna(0).astype(int)
print(df.head())
df.to_csv("./data/tatoeba_sentences_by_language.csv", index=False)
