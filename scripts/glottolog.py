import requests
import pandas as pd
from bs4 import BeautifulSoup

# Headers for request
headers = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,hu;q=0.8",
    "connection": "keep-alive",
    "host": "glottolog.org",
    "referer": "https://glottolog.org/glottolog/language",
    "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132")',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
}


# Function to remove HTML tags
def extract_text(html):
    return BeautifulSoup(html, "html.parser").text if isinstance(html, str) else html


# Column names
columns = ["Glottocode", "Name", "Top-level family", "ISO-639-3", "Macro-area", "Child dialects", "Latitude", "Longitude"]

# Initialize an empty DataFrame
df_all = pd.DataFrame(columns=columns)

# Initial values for the variables
echo = 6
start = 0
end = 1742230672960

# Loop 87 times, updating the variables each time
for _ in range(87):
    # Construct URL with updated values
    url = f"https://glottolog.org/glottolog/language?type=languages&sEcho={echo}&iColumns=8&sColumns=id%2Cname%2Ctop-level+family%2Ciso%2Cmacro-area%2Cchild_dialect_count%2Clatitude%2Clongitude&iDisplayStart={start}&iDisplayLength=100&mDataProp_0=0&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=true&mDataProp_1=1&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&mDataProp_2=2&sSearch_2=&bRegex_2=false&bSearchable_2=true&bSortable_2=true&mDataProp_3=3&sSearch_3=&bRegex_3=false&bSearchable_3=true&bSortable_3=true&mDataProp_4=4&sSearch_4=&bRegex_4=false&bSearchable_4=true&bSortable_4=false&mDataProp_5=5&sSearch_5=&bRegex_5=false&bSearchable_5=true&bSortable_5=true&mDataProp_6=6&sSearch_6=&bRegex_6=false&bSearchable_6=true&bSortable_6=true&mDataProp_7=7&sSearch_7=&bRegex_7=false&bSearchable_7=true&bSortable_7=true&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&__eid__=Families&_={end}"
    # Send request
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        response_json = response.json()
        data = response_json.get("aaData", [])

        if data:
            df = pd.DataFrame(data, columns=columns)
            df = df.map(extract_text)  # Remove HTML tags
            df_all = pd.concat([df_all, df], ignore_index=True)  # Append to main DataFrame
        else:
            print(f"No data found for iteration {_+1}.")
    else:
        print(f"Failed to fetch data at iteration {_+1}, Status Code: {response.status_code}")

    # Increment variables for the next iteration
    echo += 1
    start += 100
    end += 1

# Display the final DataFrame
# print(df_all)

df_all.to_csv("./data/glottolog_languages.csv", index=False)
print(f"Wrote {len(df_all)} languages to ./data/glottolog_languages.csv")
