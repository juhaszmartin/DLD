import pandas as pd
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os

# Correct path to your WebDriver
EDGEDRIVER_PATH = "C:/webdriver/edgedriver_win64/msedgedriver.exe"

# Edge browser binary path (adjust if needed)
EDGE_BINARY_PATH = "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe"

# Create a Service object for EdgeDriver
service = Service(EDGEDRIVER_PATH)

# Set up Edge options
options = webdriver.EdgeOptions()
options.binary_location = EDGE_BINARY_PATH  # Explicitly set Edge binary

# Start WebDriver
driver = webdriver.Edge(service=service, options=options)

# Load the language codes from the .txt file
output_folder = "./data"
file_path = os.path.join(output_folder, "ethnologue_code_list.txt")

with open(file_path, "r", encoding="utf-8") as f:
    language_codes = [line.strip() for line in f.readlines()]

# Initialize a list to store the scraped data
data = []

# Base URL for language pages
base_url = "https://www.ethnologue.com/language/"

# Iterate through each language code
for code in language_codes:
    try:
        # Construct the URL
        url = base_url + code
        driver.get(url)

        # Wait for the page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/main/article/section[2]/ul/li[2]/article/div[1]/div/div[1]/div"))
        )

        # Scrape the population size
        population_size = driver.find_element(
            By.XPATH, "/html/body/main/article/section[2]/ul/li[2]/article/div[1]/div/div[1]/div"
        ).text.strip()

        # Scrape the language vitality percentages (bar widths)
        vitality_categories = ["Institutional", "Stable", "Endangered", "Extinct"]
        vitality_data = {}

        # Locate the <ol> element containing the vitality data
        vitality_ol = driver.find_element(By.XPATH, "//ol[@class='histogram-vitality']")

        # Loop through each <li> element in the <ol>
        for li in vitality_ol.find_elements(By.XPATH, ".//li[@class='histogram__datum tooltip']"):
            # Extract the category name (e.g., Institutional, Stable, etc.)
            category = li.find_element(By.XPATH, ".//div[@class='histogram__datum-label']/label").text.strip()

            # Extract the bar width from the style attribute
            bar_element = li.find_element(By.XPATH, ".//div[@class='histogram__bar']")
            bar_width = bar_element.get_attribute("style")  # Get the style attribute
            if "--bar-width" in bar_width:
                bar_width = bar_width.split("--bar-width:")[1].split(";")[0].strip()
            else:
                bar_width = "0%"  # Default to 0% if --bar-width is not found

            # Store the bar width in the vitality_data dictionary
            vitality_data[category] = bar_width

        # Scrape the digital language support image name
        digital_support_img = driver.find_element(By.XPATH, "/html/body/main/article/section[2]/ul/li[4]/article/p/img").get_attribute(
            "src"
        )
        digital_support = digital_support_img.split("/")[-1].split(".")[0]

        # Scrape the language name
        language_name = driver.find_element(By.XPATH, "/html/body/main/article/header/h1").text.strip()

        # Scrape the summary
        summary = driver.find_element(By.XPATH, "/html/body/main/article/section[1]/p").text.strip()

        # Append the data to the list
        data.append(
            [
                code,
                language_name,
                summary,
                population_size,
                vitality_data.get("Institutional", "0%"),
                vitality_data.get("Stable", "0%"),
                vitality_data.get("Endangered", "0%"),
                vitality_data.get("Extinct", "0%"),
                digital_support,
            ]
        )

        print(f"Scraped data for {code}:{language_name} {summary} {population_size}, {vitality_data}, {digital_support}")

        # Wait a short time before moving to the next code (optional)
        # time.sleep(0.1)

    except Exception as e:
        print(f"Error scraping data for {code}: {e}")

# Close the WebDriver after scraping
driver.quit()

# Create a DataFrame from the scraped data
df = pd.DataFrame(
    data,
    columns=[
        "ISO Code",
        "Language Name",
        "Summary",
        "Population Size",
        "Institutional (%)",
        "Stable (%)",
        "Endangered (%)",
        "Extinct (%)",
        "Digital Support",
    ],
)

# Save the DataFrame to a CSV file
output_csv_path = os.path.join(output_folder, "ethnologue_language_data.csv")
df.to_csv(output_csv_path, index=False)

print(f"\nScraped data saved to {output_csv_path}")
