from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
import time

output_file_path = "./data/ethnologue_code_list.txt"

# Correct path to your WebDriver
EDGEDRIVER_PATH = "C:/webdriver/edgedriver_win64/msedgedriver.exe"
EDGE_BINARY_PATH = "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe"

# Create a Service object for EdgeDriver
service = Service(EDGEDRIVER_PATH)
options = webdriver.EdgeOptions()
options.binary_location = EDGE_BINARY_PATH

# Start WebDriver
driver = webdriver.Edge(service=service, options=options)
driver.get("https://www.ethnologue.com/browse/codes/")
time.sleep(3)  # Allow time for the page to load

codes_list = []

for letter_index in range(1, 27):  # Loop through all letters from A to Z
    try:
        letter = chr(65 + letter_index - 1)  # Get the letter (A-Z)
        print(f"Processing letter {letter}...")

        # Click on the letter tab
        letter_xpath = f"/html/body/main/div/div/div/div[3]/div/div/ul/li[{letter_index}]/a"
        driver.find_element(By.XPATH, letter_xpath).click()
        time.sleep(3)  # Allow time for the new table to load

        # Table XPath for the current letter
        table_xpath = f"/html/body/main/div/div/div/div[{letter_index + 3}]"

        # Loop through all 8 columns
        for col in range(1, 9):
            try:
                column_xpath = f"{table_xpath}/div[{col}]"
                column = driver.find_element(By.XPATH, column_xpath)

                # Extract all code links from the column
                code_elements = column.find_elements(By.TAG_NAME, "a")
                codes = [code.text for code in code_elements if code.text]
                codes_list.extend(codes)
            except Exception as col_error:
                print(f"Skipping column {col} for letter {letter} due to: {col_error}")
    except Exception as e:
        print(f"Error processing letter {letter}: {e}")

# Save results to a text file in the output folder
with open(output_file_path, "w") as file:
    for code in codes_list:
        file.write(code + "\n")

print(f"Scraping completed. Codes saved to {output_file_path}")

driver.quit()
