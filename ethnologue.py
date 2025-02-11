from selenium import webdriver
from selenium.webdriver.edge.service import Service
import time

# Correct WebDriver path
EDGEDRIVER_PATH = "/mnt/c/webdriver/edgedriver_win64/msedgedriver.exe"

# Edge browser binary path (adjust if needed)
EDGE_BINARY_PATH = "/mnt/c/Program Files (x86)/Microsoft/Edge/Application/msedge.exe"

# Create a Service object for EdgeDriver
service = Service(EDGEDRIVER_PATH)

# Set up Edge options
options = webdriver.EdgeOptions()
options.binary_location = EDGE_BINARY_PATH  # Explicitly set Edge binary

# Start WebDriver
driver = webdriver.Edge(service=service, options=options)

# Open the website
driver.get("https://www.ethnologue.com/browse/codes/")
time.sleep(5)

print(driver.title)

driver.quit()
