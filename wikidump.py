import os
import requests
from tqdm import tqdm
import bz2
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import time
import re
from pathlib import Path


def get_main_folders(base_url):
    """
    Retrieves a list of main folders from the given base URL.

    Parameters:
        base_url (str): The URL of the wikidump page.

    Returns:
        list: A list of folder names (as strings).
    """
    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, "html.parser")

    main_folders = []

    # Find all 'tr' elements with class 'even' or 'odd'
    rows = soup.find_all("tr", class_=["even", "odd"])
    for row in rows:
        # Find 'a' tags that link to directories (ending with '/')
        a_tag = row.find("a", href=True)
        if a_tag and a_tag["href"].endswith("/"):
            folder_name = a_tag["href"]
            main_folders.append(folder_name)

    return main_folders


def get_newest_subfolder(base_url, folder_name, max_retries=3, backoff_factor=1):
    """
    Retrieves the newest subfolder within a given main folder.

    Parameters:
        base_url (str): The base URL of the wikidump page.
        folder_name (str): The name of the main folder.
        max_retries (int): Maximum number of retry attempts for failed requests.
        backoff_factor (int): Factor by which the delay increases after each retry.

    Returns:
        str or None: The name of the newest subfolder (with trailing '/'), or None if none are found.
    """
    folder_url = urljoin(base_url, folder_name)
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(folder_url, timeout=10)  # Added timeout
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            subfolders = []

            # Find all 'tr' elements with class 'even' or 'odd'
            rows = soup.find_all("tr", class_=["even", "odd"])
            for row in rows:
                a_tag = row.find("a", href=True)
                if a_tag and a_tag["href"].endswith("/"):
                    subfolder_name = a_tag["href"].rstrip("/")
                    # Try to parse the date from the subfolder name
                    try:
                        folder_date = datetime.strptime(subfolder_name, "%Y%m%d")
                        subfolders.append((subfolder_name + "/", folder_date))
                    except ValueError:
                        # If the folder name doesn't match the date format, skip it
                        continue

            # Sort subfolders by date in descending order
            subfolders.sort(key=lambda x: x[1], reverse=True)

            if subfolders:
                newest_subfolder = subfolders[0][0]
                return newest_subfolder
            else:
                return None

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"WARNING: Folder not found (404) at {folder_url}. Skipping this folder.")
                return None
            else:
                print(f"HTTP Error: {e} for URL: {folder_url}")
        except requests.exceptions.ConnectionError as e:
            print(f"Connection Error: {e} on attempt {attempt} for URL: {folder_url}")
        except requests.exceptions.Timeout as e:
            print(f"Timeout Error: {e} on attempt {attempt} for URL: {folder_url}")
        except requests.exceptions.RequestException as e:
            print(f"Request Exception: {e} on attempt {attempt} for URL: {folder_url}")

        # Implement linear backoff before retrying
        if attempt < max_retries:
            sleep_time = backoff_factor * attempt
            print(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
        else:
            print(f"Failed to retrieve {folder_url} after {max_retries} attempts. Skipping this folder.")
            return None


def download_file(url, local_filename):
    """
    Downloads a file from the given URL to a local path with a progress bar.
    Handles HTTP 404 errors by logging a warning and skipping the file.

    Parameters:
        url (str): The URL of the file to download.
        local_filename (str): The local path where the file will be saved.

    Returns:
        bool: True if download is successful, False otherwise.
    """
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get("content-length", 0))
            block_size = 1024 * 1024  # 1 MB
            progress = tqdm(total=total_size, unit="iB", unit_scale=True, desc=f"Downloading {os.path.basename(local_filename)}")
            with open(local_filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=block_size):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        progress.update(len(chunk))
            progress.close()
            if total_size != 0 and progress.n != total_size:
                print(f"ERROR: Download of {local_filename} incomplete.")
                return False
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            print(f"WARNING: File not found (404) at {url}. Skipping.")
        else:
            print(f"HTTP Error: {e} for URL: {url}")
        return False
    except Exception as e:
        print(f"Error while downloading {local_filename}: {e}")
        return False
    return True


def process_folders(base_url):
    """
    Processes each main folder to find and navigate into the newest subfolder.

    Parameters:
        base_url (str): The base URL of the wikidump page.
    """
    main_folders = get_main_folders(base_url)
    for folder_name in main_folders:
        newest_subfolder = get_newest_subfolder(base_url, folder_name)
        if newest_subfolder:
            print(f"In folder '{folder_name}', the newest subfolder is '{newest_subfolder}'")
            # Construct the URL to the newest subfolder
            subfolder_url = urljoin(urljoin(base_url, folder_name), newest_subfolder)
            # You can add code here to navigate into subfolder_url and perform further actions
        else:
            print(f"No subfolders found in folder '{folder_name}'")


def construct_dump_urls(base_url, main_folder, subfolder):
    """
    Constructs the necessary dump filenames and their corresponding URLs.

    Parameters:
        base_url (str): The base URL of the wikidump page.
        main_folder (str): The main folder name (e.g., 'huwiki/').
        subfolder (str): The newest subfolder name (e.g., '20240920/').

    Returns:
        dict: A dictionary containing 'dump_base', 'multistream_url', and 'index_url'.
    """
    # Remove trailing slashes for consistent naming
    main_folder_clean = main_folder.rstrip("/")
    subfolder_clean = subfolder.rstrip("/")

    # Construct dump_base
    dump_base = f"{main_folder_clean}-{subfolder_clean}-pages-articles"

    # Construct filenames
    multistream_filename = f"{dump_base}-multistream.xml.bz2"
    index_filename = f"{dump_base}-multistream-index.txt.bz2"

    # Construct URLs
    subfolder_url = urljoin(base_url, f"{main_folder}{subfolder}")
    multistream_url = urljoin(subfolder_url, multistream_filename)
    index_url = urljoin(subfolder_url, index_filename)

    return {
        "dump_base": dump_base,
        "multistream_filename": multistream_filename,
        "index_filename": index_filename,
        "multistream_url": multistream_url,
        "index_url": index_url,
    }


def process_folders(base_url):
    """
    Processes each main folder to find the newest subfolder and construct dump URLs.

    Parameters:
        base_url (str): The base URL of the wikidump page.
    """
    main_folders = get_main_folders(base_url)
    dump_info_list = []  # To store information about each dump

    for folder_name in main_folders:
        newest_subfolder = get_newest_subfolder(base_url, folder_name)
        if newest_subfolder:
            dump_info = construct_dump_urls(base_url, folder_name, newest_subfolder)
            dump_info_list.append(
                {
                    "main_folder": folder_name.rstrip("/"),
                    "newest_subfolder": newest_subfolder.rstrip("/"),
                    "dump_base": dump_info["dump_base"],
                    "multistream_filename": dump_info["multistream_filename"],
                    "index_filename": dump_info["index_filename"],
                    "multistream_url": dump_info["multistream_url"],
                    "index_url": dump_info["index_url"],
                }
            )
            print(f"In folder '{folder_name}', the newest subfolder is '{newest_subfolder}'")
            print(f"  Dump Base: {dump_info['dump_base']}")
            print(f"  Multistream URL: {dump_info['multistream_url']}")
            print(f"  Index URL: {dump_info['index_url']}\n")
        else:
            print(f"No valid subfolders found in folder '{folder_name}'\n")

    return dump_info_list


def download_all_wiki_dumps(how_many_dumps=4):
    """
    Downloads wiki dump multistream article files and indices and saves them.

    Parameters:
        how_many_dumps: The number of dumps to download and save. Default is 4. Set to None if you want all (~100GB).
    """
    base_url = "https://mirror.accum.se/mirror/wikimedia.org/dumps/"  # Ensure this URL is correct

    # Process folders and get dump information
    dumps = process_folders(base_url)

    # Apply the how_many_dumps limit if specified
    if how_many_dumps is not None:
        limited_dumps = dumps[:how_many_dumps]
        print(f"Limiting downloads to the first {how_many_dumps} dump(s).\n")
    else:
        limited_dumps = dumps

    if not limited_dumps:
        print("No dumps to download. Exiting.")
        return

    # Example: Downloading the multistream and index files for each dump
    for dump in limited_dumps:
        multistream_url = dump["multistream_url"]
        index_url = dump["index_url"]
        dump_base = dump["dump_base"]

        # Define local paths (you can customize the directory as needed)
        multistream_dest = os.path.join("downloads", dump["multistream_filename"])
        index_dest = os.path.join("downloads", dump["index_filename"])

        # Ensure the downloads directory exists
        os.makedirs(os.path.dirname(multistream_dest), exist_ok=True)

        # Download the multistream file if it doesn't exist
        if not os.path.exists(multistream_dest):
            print(f"Downloading {dump['multistream_filename']}...")
            success = download_file(multistream_url, multistream_dest)
            if not success:
                print(f"Warning: Failed to download the multistream dump for '{dump_base}'. Skipping to next dump.\n")
        else:
            print(f"{dump['multistream_filename']} already exists. Skipping download.")

        # Download the multistream index if it doesn't exist
        if not os.path.exists(index_dest):
            print(f"Downloading {dump['index_filename']}...")
            success = download_file(index_url, index_dest)
            if not success:
                print(f"Warning: Failed to download the multistream index for '{dump_base}'. Skipping to next dump.\n")
        else:
            print(f"{dump['index_filename']} already exists. Skipping download.")

    print("Download process completed.")


def count_articles_per_language(dump_directory):
    """
    Counts the number of articles in each wiki dump within the specified directory.
    Prints "{wiki_code} has {n_lang} articles" for each language.

    Args:
        dump_directory (str or Path): Path to the directory containing wiki dump index files.
    """
    dump_path = Path(dump_directory)

    if not dump_path.is_dir():
        print(f"Error: '{dump_directory}' is not a valid directory.")
        return

    # Regex pattern to match index files and extract the wiki_code
    # Example filename: huwiki-20240920-pages-articles-multistream-index.txt.bz2
    pattern = re.compile(r"^([a-z]{2,3})wiki-.*-multistream-index\.txt\.bz2$")

    # Dictionary to store the count of articles per language
    lang_article_counts = {}

    # Iterate over all files in the specified directory
    for file in dump_path.iterdir():
        if file.is_file():
            match = pattern.match(file.name)
            if match:
                wiki_code = match.group(1)
                try:
                    # Open the bz2-compressed index file in text mode
                    with bz2.open(file, "rt", encoding="utf-8") as f:
                        # Count the number of lines, each representing an article
                        article_count = sum(1 for _ in f)
                    lang_article_counts[wiki_code] = article_count
                except Exception as e:
                    print(f"Error processing '{file.name}': {e}")

    # Print the results
    for wiki_code, count in lang_article_counts.items():
        print(f"{wiki_code} has {count} articles")


def main():
    download_all_wiki_dumps(how_many_dumps=6)
    count_articles_per_language("downloads")

    ## URLs for the multistream dump and index
    # multistream_url = base_url + multistream_filename
    # index_url = base_url + index_filename


#
## Download the multistream dump if it doesn't exist
# if not os.path.exists(multistream_filename):
#    print(f"Downloading {multistream_filename}...")
#    success = download_file(multistream_url, multistream_filename)
#    if not success:
#        print("Failed to download the multistream dump. Exiting.")
#        return
# else:
#    print(f"{multistream_filename} already exists. Skipping download.")
#
## Download the multistream index if it doesn't exist
# if not os.path.exists(index_filename):
#    print(f"Downloading {index_filename}...")
#    success = download_file(index_url, index_filename)
#    if not success:
#        print("Failed to download the multistream index. Exiting.")
#        return
# else:
#    print(f"{index_filename} already exists. Skipping download.")

# Inspect the index file to understand its format
# NOTE: index file has format: <stream_number>:<section_number>:<title>
# inspect_index_file(index_filename, num_lines=5)

# Parse the multistream index and extract titles
# parse_multistream_index(index_filename, titles_output)
# print(f"Titles have been saved to {titles_output}")

if __name__ == "__main__":
    main()
