import os
import json
import bz2
import re
import csv
from pathlib import Path
import cld3
import pycountry


def load_iso_mapping():
    """
    Loads the dictionary that maps wiki codes to ISO 639-3 codes.

    Returns:
        dict: A dictionary where the keys are wiki codes and values are ISO 639-3 codes.
    """
    with open("./dicts/wiki_code_to_iso_code.json", "r", encoding="utf-8") as f:
        return json.load(f)

def initialize_matrix(size):
    """
    Initializes a square matrix of given size with zeros.

    Args:
        size (int): Size of the matrix (number of languages).

    Returns:
        list: 2D list (matrix) initialized with zeros.
    """
    return [[0 for _ in range(size)] for _ in range(size)]

def get_iso6393_code(language_code):
    """
    Maps a BCP-47 language code to an ISO 639-3 code.

    Args:
        language_code (str): BCP-47 language code (e.g., 'en', 'fr').

    Returns:
        str: ISO 639-3 code (e.g., 'eng', 'fra') or None if not found.
    """
    try:
        # Handle cases like 'zh-Hans', 'zh-Hant'
        language_code = language_code.split('-')[0]
        lang = pycountry.languages.get(alpha_2=language_code)
        if lang and hasattr(lang, 'alpha_3'):
            return lang.alpha_3
        else:
            # Try lookup by name
            lang = pycountry.languages.lookup(language_code)
            if hasattr(lang, 'alpha_3'):
                return lang.alpha_3
    except (LookupError, AttributeError):
        return None
    return None

def get_language_mention_matrix(dump_directory, iso_mapping):
    """
    Builds a matrix where each element (i, j) represents the number of times language i
    is mentioned in language j based on pycld3 language detection.

    Args:
        dump_directory (str or Path): Path to the directory containing wiki dump files.
        iso_mapping (dict): Dictionary mapping wiki codes to ISO codes.

    Returns:
        tuple: (matrix, iso_codes) where 'matrix' is a 2D list of language mention counts,
               and 'iso_codes' is a list of ISO 639-3 codes corresponding to matrix indices.
    """
    dump_path = Path(dump_directory)
    if not dump_path.is_dir():
        print(f"Error: '{dump_directory}' is not a valid directory.")
        return [], []

    # List of ISO codes, to keep track of index positions
    iso_codes = list(set(iso_mapping.values()))
    iso_codes.sort()  # Ensure consistent ordering
    iso_index = {iso: idx for idx, iso in enumerate(iso_codes)}  # Map ISO codes to matrix indices
    matrix = initialize_matrix(len(iso_codes))

    for wiki_code in iso_mapping.keys():
        iso_code = iso_mapping.get(wiki_code)
        if not iso_code or iso_code not in iso_index:
            continue  # Skip if ISO code is unknown

        # Column index in the matrix for the current Wikipedia language
        col_idx = iso_index[iso_code]

        # Find the index file for the current wiki_code
        index_files = [file for file in dump_path.iterdir()
                       if file.is_file() and re.match(rf"^{wiki_code}wiki-.*-multistream-index\.txt\.bz2$", file.name)]

        if not index_files:
            print(f"No index file found for wiki code '{wiki_code}'.")
            continue
        elif len(index_files) > 1:
            # If multiple index files are found, you may want to choose the latest one
            # For simplicity, we'll take the first one (you can adjust this as needed)
            index_file = index_files[0]
            print(f"Multiple index files found for '{wiki_code}'. Using '{index_file.name}'.")
        else:
            index_file = index_files[0]

        try:
            with bz2.open(index_file, "rt", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split(":", 2)
                    if len(parts) != 3:
                        continue
                    title = parts[2]
                    # Detect language of the title using pycld3
                    result = cld3.get_language(title)
                    if result.is_reliable:
                        detected_lang_code = result.language  # BCP-47 code
                        iso6393_code = get_iso6393_code(detected_lang_code)
                        if iso6393_code and iso6393_code in iso_index:
                            row_idx = iso_index[iso6393_code]
                            if row_idx != col_idx:
                                matrix[row_idx][col_idx] += 1  # Increment mention count
        except Exception as e:
            print(f"Error processing '{index_file.name}': {e}")

    return matrix, iso_codes

def save_matrix_to_csv(matrix, iso_codes, output_file):
    """
    Saves the matrix to a CSV file with ISO codes as row and column headers.

    Args:
        matrix (list): 2D list (matrix) of language mention counts.
        iso_codes (list): List of ISO codes for headers.
        output_file (str): Path to the output CSV file.
    """
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Write header row
        writer.writerow(["ISO_Code"] + iso_codes)
        # Write matrix rows with ISO code labels
        for i, row in enumerate(matrix):
            writer.writerow([iso_codes[i]] + row)

def main():
    dump_directory = "/mnt/c/matek_msc/AML/DLD/DLD/downloads"
    matrix_output_file = "./dicts/language_mention_matrix.csv"

    # Load ISO mapping and language names
    iso_mapping = load_iso_mapping()

    # Get language mention matrix
    matrix, iso_codes = get_language_mention_matrix(dump_directory, iso_mapping)

    # Save the matrix to CSV
    save_matrix_to_csv(matrix, iso_codes, matrix_output_file)
    print(f"Language mention matrix saved to {matrix_output_file}")

if __name__ == "__main__":
    main()
