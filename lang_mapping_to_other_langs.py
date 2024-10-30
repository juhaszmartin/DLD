import os
import json
import bz2
import re
import csv
from pathlib import Path
import langid

# TODO: fix this to fill the matrix with correct values
# as of now, the detection function is not correct.


def load_iso_mapping():
    """
    Loads the dictionary that maps wiki codes to ISO codes.

    Returns:
        dict: A dictionary where the keys are wiki codes and values are ISO codes.
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


def get_language_mention_matrix(dump_directory, iso_mapping):
    """
    Builds a matrix where each element (i, j) represents the number of times language i
    is mentioned in language j based on langid language detection.

    Args:
        dump_directory (str or Path): Path to the directory containing wiki dump index files.
        iso_mapping (dict): Dictionary mapping wiki codes to ISO codes.

    Returns:
        list: 2D list (matrix) of language mention counts.
    """
    dump_path = Path(dump_directory)
    if not dump_path.is_dir():
        print(f"Error: '{dump_directory}' is not a valid directory.")
        return []

    # List of ISO codes, to keep track of index positions
    iso_codes = list(iso_mapping.values())
    iso_index = {iso: idx for idx, iso in enumerate(iso_codes)}  # Map ISO codes to matrix indices
    matrix = initialize_matrix(len(iso_codes))

    pattern = re.compile(r"^([a-z]{2,3})wiki-.*-multistream-index\.txt\.bz2$")

    for file in dump_path.iterdir():
        if file.is_file():
            match = pattern.match(file.name)
            if match:
                wiki_code = match.group(1)
                iso_code = iso_mapping.get(wiki_code, "Unknown")
                if iso_code == "Unknown" or iso_code not in iso_index:
                    continue  # Skip if ISO code is unknown

                # Column index in the matrix for the current Wikipedia language
                col_idx = iso_index[iso_code]

                try:
                    with bz2.open(file, "rt", encoding="utf-8") as f:
                        for line in f:
                            title = line.strip()
                            # Detect language of the title using langid
                            detected_lang, _ = langid.classify(title)
                            if detected_lang in iso_index:
                                row_idx = iso_index[detected_lang]
                                if row_idx != col_idx:
                                    matrix[row_idx][col_idx] += 1  # Increment mention count
                except Exception as e:
                    print(f"Error processing '{file.name}': {e}")

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
    dump_directory = "./downloads"
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
