import json
import bz2
import re
import csv
from pathlib import Path
import cld3
import pycountry
from multiprocessing import Pool, cpu_count

def load_iso_mapping():
    with open("./dicts/wiki_code_to_iso_code.json", "r", encoding="utf-8") as f:
        return json.load(f)

def get_iso6393_code(language_code):
    try:
        language_code = language_code.split('-')[0]
        lang = pycountry.languages.get(alpha_2=language_code)
        if lang and hasattr(lang, 'alpha_3'):
            return lang.alpha_3
        else:
            lang = pycountry.languages.lookup(language_code)
            if hasattr(lang, 'alpha_3'):
                return lang.alpha_3
    except (LookupError, AttributeError):
        return None
    return None

def process_single_dump(wiki_code, iso_code, dump_path, iso_index):
    counts = []
    col_idx = iso_index[iso_code]
    
    # Find the index file for the current wiki_code
    index_files = [file for file in dump_path.iterdir()
                   if file.is_file() and re.match(rf"^{wiki_code}wiki-.*-multistream-index\.txt\.bz2$", file.name)]

    if not index_files:
        print(f"No index file found for wiki code '{wiki_code}'.")
        return counts
    elif len(index_files) > 1:
        # Choose the first one or implement logic to select the desired file
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
                            counts.append((row_idx, col_idx, 1))
    except Exception as e:
        print(f"Error processing '{index_file.name}': {e}")
    
    return counts

def initialize_matrix(size):
    return [[0 for _ in range(size)] for _ in range(size)]

def aggregate_counts(matrix, partial_counts):
    for row_idx, col_idx, count in partial_counts:
        matrix[row_idx][col_idx] += count

def save_matrix_to_csv(matrix, iso_codes, output_file):
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

    # Prepare ISO codes and index mapping
    iso_codes = list(set(iso_mapping.values()))
    iso_codes.sort()  # Ensure consistent ordering
    iso_index = {iso: idx for idx, iso in enumerate(iso_codes)}  # Map ISO codes to matrix indices
    matrix = initialize_matrix(len(iso_codes))

    dump_path = Path(dump_directory)
    if not dump_path.is_dir():
        print(f"Error: '{dump_directory}' is not a valid directory.")
        return

    # Prepare tasks
    tasks = []
    for wiki_code, iso_code in iso_mapping.items():
        if not iso_code or iso_code not in iso_index:
            continue  # Skip if ISO code is unknown
        tasks.append((wiki_code, iso_code))

    # Initialize multiprocessing Pool
    num_processes = min(cpu_count(), len(tasks))  # Avoid creating more processes than tasks
    with Pool(processes=num_processes) as pool:
        # Map tasks to the pool
        results = pool.starmap(process_single_dump, [(wiki_code, iso_code, dump_path, iso_index) for wiki_code, iso_code in tasks])

    # Aggregate all partial counts
    for partial_counts in results:
        aggregate_counts(matrix, partial_counts)

    # Save the matrix to CSV
    save_matrix_to_csv(matrix, iso_codes, matrix_output_file)
    print(f"Language mention matrix saved to {matrix_output_file}")

if __name__ == "__main__":
    main()
