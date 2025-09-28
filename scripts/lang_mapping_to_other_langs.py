import json
import bz2
import re
import csv
from pathlib import Path
import cld3
import pycountry
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import mwxml
from collections import Counter  # Import Counter for efficient counting


def load_iso_mapping():
    with open("./data/wiki_code_to_iso_code.json", "r", encoding="utf-8") as f:
        return json.load(f)


def get_iso6393_code(language_code):
    try:
        language_code = language_code.split("-")[0]
        lang = pycountry.languages.get(alpha_2=language_code)
        if lang and hasattr(lang, "alpha_3"):
            return lang.alpha_3
        else:
            lang = pycountry.languages.lookup(language_code)
            if hasattr(lang, "alpha_3"):
                return lang.alpha_3
    except (LookupError, AttributeError):
        return None


def process_single_dump(args):
    wiki_code, iso_code, dump_path, iso_index, position = args
    col_idx = iso_index[iso_code]
    local_counts = Counter()  # Use Counter for efficient counting

    # Find the multistream XML file for the current wiki_code
    files = [
        file
        for file in dump_path.iterdir()
        if file.is_file() and re.match(rf"^{wiki_code}wiki-.*-pages-articles-multistream\.xml\.bz2$", file.name)
    ]

    if not files:
        print(f"No multistream XML file found for wiki code '{wiki_code}'.")
        return None
    elif len(files) > 1:
        dump_file = files[0]
        print(f"Multiple files found for '{wiki_code}'. Using '{dump_file.name}'.")
    else:
        dump_file = files[0]

    try:
        dump_file_path = dump_file
        # Open the compressed file using bz2.open() in binary mode
        with bz2.open(str(dump_file_path), "rb") as f:
            dump = mwxml.Dump.from_file(f)
            pages = dump.pages  # Get the pages iterator

            # Initialize tqdm progress bar for this dump
            pbar = tqdm(desc=f"Processing {iso_code}", position=position)
            chunk_size = 2000  # Adjust the chunk size as needed
            while True:
                chunk = []
                try:
                    for _ in range(chunk_size):
                        page = next(pages)
                        chunk.append(page)
                except StopIteration:
                    pass  # Reached the end of the iterator

                if not chunk:
                    break  # No more pages to process

                for page in chunk:
                    # Process only articles in the main namespace
                    if page.namespace != 0:
                        continue

                    latest_revision_text = None  # Store latest revision text

                    for revision in page:
                        if revision.text:
                            latest_revision_text = revision.text  # Overwrite to get the latest one

                    if latest_revision_text:  # Process only if we have a valid text
                        # Detect language of the text using cld3
                        result = cld3.get_language(latest_revision_text)
                        if result and result.is_reliable:
                            detected_lang_code = result.language  # BCP-47 code
                            iso6393_code = get_iso6393_code(detected_lang_code)
                            if iso6393_code and iso6393_code in iso_index:
                                row_idx = iso_index[iso6393_code]
                                if row_idx != col_idx:
                                    local_counts[(row_idx, col_idx)] += 1

                pbar.update(len(chunk))  # Update progress bar
            # pbar.close()
    except Exception as e:
        print(f"Error processing '{dump_file.name}': {e}")
        return None

    return local_counts


def initialize_matrix(size):
    return [[0 for _ in range(size)] for _ in range(size)]


def aggregate_counts(matrix, partial_counts):
    for (row_idx, col_idx), count in partial_counts.items():
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
    dump_directory = "./downloads"
    matrix_output_file = "./data/language_mention_matrix.csv"

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
    for idx, (wiki_code, iso_code) in enumerate(iso_mapping.items()):
        if not iso_code or iso_code not in iso_index:
            continue  # Skip if ISO code is unknown
        tasks.append((wiki_code, iso_code, dump_path, iso_index, idx))

    # Initialize multiprocessing Pool
    num_processes = min(cpu_count(), len(tasks))  # Avoid creating more processes than tasks

    with Pool(processes=num_processes) as pool:
        # Map tasks to the pool
        results = []
        for local_counts in tqdm(pool.imap_unordered(process_single_dump, tasks), total=len(tasks), desc="Processing Dumps"):
            if local_counts:
                aggregate_counts(matrix, local_counts)

    # Save the matrix to CSV
    save_matrix_to_csv(matrix, iso_codes, matrix_output_file)
    print(f"Language mention matrix saved to {matrix_output_file}")


if __name__ == "__main__":
    main()
