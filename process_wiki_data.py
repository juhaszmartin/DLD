import os
import json
import bz2
import re
from pathlib import Path
from tqdm import tqdm
import statistics

print(os.getcwd())


def load_iso_mapping():
    """
    Loads the dictionary that maps wiki codes to ISO codes.

    Returns:
        dict: A dictionary where the keys are wiki codes and values are ISO codes.
    """
    with open("./dicts/wiki_code_to_iso_code.json", "r", encoding="utf-8") as f:
        return json.load(f)


def count_articles_per_language(dump_directory, iso_mapping):
    """
    Counts the number of articles in each wiki dump and maps them to ISO codes.

    Args:
        dump_directory (str or Path): Path to the directory containing wiki dump index files.
        iso_mapping (dict): Dictionary mapping wiki codes to ISO codes.

    Returns:
        dict: Dictionary mapping ISO codes to article counts.
    """
    dump_path = Path(dump_directory)

    if not dump_path.is_dir():
        print(f"Error: '{dump_directory}' is not a valid directory.")
        return {}

    pattern = re.compile(r"^([a-z]{2,3})wiki-.*-multistream-index\.txt\.bz2$")

    iso_article_counts = {}

    for file in dump_path.iterdir():
        if file.is_file():
            match = pattern.match(file.name)
            if match:
                wiki_code = match.group(1)
                iso_code = iso_mapping.get(wiki_code, "Unknown")

                try:
                    with bz2.open(file, "rt", encoding="utf-8") as f:
                        article_count = sum(1 for _ in f)

                    if iso_code in iso_article_counts:
                        iso_article_counts[iso_code] += article_count
                    else:
                        iso_article_counts[iso_code] = article_count

                except Exception as e:
                    print(f"Error processing '{file.name}': {e}")

    return iso_article_counts


def calculate_avg_article_length_per_language(dump_directory, iso_mapping):
    """
    Calculates the average article length (in characters) in each wiki dump and maps them to ISO codes.

    Args:
        dump_directory (str or Path): Path to the directory containing wiki dump index files.
        iso_mapping (dict): Dictionary mapping wiki codes to ISO codes.

    Returns:
        dict: Dictionary mapping ISO codes to average article lengths.
    """
    dump_path = Path(dump_directory)

    if not dump_path.is_dir():
        print(f"Error: '{dump_directory}' is not a valid directory.")
        return {}

    pattern = re.compile(r"^([a-z]{2,3})wiki-.*-multistream-index\.txt\.bz2$")

    iso_article_lengths = {}

    for file in dump_path.iterdir():
        if file.is_file():
            match = pattern.match(file.name)
            if match:
                wiki_code = match.group(1)
                iso_code = iso_mapping.get(wiki_code, "Unknown")

                total_length = 0
                article_count = 0

                try:
                    with bz2.open(file, "rt", encoding="utf-8") as f:
                        for line in f:
                            article_length = len(line.strip())  # Length in characters
                            total_length += article_length
                            article_count += 1

                    if article_count > 0:
                        avg_length = total_length / article_count
                    else:
                        avg_length = 0

                    iso_article_lengths[iso_code] = avg_length

                except Exception as e:
                    print(f"Error processing '{file.name}': {e}")

    return iso_article_lengths


def calculate_median_article_length_per_language(dump_directory, iso_mapping):
    """
    Calculates the median article length (in characters) in each wiki dump and maps them to ISO codes.

    Args:
        dump_directory (str or Path): Path to the directory containing wiki dump index files.
        iso_mapping (dict): Dictionary mapping wiki codes to ISO codes.

    Returns:
        dict: Dictionary mapping ISO codes to median article lengths.
    """
    dump_path = Path(dump_directory)

    if not dump_path.is_dir():
        print(f"Error: '{dump_directory}' is not a valid directory.")
        return {}

    pattern = re.compile(r"^([a-z]{2,3})wiki-.*-multistream-index\.txt\.bz2$")

    iso_median_lengths = {}

    for file in dump_path.iterdir():
        if file.is_file():
            match = pattern.match(file.name)
            if match:
                wiki_code = match.group(1)
                iso_code = iso_mapping.get(wiki_code, "Unknown")

                article_lengths = []

                try:
                    with bz2.open(file, "rt", encoding="utf-8") as f:
                        for line in f:
                            article_length = len(line.strip())  # Length in characters
                            article_lengths.append(article_length)

                    if article_lengths:
                        median_length = statistics.median(article_lengths)
                    else:
                        median_length = 0

                    iso_median_lengths[iso_code] = median_length

                except Exception as e:
                    print(f"Error processing '{file.name}': {e}")

    return iso_median_lengths


def save_iso_article_counts(counts, output_file):
    """
    Saves the ISO-to-article-count mapping as a JSON file.

    Args:
        counts (dict): Dictionary mapping ISO codes to article counts.
        output_file (str): Path to the output JSON file.
    """
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(counts, f, ensure_ascii=False, indent=4)


def main():
    dump_directory = "./downloads"
    count_output_file = "./dicts/iso_article_counts.json"
    avg_length_output_file = "./dicts/iso_avg_article_lengths.json"
    median_length_output_file = "./dicts/iso_median_article_lengths.json"

    iso_mapping = load_iso_mapping()

    # Get article counts per language and map to ISO codes
    iso_article_counts = count_articles_per_language(dump_directory, iso_mapping)
    save_iso_article_counts(iso_article_counts, count_output_file)
    print(f"ISO-to-article-count mapping saved to {count_output_file}")

    # Get average article length per language and map to ISO codes
    iso_avg_article_lengths = calculate_avg_article_length_per_language(dump_directory, iso_mapping)
    save_iso_article_counts(iso_avg_article_lengths, avg_length_output_file)
    print(f"ISO-to-average-article-length mapping saved to {avg_length_output_file}")

    # Get median article length per language and map to ISO codes
    iso_median_article_lengths = calculate_median_article_length_per_language(dump_directory, iso_mapping)
    save_iso_article_counts(iso_median_article_lengths, median_length_output_file)
    print(f"ISO-to-median-article-length mapping saved to {median_length_output_file}")


if __name__ == "__main__":
    main()
