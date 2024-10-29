import os
import json
import bz2
import re
from pathlib import Path
from tqdm import tqdm

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

    # Regex pattern to match index files and extract the wiki_code
    pattern = re.compile(r"^([a-z]{2,3})wiki-.*-multistream-index\.txt\.bz2$")

    # Dictionary to store the count of articles per ISO code
    iso_article_counts = {}

    # Iterate over all files in the specified directory
    for file in dump_path.iterdir():
        if file.is_file():
            match = pattern.match(file.name)
            if match:
                wiki_code = match.group(1)

                # Get ISO code using the loaded mapping
                iso_code = iso_mapping.get(wiki_code, "Unknown")

                try:
                    # Open the bz2-compressed index file in text mode
                    with bz2.open(file, "rt", encoding="utf-8") as f:
                        # Count the number of lines, each representing an article
                        article_count = sum(1 for _ in f)

                    # Add the article count to the ISO code entry
                    if iso_code in iso_article_counts:
                        iso_article_counts[iso_code] += article_count
                    else:
                        iso_article_counts[iso_code] = article_count

                except Exception as e:
                    print(f"Error processing '{file.name}': {e}")

    return iso_article_counts


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
    output_file = "./dicts/iso_article_counts.json"

    # Load the ISO mapping
    iso_mapping = load_iso_mapping()

    # Get article counts per language and map to ISO codes
    iso_article_counts = count_articles_per_language(dump_directory, iso_mapping)

    # Save the result
    save_iso_article_counts(iso_article_counts, output_file)
    print(f"ISO-to-article-count mapping saved to {output_file}")


if __name__ == "__main__":
    main()
