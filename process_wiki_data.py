import os
import json
import bz2
import re
import io
from pathlib import Path
from tqdm import tqdm
import statistics
import xml.etree.ElementTree as ET
import numpy as np
from collections import Counter

# TODO: threshold to exclude articles with low char. count

def load_iso_mapping():
    """
    Loads the dictionary that maps wiki codes to ISO codes.

    Returns:
        dict: A dictionary where the keys are wiki codes and values are ISO codes.
    """
    with open("./dicts/wiki_code_to_iso_code.json", "r", encoding="utf-8") as f:
        return json.load(f)
    

def compute_character_entropy(article_texts):
    """
    Computes the character entropy for a language based on its article texts.

    Args:
        article_texts (list of str): A list of article texts in the language.

    Returns:
        float: The character entropy of the language.
    """
    char_counts = Counter()
    for text in article_texts:
        char_counts.update(text)
    total_chars = sum(char_counts.values())
    entropy = 0.0
    for count in char_counts.values():
        p = count / total_chars
        entropy -= p * np.log2(p)
    return entropy


def parse_index_file(index_file):
    """
    Parses the index file and returns a sorted list of unique block offsets.

    Args:
        index_file (Path): Path to the index file.

    Returns:
        list: Sorted list of unique block offsets.
    """
    block_offsets = set()
    with bz2.open(index_file, "rt", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(":")
            if len(parts) >= 3:
                byte_offset = int(parts[0])
                block_offsets.add(byte_offset)
    return sorted(block_offsets)


def decompress_bz2_block(file_obj, start_offset, end_offset=None):
    """
    Decompresses a BZ2 block from the given file object starting at start_offset.

    Args:
        file_obj (file object): Opened file object in binary mode.
        start_offset (int): Start byte offset of the block.
        end_offset (int, optional): End byte offset of the block.

    Returns:
        bytes: Decompressed data of the block.
    """
    file_obj.seek(start_offset)
    compressed_data = file_obj.read() if end_offset is None else file_obj.read(end_offset - start_offset)
    decompressor = bz2.BZ2Decompressor()
    try:
        decompressed_data = decompressor.decompress(compressed_data)
    except OSError as e:
        # Handle incomplete or corrupt data
        decompressed_data = b""
    return decompressed_data
def calculate_article_lengths_with_index(dump_directory, iso_mapping):
    """
    Calculates various statistics for each language's Wikipedia, including adjusted article lengths,
    real ratios, adjusted Wikipedia sizes, and character entropy, using index files for optimization.

    Args:
        dump_directory (str or Path): Path to the directory containing Wikipedia dump files.
        iso_mapping (dict): Dictionary mapping Wikipedia language codes to ISO codes.

    Returns:
        tuple: A tuple containing dictionaries for each language mapping ISO codes to:
            - Average adjusted article lengths (iso_avg_lengths)
            - Median adjusted article lengths (iso_median_lengths)
            - Real ratios (iso_real_ratios)
            - Adjusted Wikipedia sizes (iso_adjusted_wikipedia_sizes)
            - Character entropy values (iso_entropy_values)
    """
    dump_path = Path(dump_directory)

    if not dump_path.is_dir():
        print(f"Error: '{dump_directory}' is not a valid directory.")
        return {}, {}, {}, {}, {}

    # Patterns to match the XML dump and index files
    xml_pattern = re.compile(r"^([a-z]{2,3})wiki-.*-pages-articles-multistream\.xml\.bz2$")
    index_pattern = re.compile(r"^([a-z]{2,3})wiki-.*-multistream-index\.txt\.bz2$")

    iso_avg_lengths = {}
    iso_median_lengths = {}
    iso_adjusted_wikipedia_sizes = {}
    iso_real_ratios = {}
    iso_entropy_values = {}

    wiki_files = {}
    for file in dump_path.iterdir():
        if file.is_file():
            xml_match = xml_pattern.match(file.name)
            index_match = index_pattern.match(file.name)
            if xml_match:
                wiki_code = xml_match.group(1)
                if wiki_code not in wiki_files:
                    wiki_files[wiki_code] = {}
                wiki_files[wiki_code]["dump"] = file
            elif index_match:
                wiki_code = index_match.group(1)
                if wiki_code not in wiki_files:
                    wiki_files[wiki_code] = {}
                wiki_files[wiki_code]["index"] = file

    # Process German first to get german_entropy
    german_entropy = None
    if 'de' in wiki_files:
        print("Processing German ('de') Wikipedia to compute German entropy.")
        files = wiki_files['de']
        iso_code = 'deu'  # ISO code for German
        dump_file = files.get("dump")
        index_file = files.get("index")
        if dump_file and index_file:
            # Process German Wikipedia to compute entropy
            article_texts = extract_article_texts(dump_file, index_file)
            if article_texts:
                german_entropy = compute_character_entropy(article_texts)
                iso_entropy_values[iso_code] = german_entropy
            else:
                print("No articles found in German Wikipedia.")
                german_entropy = 1.0  # Default value
        else:
            print("Missing dump or index file for German Wikipedia.")
            german_entropy = 1.0  # Default value
    else:
        print("German ('de') Wikipedia dump not found in the directory.")
        german_entropy = 1.0  # Default value

    for wiki_code, files in wiki_files.items():
        iso_code = iso_mapping.get(wiki_code, "Unknown")
        if "dump" not in files or "index" not in files:
            print(f"Missing dump or index file for wiki code '{wiki_code}'")
            continue

        dump_file = files["dump"]
        index_file = files["index"]

        print(f"Processing language '{iso_code}' with wiki code '{wiki_code}'")

        try:
            # Get the list of block offsets from the index file
            block_offsets = parse_index_file(index_file)

            article_lengths = []
            adjusted_lengths = []
            real_article_lengths = []
            article_texts = []

            with open(dump_file, "rb") as f:
                for i in tqdm(range(len(block_offsets)), desc=f"Processing blocks in {dump_file.name}"):
                    block_offset = block_offsets[i]
                    if i + 1 < len(block_offsets):
                        next_block_offset = block_offsets[i + 1]
                    else:
                        next_block_offset = None  # Read until the end of the file

                    # Decompress the block
                    decompressed_data = decompress_bz2_block(f, block_offset, next_block_offset)
                    if not decompressed_data:
                        continue

                    # Parse the XML data for this block
                    try:
                        context = ET.iterparse(io.BytesIO(decompressed_data), events=("end",))
                        for event, elem in context:
                            tag = elem.tag
                            if "}" in tag:
                                tag = tag.split("}", 1)[1]  # Strip namespace

                            if tag == "page":
                                ns = elem.find("ns")
                                if ns is not None and ns.text != '0':
                                    # Skip non-article pages (e.g., Talk, User pages)
                                    elem.clear()
                                    continue

                                revision = elem.find(".//revision")
                                if revision is not None:
                                    text = revision.find("text")
                                    if text is not None and text.text is not None:
                                        article_text = text.text
                                        article_length = len(article_text)
                                        article_lengths.append(article_length)
                                        article_texts.append(article_text)
                                elem.clear()
                        del context
                    except ET.ParseError:
                        # Handle parse errors if any
                        continue

            if not article_texts:
                print(f"No articles found for language '{iso_code}'. Skipping...")
                continue

            # Compute character entropy
            if iso_code == 'deu':
                # German entropy already computed
                language_entropy = german_entropy
            else:
                language_entropy = compute_character_entropy(article_texts)
                iso_entropy_values[iso_code] = language_entropy

            # Compute normalization factor
            normalization_factor = german_entropy / language_entropy if language_entropy != 0 else 1.0

            # Adjust article lengths and filter real articles
            real_articles = 0
            for length in article_lengths:
                adjusted_length = length * normalization_factor
                adjusted_lengths.append(adjusted_length)
                if adjusted_length >= 450:
                    real_articles += 1
                    real_article_lengths.append(adjusted_length)

            # Compute statistics
            if adjusted_lengths:
                avg_length = sum(adjusted_lengths) / len(adjusted_lengths)
                median_length = statistics.median(adjusted_lengths)
            else:
                avg_length = 0
                median_length = 0

            # Compute real ratio
            total_articles = len(article_lengths)
            real_ratio = real_articles / total_articles if total_articles > 0 else 0

            # Compute adjusted Wikipedia size
            adjusted_wikipedia_size = sum(real_article_lengths)

            # Store results
            iso_avg_lengths[iso_code] = avg_length
            iso_median_lengths[iso_code] = median_length
            iso_real_ratios[iso_code] = real_ratio
            iso_adjusted_wikipedia_sizes[iso_code] = adjusted_wikipedia_size

        except Exception as e:
            print(f"Error processing '{dump_file.name}': {e}")

    return iso_avg_lengths, iso_median_lengths, iso_real_ratios, iso_adjusted_wikipedia_sizes, iso_entropy_values


def extract_article_texts(dump_file, index_file):
    """
    Extracts article texts from a Wikipedia dump using index files for optimization.

    Args:
        dump_file (Path): Path to the Wikipedia dump XML file.
        index_file (Path): Path to the Wikipedia multistream index file.

    Returns:
        list of str: A list of article texts extracted from the dump.
    """
    article_texts = []

    # Get the list of block offsets from the index file
    block_offsets = parse_index_file(index_file)

    with open(dump_file, "rb") as f:
        for i in tqdm(range(len(block_offsets)), desc=f"Processing blocks in {dump_file.name}"):
            block_offset = block_offsets[i]
            if i + 1 < len(block_offsets):
                next_block_offset = block_offsets[i + 1]
            else:
                next_block_offset = None  # Read until the end of the file

            # Decompress the block
            decompressed_data = decompress_bz2_block(f, block_offset, next_block_offset)
            if not decompressed_data:
                continue

            # Parse the XML data for this block
            try:
                context = ET.iterparse(io.BytesIO(decompressed_data), events=("end",))
                for event, elem in context:
                    tag = elem.tag
                    if "}" in tag:
                        tag = tag.split("}", 1)[1]  # Strip namespace

                    if tag == "page":
                        ns = elem.find("ns")
                        if ns is not None and ns.text != '0':
                            # Skip non-article pages
                            elem.clear()
                            continue

                        revision = elem.find(".//revision")
                        if revision is not None:
                            text = revision.find("text")
                            if text is not None and text.text is not None:
                                article_text = text.text
                                article_texts.append(article_text)
                        elem.clear()
                del context
            except ET.ParseError:
                # Handle parse errors if any
                continue

    return article_texts


def count_articles_per_language(dump_directory, iso_mapping):
    """
    Counts the number of articles in each Wikipedia dump using index files and maps them to ISO codes.

    Args:
        dump_directory (str or Path): Path to the directory containing Wikipedia dump index files.
        iso_mapping (dict): Dictionary mapping Wikipedia language codes to ISO codes.

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
                index_file = dump_path / file.name

                try:
                    with bz2.open(index_file, "rt", encoding="utf-8") as f:
                        article_count = sum(1 for _ in f)

                    iso_article_counts[iso_code] = article_count

                except Exception as e:
                    print(f"Error processing '{file.name}': {e}")

    return iso_article_counts


def save_iso_article_counts(counts, output_file):
    """
    Saves the ISO-to-article-count mapping as a JSON file.

    Args:
        counts (dict): Dictionary mapping ISO codes to counts.
        output_file (str): Path to the output JSON file.
    """
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(counts, f, ensure_ascii=False, indent=4)


def main():
    """
    Main function to process Wikipedia dumps and compute various statistics per language.

    Steps:
        - Loads the ISO mapping.
        - Counts the number of articles per language.
        - Calculates average and median article lengths, real ratios, adjusted Wikipedia sizes, and character entropy.
        - Prepares the s1 matrix with the collected features.
        - Saves all results to JSON and CSV files.
    """
    dump_directory = "./downloads"
    count_output_file = "./dicts/iso_article_counts.json"
    avg_length_output_file = "./dicts/iso_avg_article_lengths.json"
    median_length_output_file = "./dicts/iso_median_article_lengths.json"
    real_ratio_output_file = "./dicts/iso_real_ratios.json"
    adjusted_wikipedia_size_output_file = "./dicts/iso_adjusted_wikipedia_sizes.json"
    entropy_output_file = "./dicts/iso_entropy_values.json"
    s1_output_file = "./s1_matrix.csv"

    iso_mapping = load_iso_mapping()

    # Get article counts per language from index files and map to ISO codes
    iso_article_counts = count_articles_per_language(dump_directory, iso_mapping)
    save_iso_article_counts(iso_article_counts, count_output_file)
    print(f"ISO-to-article-count mapping saved to {count_output_file}")

    # Calculate average and median article lengths, real ratios, adjusted sizes, and entropy
    (iso_avg_article_lengths, iso_median_article_lengths, iso_real_ratios,
     iso_adjusted_wikipedia_sizes, iso_entropy_values) = calculate_article_lengths_with_index(dump_directory, iso_mapping)

    save_iso_article_counts(iso_avg_article_lengths, avg_length_output_file)
    print(f"ISO-to-average-article-length mapping saved to {avg_length_output_file}")

    save_iso_article_counts(iso_median_article_lengths, median_length_output_file)
    print(f"ISO-to-median-article-length mapping saved to {median_length_output_file}")

    save_iso_article_counts(iso_real_ratios, real_ratio_output_file)
    print(f"ISO-to-real-ratio mapping saved to {real_ratio_output_file}")

    save_iso_article_counts(iso_adjusted_wikipedia_sizes, adjusted_wikipedia_size_output_file)
    print(f"ISO-to-adjusted-wikipedia-size mapping saved to {adjusted_wikipedia_size_output_file}")

    save_iso_article_counts(iso_entropy_values, entropy_output_file)
    print(f"ISO-to-entropy mapping saved to {entropy_output_file}")


if __name__ == "__main__":
    main()
