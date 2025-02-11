import json
import re
import statistics
from pathlib import Path
from tqdm import tqdm
from collections import Counter
import numpy as np
import multiprocessing
import mwxml
import bz2  

def load_iso_mapping():
    """
    Loads the dictionary that maps Wikipedia language codes to ISO 639-3 codes.

    Returns:
        dict: A dictionary where the keys are Wikipedia language codes (e.g., 'en') and values are ISO 639-3 codes (e.g., 'eng').
    """
    with open("./dicts/wiki_code_to_iso_code.json", "r", encoding="utf-8") as f:
        return json.load(f)

def compute_entropy_from_counts(char_counts, total_chars):
    """
    Computes the character entropy from character counts.

    Args:
        char_counts (Counter): Counter of character counts.
        total_chars (int): Total number of characters.

    Returns:
        float: Character entropy.
    """
    entropy = 0.0
    for count in char_counts.values():
        p = count / total_chars
        entropy -= p * np.log2(p)
    return entropy

def process_language(args):
    """
    Processes a language's Wikipedia dump using mwxml and counts articles.

    Args:
        args (tuple): (wiki_code, dump_file, iso_mapping, german_entropy)

    Returns:
        dict: Results containing statistics for the language.
    """
    wiki_code, dump_file, iso_mapping, german_entropy = args
    iso_code = iso_mapping.get(wiki_code, "Unknown")

    if not dump_file:
        print(f"No dump file for wiki code '{wiki_code}'")
        return None

    article_lengths = []
    char_counts_total = Counter()
    article_count = 0  # Initialize article count

    try:
        dump_file_path = dump_file
        language_name = iso_code

        def process_page(page):
            nonlocal article_count
            # Skip non-article pages
            if page.namespace != 0:
                return

            article_count += 1  # Increment article count

            for revision in reversed(page):  # Reverse to get the latest revision first
                text = revision.text
                if text:
                    article_length = len(text)
                    char_counts = Counter(text)
                    article_lengths.append(article_length)
                    char_counts_total.update(char_counts)
                    break  # Only process the latest revision
            return

        # Open the compressed file using bz2.open() in binary mode
        with bz2.open(str(dump_file_path), 'rb') as f:
            dump = mwxml.Dump.from_file(f)
            for page in tqdm(dump, desc=f"Processing {language_name} Wikipedia"):
                process_page(page)

        if not article_lengths:
            print(f"No articles found for language '{iso_code}'.")
            return None

        # Compute character entropy
        total_chars = sum(char_counts_total.values())
        entropy = compute_entropy_from_counts(char_counts_total, total_chars)

        # Compute normalization factor
        if iso_code == 'deu':
            normalization_factor = 1.0
        else:
            normalization_factor = german_entropy / entropy if entropy != 0 else 1.0

        # Adjust article lengths and filter real articles
        adjusted_lengths = [length * normalization_factor for length in article_lengths]
        real_article_lengths = [alength for alength in adjusted_lengths if alength >= 450]
        real_articles = len(real_article_lengths)

        # Compute statistics
        avg_length = sum(adjusted_lengths) / len(adjusted_lengths) if adjusted_lengths else 0
        median_length = statistics.median(adjusted_lengths) if adjusted_lengths else 0

        # Compute real ratio
        total_articles = len(article_lengths)
        real_ratio = real_articles / total_articles if total_articles > 0 else 0

        # Compute adjusted Wikipedia size
        adjusted_wikipedia_size = sum(real_article_lengths)

        # Store results
        result = {
            'iso_code': iso_code,
            'article_count': article_count,  # Include article count
            'avg_length': avg_length,
            'median_length': median_length,
            'real_ratio': real_ratio,
            'adjusted_wikipedia_size': adjusted_wikipedia_size,
            'entropy': entropy,
        }

        return result

    except Exception as e:
        print(f"Error processing '{dump_file.name}': {e}")
        return None

def calculate_article_lengths_with_index(dump_directory, iso_mapping):
    """
    Calculates various statistics for each language's Wikipedia, including adjusted article lengths,
    real ratios, adjusted Wikipedia sizes, and character entropy.

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
            - Article counts (iso_article_counts)
    """
    dump_path = Path(dump_directory)

    if not dump_path.is_dir():
        print(f"Error: '{dump_directory}' is not a valid directory.")
        return {}, {}, {}, {}, {}, {}

    # Patterns to match the XML dump files
    xml_pattern = re.compile(r"^([a-z]{2,3})wiki-.*-pages-articles-multistream\.xml\.bz2$")

    # Initialize dictionaries to store statistics
    iso_avg_lengths = {}
    iso_median_lengths = {}
    iso_adjusted_wikipedia_sizes = {}
    iso_real_ratios = {}
    iso_entropy_values = {}
    iso_article_counts = {}

    # Collect all dump files
    wiki_files = {}
    for file in dump_path.iterdir():
        if file.is_file():
            xml_match = xml_pattern.match(file.name)
            if xml_match:
                wiki_code = xml_match.group(1)
                if wiki_code not in wiki_files:
                    wiki_files[wiki_code] = {}
                wiki_files[wiki_code]["dump"] = file

    # Identify German dump
    german_dump = wiki_files.get('de', {}).get('dump', None)

    # === Step 1: Process German Dump Sequentially ===
    if german_dump:
        print("Processing German ('deu') Wikipedia to compute German entropy.")
        german_args = ('de', german_dump, iso_mapping, None)
        german_result = process_language(german_args)

        if german_result:
            german_entropy = german_result['entropy']
            iso_code = german_result['iso_code']
            iso_entropy_values[iso_code] = german_entropy
            iso_article_counts[iso_code] = german_result['article_count']

            # Store results for German
            iso_avg_lengths[iso_code] = german_result['avg_length']
            iso_median_lengths[iso_code] = german_result['median_length']
            iso_real_ratios[iso_code] = german_result['real_ratio']
            iso_adjusted_wikipedia_sizes[iso_code] = german_result['adjusted_wikipedia_size']
        else:
            print("No articles found in German Wikipedia.")
            german_entropy = 1.0  # Default value
    else:
        print("German ('deu') Wikipedia dump not found in the directory.")
        german_entropy = 1.0  # Default value

    # === Step 2: Prepare Arguments for Other Languages ===
    other_args = []
    for wiki_code, files in wiki_files.items():
        if wiki_code == 'de':
            continue  # Already processed
        dump_file = files.get("dump", None)
        if not dump_file:
            print(f"Missing dump file for wiki code '{wiki_code}'")
            continue
        # Pass the computed german_entropy for normalization
        other_args.append((wiki_code, dump_file, iso_mapping, german_entropy))

    # === Step 3: Process Other Languages in Parallel ===
    if other_args:
        num_cpus = multiprocessing.cpu_count()
        print(f"Using {num_cpus} CPU cores for multiprocessing of other languages.")
        with multiprocessing.Pool(processes=num_cpus) as pool_other:
            # Use imap_unordered for better performance and progress tracking
            for result in pool_other.imap_unordered(process_language, other_args):
                if result is None:
                    continue
                iso_code = result['iso_code']
                iso_article_counts[iso_code] = result['article_count']
                iso_avg_lengths[iso_code] = result['avg_length']
                iso_median_lengths[iso_code] = result['median_length']
                iso_real_ratios[iso_code] = result['real_ratio']
                iso_adjusted_wikipedia_sizes[iso_code] = result['adjusted_wikipedia_size']
                iso_entropy_values[iso_code] = result['entropy']

    return iso_avg_lengths, iso_median_lengths, iso_real_ratios, iso_adjusted_wikipedia_sizes, iso_entropy_values, iso_article_counts

def save_iso_article_counts(counts, output_file):
    """
    Saves the ISO-to-count mapping as a JSON file.

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
        - Processes each dump to calculate statistics and count articles.
        - Saves all results to JSON files.
    """
    dump_directory = "/mnt/c/matek_msc/AML/DLD/DLD/downloads"
    count_output_file = "./dicts/iso_article_counts.json"
    avg_length_output_file = "./dicts/iso_avg_article_lengths.json"
    median_length_output_file = "./dicts/iso_median_article_lengths.json"
    real_ratio_output_file = "./dicts/iso_real_ratios.json"
    adjusted_wikipedia_size_output_file = "./dicts/iso_adjusted_wikipedia_sizes.json"
    entropy_output_file = "./dicts/iso_entropy_values.json"

    iso_mapping = load_iso_mapping()

    # Calculate statistics and count articles
    (iso_avg_article_lengths, iso_median_article_lengths, iso_real_ratios,
     iso_adjusted_wikipedia_sizes, iso_entropy_values, iso_article_counts) = calculate_article_lengths_with_index(dump_directory, iso_mapping)

    # Save all results
    save_iso_article_counts(iso_article_counts, count_output_file)
    print(f"ISO-to-article-count mapping saved to {count_output_file}")

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
