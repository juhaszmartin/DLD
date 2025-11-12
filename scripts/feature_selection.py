import os
import random
import pandas as pd
import numpy as np
from scipy.stats import ttest_ind

# === CONFIG ===
MASTER_CSV = "./data/master_features_by_code.csv"
SEED_DIR = "./langdeath/classification/seed_data"
SAMPLE_PER_CLASS = 50
OUTPUT_MATRIX = "./data/feature_class_significance.csv"

# === LOAD MASTER DATA ===
df = pd.read_csv(MASTER_CSV)

# if cols 'iso639_3' and 'glottocode' exist, remove them
for col in ["iso639_3", "glottocode"]:
    if col in df.columns:
        df = df.drop(columns=[col])


df = df.set_index(df.columns[0])  # assume first col is code
print(f"Loaded master data with {df.shape[0]} rows, {df.shape[1]} cols")

# Identify numeric feature columns
feature_cols = df.select_dtypes(include=[np.number]).columns.tolist()
print(f"Detected {len(feature_cols)} numeric features.")

# === LOAD CLASS SEEDS ===
seed_files = [f for f in os.listdir(SEED_DIR) if os.path.isfile(os.path.join(SEED_DIR, f))]
class_to_codes = {}

for sf in seed_files:
    label = os.path.splitext(sf)[0]  # e.g. 'g', 't', 's', etc.
    with open(os.path.join(SEED_DIR, sf), encoding="utf-8") as f:
        codes = [line.strip() for line in f if line.strip()]
    class_to_codes[label] = codes

print(f"Loaded {len(class_to_codes)} classes: {list(class_to_codes.keys())}")

# === SAMPLE DATA ===
samples = []
for label, codes in class_to_codes.items():
    subset = df.loc[df.index.intersection(codes)]
    if len(subset) == 0:
        print(f"⚠️ No matching rows found for class {label}")
        continue
    sample = subset.sample(n=min(SAMPLE_PER_CLASS, len(subset)), random_state=42)
    sample["class_label"] = label
    samples.append(sample)

if not samples:
    raise ValueError("No samples found — check your seed files and master data.")

combined = pd.concat(samples)
print(f"Combined sample size: {combined.shape[0]} rows, {combined.shape[1]} columns")

# === SIGNIFICANCE TESTING ===
classes = sorted(combined["class_label"].unique())
sig_matrix = pd.DataFrame(0, index=feature_cols, columns=classes)

for feat in feature_cols:
    for label in classes:
        group_a = combined[combined["class_label"] == label][feat].dropna()
        group_b = combined[combined["class_label"] != label][feat].dropna()
        if len(group_a) < 3 or len(group_b) < 3:
            continue  # skip small groups
        stat, pval = ttest_ind(group_a, group_b, equal_var=False)
        if pval < 0.05:
            sig_matrix.loc[feat, label] = 1

# === SAVE OUTPUT ===
os.makedirs(os.path.dirname(OUTPUT_MATRIX), exist_ok=True)
sig_matrix.to_csv(OUTPUT_MATRIX)
print(f"✅ Saved feature significance matrix to {OUTPUT_MATRIX}")