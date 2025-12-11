#!/usr/bin/env python
"""
replicated_classifier.py

Creates a TSV from data/master_features_by_code.csv with a `seed_label` column
populated from langdeath/classification/seed_data (files: g,h,s,t,v). Then
loads the original classifier implementation and runs the same training
pipeline (LogisticRegression / MaxEnt-style with SelectFromModel).

Usage (defaults target repo layout):
  python replicated_classifier.py

You can also pass --master-csv, --seed-dir, and other classifier-alike args.
"""
import os
import argparse
import logging
import pandas as pd
import importlib.util
import sys


def read_seed_map(seed_dir):
    """Read seed files named exactly 'g','h','s','t','v' from seed_dir and
    return a dict mapping code -> label.
    """
    mapping = {}
    for label in ['g', 'h', 's', 't', 'v']:
        path = os.path.join(seed_dir, label)
        if not os.path.exists(path):
            continue
        with open(path, 'r', encoding='utf-8') as fh:
            for line in fh:
                code = line.strip()
                if not code:
                    continue
                mapping[code] = label
    return mapping


def numericise_features(df, keep_cols):
    # convert everything except keep_cols to numeric where possible
    feats = df.drop(columns=keep_cols)
    feats = feats.apply(pd.to_numeric, errors='coerce').fillna(0)
    result = pd.concat([df[keep_cols].reset_index(drop=True), feats.reset_index(drop=True)], axis=1)
    return result


def load_classifier_module(path_to_classifier_py):
    spec = importlib.util.spec_from_file_location('ld_classifier', path_to_classifier_py)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def build_temp_tsv(master_csv, seed_dir, tmp_tsv_path):
    df = pd.read_csv(master_csv)

    # create integrated_code column expected by Classifier
    if 'code' in df.columns:
        df['integrated_code'] = df['code']
    elif 'iso639_3' in df.columns:
        df['integrated_code'] = df['iso639_3']
    else:
        raise ValueError('master CSV does not contain `code` or `iso639_3` column')

    seed_map = read_seed_map(seed_dir)
    df['seed_label'] = df['integrated_code'].map(seed_map).fillna('-')

    # drop obvious textual columns that aren't features
    drop_cols = []
    for c in ['iso639_3', 'glottocode', 'code']:
        if c in df.columns:
            drop_cols.append(c)

    # keep integrated_code and seed_label, convert others to numeric
    keep_cols = ['integrated_code', 'seed_label']
    df = df.drop(columns=[c for c in drop_cols if c not in keep_cols], errors='ignore')

    df = numericise_features(df, keep_cols)

    # write TSV that matches the format classifier expects
    os.makedirs(os.path.dirname(tmp_tsv_path), exist_ok=True)
    df.to_csv(tmp_tsv_path, sep='\t', index=False, encoding='utf-8')
    return tmp_tsv_path


def get_default_paths():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    master_csv = os.path.join(repo_root, 'data', 'master_features_by_code.csv')
    seed_dir = os.path.join(repo_root, 'langdeath', 'classification', 'seed_data')
    classifier_py = os.path.join(repo_root, 'langdeath', 'classification', 'classifier.py')
    out_template = os.path.join(repo_root, 'scripts', 'replicated_results', 'replicated')
    tmp_tsv = os.path.join(repo_root, 'scripts', 'tmp_master_for_classification.tsv')
    return master_csv, seed_dir, classifier_py, out_template, tmp_tsv


def main():
    master_csv_def, seed_dir_def, classifier_py_def, out_template_def, tmp_tsv_def = get_default_paths()

    parser = argparse.ArgumentParser(description='Replicated classifier runner using master_features_by_code.csv')
    parser.add_argument('--master-csv', default=master_csv_def)
    parser.add_argument('--seed-dir', default=seed_dir_def)
    parser.add_argument('--classifier-py', default=classifier_py_def)
    parser.add_argument('--out-template', default=out_template_def)
    parser.add_argument('--tmp-tsv', default=tmp_tsv_def)
    parser.add_argument('-e', '--experiment_count', type=int, default=20)
    parser.add_argument('-c', '--class_counts', type=int, default=2, choices=[2,3,4,5])
    parser.add_argument('-t', '--threshold', type=float, default=0.9)
    parser.add_argument('-l', '--log-file', default=os.path.join(os.path.dirname(__file__), 'replicated_classifier.log'))
    parser.add_argument('-s', '--status', action='store_true')
    parser.add_argument('-r', '--regularizaton_weight', type=float, default=None)
    args = parser.parse_args()

    # tolerate users passing quoted paths (e.g. 'C:\path\file') on Windows
    def _strip_quotes(x):
        # remove surrounding whitespace and any single/double quotes that
        # might have been introduced by nested shell quoting
        if isinstance(x, str):
            return x.strip(" '\"")
        return x

    args.master_csv = _strip_quotes(args.master_csv)
    args.seed_dir = _strip_quotes(args.seed_dir)
    args.classifier_py = _strip_quotes(args.classifier_py)
    args.out_template = _strip_quotes(args.out_template)
    args.tmp_tsv = _strip_quotes(args.tmp_tsv)
    args.log_file = _strip_quotes(args.log_file)

    # build tmp TSV to feed the original Classifier
    tmp_tsv = build_temp_tsv(args.master_csv, args.seed_dir, args.tmp_tsv)

    # load the classifier module dynamically
    classifier_mod = load_classifier_module(args.classifier_py)

    # prepare logger
    logger = classifier_mod.get_logger(args.log_file)

    # instantiate and run the classifier with same interface
    # ensure regularization weight is a valid float (LogisticRegression requires a positive float)
    if args.regularizaton_weight is None:
        args.regularizaton_weight = 1.0

    # ensure output directory exists so the classifier can write files there
    out_dir = os.path.dirname(args.out_template)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    clf = classifier_mod.Classifier(tmp_tsv, args.experiment_count, args.class_counts,
                                    args.threshold, logger, args.out_template,
                                    args.status, float(args.regularizaton_weight))
    clf.train_classify()


if __name__ == '__main__':
    main()
