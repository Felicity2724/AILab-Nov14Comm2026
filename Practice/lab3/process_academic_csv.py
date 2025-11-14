#!/usr/bin/env python3
"""Process academic CSV: deduplicate by title, detect moral theories, create outputs.

Usage:
  python3 process_academic_csv.py --input /path/to/input.csv --output_dir /path/to/output
"""
import argparse
import os
import re
import sys
from collections import Counter

try:
    import pandas as pd
except Exception:
    print("pandas is required. Install with: pip install pandas", file=sys.stderr)
    raise

THEORY_KEYWORDS = {
    'Utilitarianism': [r'utilitarian', r'consequent', r'consequential', r'greatest good', r'maxim', r'utility'],
    'Deontology': [r'deontol', r'duty', r'kant', r'rights', r'obligation'],
    'Virtue Ethics': [r'virtue', r'virtues', r'character', r'aristot', r'excellenc'],
    'Care Ethics': [r'care ethics', r'care', r'relational', r'care-based', r'care-based'],
    'Principlism': [r'principlism', r'autonom', r'beneficence', r'non-?maleficence', r'justice'],
    'Contractualism': [r'contract', r'contractualis', r'rawls', r'fairness'],
    'Moral Foundations': [r'moral foundation', r'moral foundations', r'fairness', r'loyalty', r'authority', r'sanctity', r'care'],
    'Consequentialism': [r'consequent', r'consequentialism'],
    'Care-orientation': [r'care-based', r'care ethics'],
    'Feminist Ethics': [r'feminist'],
    'Moral Psychology': [r'moral psychology', r'moral-psycholog'],
}

COMPILED_PATTERNS = {t: [re.compile(pat, re.I) for pat in pats] for t, pats in THEORY_KEYWORDS.items()}


def detect_theories(text):
    if not isinstance(text, str) or not text.strip():
        return []
    found = set()
    for theory, patterns in COMPILED_PATTERNS.items():
        for pat in patterns:
            if pat.search(text):
                found.add(theory)
                break
    return sorted(found)


def normalize_title(title):
    if not isinstance(title, str):
        return ''
    t = title.strip().lower()
    # collapse whitespace and punctuation normalization
    t = re.sub(r"\s+", " ", t)
    return t


def likely_journal_article(row):
    # Heuristic: has a non-empty Venue and Venue is not 'arXiv' or empty string
    venue = row.get('Venue', '')
    source = row.get('Source Database', '')
    if isinstance(venue, str) and venue.strip() and 'arxiv' not in venue.lower():
        return True
    if isinstance(source, str) and source.lower() in ('semantic scholar', 'scopus', 'pubmed', 'openalex'):
        return True
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i', required=True, help='Input CSV file')
    parser.add_argument('--output_dir', '-o', default='output', help='Output directory')
    args = parser.parse_args()

    input_csv = args.input
    out_dir = args.output_dir
    os.makedirs(out_dir, exist_ok=True)

    print(f"Reading {input_csv}")
    df = pd.read_csv(input_csv, dtype=str)
    print(f"Rows loaded: {len(df)}")

    # Normalize titles and deduplicate
    df['__norm_title'] = df.get('Title', '').apply(normalize_title)
    before = len(df)
    df = df.drop_duplicates(subset=['__norm_title'], keep='first').copy()
    after = len(df)
    print(f"Deduplicated by Title: {before} -> {after}")

    # Detect theories using Title + Abstract
    df['text_for_detection'] = (df.get('Title', '').fillna('') + '. ' + df.get('Abstract', '').fillna(''))
    df['moral_theories_list'] = df['text_for_detection'].apply(detect_theories)
    df['moral_theories'] = df['moral_theories_list'].apply(lambda lst: '; '.join(lst) if lst else '')

    # Mark likely journal articles
    df['likely_journal_article'] = df.apply(likely_journal_article, axis=1)

    # Save CSV
    out_csv = os.path.join(out_dir, 'academic_search_results_with_theories.csv')
    cols_to_save = list(df.columns)
    df.to_csv(out_csv, index=False)
    print(f"Saved annotated CSV to {out_csv}")

    # Produce a small markdown summary
    total = len(df)
    num_journal = int(df['likely_journal_article'].sum()) if 'likely_journal_article' in df else 0
    theory_counter = Counter()
    for lst in df['moral_theories_list']:
        for t in lst:
            theory_counter[t] += 1

    summary_lines = []
    summary_lines.append('# Summary of Screening and Moral Theory Detection')
    summary_lines.append('')
    summary_lines.append(f'- Input file: `{input_csv}`')
    summary_lines.append(f'- Total records after deduplication: **{total}**')
    summary_lines.append(f'- Records likely journal articles: **{num_journal}**')
    summary_lines.append('')
    summary_lines.append('## Top detected moral theories')
    summary_lines.append('')
    if theory_counter:
        for theory, cnt in theory_counter.most_common(20):
            summary_lines.append(f'- **{theory}**: {cnt} articles')
    else:
        summary_lines.append('- No moral theories detected using basic keyword heuristics.')

    summary_lines.append('')
    summary_lines.append('## Examples (first 10 records)')
    summary_lines.append('')
    sample = df.head(10)
    for _, r in sample.iterrows():
        title = r.get('Title', '')
        venue = r.get('Venue', '')
        theories = r.get('moral_theories', '')
        summary_lines.append(f'- `{title}` — Venue: `{venue}` — Theories: `{theories}`')

    summary_md = '\n'.join(summary_lines)
    out_md = os.path.join(out_dir, 'summary.md')
    with open(out_md, 'w', encoding='utf-8') as f:
        f.write(summary_md)
    print(f"Saved summary to {out_md}")

    print('Done.')


if __name__ == '__main__':
    main()
