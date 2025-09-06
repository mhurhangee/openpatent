#!/usr/bin/env python
"""
coarse_clean_patents.py

Command-line script for cleaning European Patent (EP) XML documents
for use in NLP tasks, such as pretraining language models.

Usage:
    python clean_patents.py --input_folder <XML_ROOT_FOLDER> --output_file <OUTPUT_JSONL> [--workers N]

Arguments:
    --input_folder : str
        Top-level folder containing EP XML patent files. The script will recursively
        scan all subfolders for XML files.
    --output_file  : str
        Path to save the cleaned output as a JSONL file. Each line is a JSON object
        with keys "filename", "description", and "claim1".
    --workers      : int, optional (default=1)
        Number of parallel processes to use. Increase for faster processing on multi-core machines.

Processing / Cleaning Steps:
1. Only English descriptions are processed:
    - XML elements <description lang="en"> are required; non-English documents are skipped.
    - Only the first English claim <claims lang="en"><claim num="0001"> is included.
2. Text extraction:
    - Extracts text recursively from child elements.
    - Paragraphs (<p>) are preserved with "\n\n" separators.
3. Special token replacement:
    - Certain XML tags are replaced with tokens and contents are dropped:
        <table> -> <TAB>
        <img> -> <IMG>
        <figref> -> <FIG>
        <patcit> -> <CIT>
        <nplcit> -> <NPL>
        <chemistry> -> <CHM>
        <maths> -> <MAT>
        <heading> -> <HEAD>
        <ol>, <ul>, <dl> -> dropped entirely
4. Number normalization:
    - Numbers with optional SI units (e.g., "20 mm", "3.5 kg") are replaced with <NUM>.
    - Numbers inside words like "CO2" are preserved.
5. Whitespace normalization:
    - Redundant spaces are removed.
    - Paragraph separation is maintained with double newlines.
6. Optional first and last paragraph removal can be added downstream to reduce boilerplate.
7. Output:
    - Saved as JSONL, one patent per line, suitable for downstream NLP processing.

Notes:
- The script uses lxml with comment removal.
- Parallel processing is supported via ProcessPoolExecutor.
- Memory usage is modest since each XML file is processed independently.
"""


import os
import json
import re
import argparse
from lxml import etree
from tqdm.auto import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

# ---------- Special tokens and number units ----------
SPECIAL_TOKENS = {
    "table": "<TAB>",
    "img": "<IMG>",
    "figref": "<FIG>",
    "patcit": "<CIT>",
    "nplcit": "<NPL>",
    "chemistry": "<CHM>",
    "maths": "<MAT>",
    "heading": "<HEAD>",
    "ol": "",
    "ul": "",
    "dl": ""
}

UNITS = [
    "mm", "cm", "m", "km",
    "mg", "g", "kg",
    "ml", "l",
    "s", "ms", "µs", "us", "min", "h",
    "hz", "khz", "mhz", "ghz",
    "°c", "k",
    "µm", "um", "nm",
]
UNIT_PATTERN = "|".join(map(re.escape, UNITS))
NUM_REGEX = re.compile(
    rf"(?<![A-Za-z])\b\d+(\.\d+)?\s*(?:{UNIT_PATTERN})?\b(?![A-Za-z])",
    flags=re.IGNORECASE,
)
xml_parser = etree.XMLParser(remove_comments=True, recover=True)

# ---------- Text processing functions ----------
def normalize_numbers(text: str) -> str:
    if not text:
        return ""
    return NUM_REGEX.sub("<NUM>", text)

def normalize_whitespace_preserve_paragraphs(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def extract_with_tokens(elem, preserve_paragraphs=False):
    if elem.tag in SPECIAL_TOKENS:
        return SPECIAL_TOKENS[elem.tag]
    if preserve_paragraphs and elem.tag == "p":
        parts = []
        if elem.text:
            parts.append(normalize_numbers(elem.text))
        for child in elem:
            parts.append(extract_with_tokens(child, preserve_paragraphs))
            if child.tail:
                parts.append(normalize_numbers(child.tail))
        return "".join(parts).strip()
    parts = []
    if elem.text:
        parts.append(normalize_numbers(elem.text))
    for child in elem:
        parts.append(extract_with_tokens(child, preserve_paragraphs))
        if child.tail:
            parts.append(normalize_numbers(child.tail))
    return "".join(parts)

def drop_first_last_paragraphs(text: str) -> str:
    paragraphs = text.split("\n\n")
    if len(paragraphs) > 2:
        paragraphs = paragraphs[1:-1]
    return "\n\n".join(paragraphs).strip()

# ---------- Process a single file ----------
def process_file(path):
    try:
        tree = etree.parse(path, xml_parser)
        root = tree.getroot()

        desc = root.find(".//description[@lang='en']")
        if desc is None:
            return None

        # paragraphs
        paras = [extract_with_tokens(p, preserve_paragraphs=True) for p in desc.findall(".//p")]
        paras = [re.sub(r"[ \t]+", " ", p).strip() for p in paras if p]
        desc_text = "\n\n".join(paras) if paras else extract_with_tokens(desc)
        desc_text = normalize_whitespace_preserve_paragraphs(desc_text)
        desc_text = drop_first_last_paragraphs(desc_text)

        # first claim
        claim1_elem = root.find(".//claims[@lang='en']/claim[@num='0001']")
        claim1_text = ""
        if claim1_elem is not None:
            claim1_text = extract_with_tokens(claim1_elem)
            claim1_text = normalize_whitespace_preserve_paragraphs(claim1_text)

        if desc_text.strip() or claim1_text.strip():
            return {
                "description": desc_text,
                "claim1": claim1_text
            }
    except Exception as e:
        print(f"Error parsing {path}: {e}")
    return None

# ---------- Main ----------
def main(input_folder, output_file, workers=1):
    xml_files = [os.path.join(dp, f) for dp, dn, fn in os.walk(input_folder) for f in fn if f.endswith(".xml")]
    with open(output_file, "w", encoding="utf-8") as fout:
        if workers == 1:
            for f in tqdm(xml_files, desc="Processing XML"):
                rec = process_file(f)
                if rec:
                    fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
        else:
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(process_file, f): f for f in xml_files}
                for fut in tqdm(as_completed(futures), total=len(futures), desc="Processing XML"):
                    rec = fut.result()
                    if rec:
                        fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--input_folder", type=str, required=True, help="Top-level XML folder")
    arg_parser.add_argument("--output_file", type=str, required=True, help="Output JSONL file")
    arg_parser.add_argument("--workers", type=int, default=1, help="Number of parallel workers")
    args = arg_parser.parse_args()

    main(args.input_folder, args.output_file, args.workers)
