#!/usr/bin/env python3
"""
raw_count.py

A parallel XML character counting script using joblib to process patent XML files
from the EPO publication server. This serves as a foundation for building robust
patent processing pipelines that can handle 400k+ documents efficiently.

Usage:
    python raw_count.py [--xml-dir XML_DIR] [--workers N] [--verbose]

Arguments:
    --xml-dir    : str, optional (default: ./xml)
        Directory containing XML files and subdirectories to process
    --workers    : int, optional (default: -1, uses all CPU cores)
        Number of parallel workers for processing
    --verbose    : bool, optional (default: False)
        Enable verbose output showing progress per file

Example:
    python raw_count.py --xml-dir ./xml --workers 8 --verbose
"""

import argparse
import os
import sys
import time
from pathlib import Path
from typing import List, Tuple, Dict
import logging

from joblib import Parallel, delayed
from tqdm import tqdm


def setup_logging(verbose: bool = False) -> None:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )


def find_xml_files(xml_dir: Path) -> List[Path]:
    """
    Recursively find all XML files in the given directory.
    
    Args:
        xml_dir: Path to the XML directory
        
    Returns:
        List of Path objects for all XML files found
    """
    xml_files = []
    
    if not xml_dir.exists():
        raise FileNotFoundError(f"XML directory not found: {xml_dir}")
    
    # Recursively find all .xml files
    for xml_file in xml_dir.rglob("*.xml"):
        if xml_file.is_file():
            xml_files.append(xml_file)
    
    return sorted(xml_files)


def count_file_characters(xml_file: Path) -> Tuple[str, int, bool]:
    """
    Count characters in a single XML file.
    
    Args:
        xml_file: Path to the XML file
        
    Returns:
        Tuple of (filename, character_count, success_flag)
    """
    try:
        with open(xml_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            char_count = len(content)
        return (str(xml_file), char_count, True)
    
    except Exception as e:
        logging.error(f"Error processing {xml_file}: {e}")
        return (str(xml_file), 0, False)


def process_xml_files(xml_files: List[Path], n_jobs: int = -1, verbose: bool = False) -> Dict[str, int]:
    """
    Process XML files in parallel to count characters.
    
    Args:
        xml_files: List of XML file paths
        n_jobs: Number of parallel jobs (-1 uses all cores)
        verbose: Enable verbose progress output
        
    Returns:
        Dictionary mapping file paths to character counts
    """
    if not xml_files:
        logging.warning("No XML files found to process")
        return {}
    
    logging.info(f"Processing {len(xml_files)} XML files with {n_jobs if n_jobs > 0 else 'all available'} workers")
    
    start_time = time.time()
    
    # Use joblib to process files in parallel with progress bar
    results = Parallel(n_jobs=n_jobs, verbose=1 if verbose else 0)(
        delayed(count_file_characters)(xml_file) 
        for xml_file in tqdm(xml_files, desc="Processing XML files", disable=not verbose)
    )
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    # Collect results and statistics
    char_counts = {}
    successful_files = 0
    failed_files = 0
    total_characters = 0
    
    for filename, char_count, success in results:
        char_counts[filename] = char_count
        total_characters += char_count
        
        if success:
            successful_files += 1
        else:
            failed_files += 1
    
    # Log summary statistics
    logging.info(f"\n{'='*60}")
    logging.info(f"PROCESSING SUMMARY")
    logging.info(f"{'='*60}")
    logging.info(f"Total files processed: {len(xml_files)}")
    logging.info(f"Successful: {successful_files}")
    logging.info(f"Failed: {failed_files}")
    logging.info(f"Total characters: {total_characters:,}")
    logging.info(f"Average characters per file: {total_characters // successful_files if successful_files > 0 else 0:,}")
    logging.info(f"Processing time: {processing_time:.2f} seconds")
    logging.info(f"Files per second: {len(xml_files) / processing_time:.2f}")
    logging.info(f"Characters per second: {total_characters / processing_time:,.0f}")
    logging.info(f"{'='*60}")
    
    return char_counts


def main():
    """Main function to parse arguments and run the character counting."""
    parser = argparse.ArgumentParser(
        description="Count characters in XML files using parallel processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--xml-dir",
        type=str,
        default="./xml",
        help="Directory containing XML files (default: ./xml)"
    )
    
    parser.add_argument(
        "--workers",
        type=int,
        default=-1,
        help="Number of parallel workers (-1 uses all cores, default: -1)"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    try:
        # Find XML files
        xml_dir = Path(args.xml_dir)
        xml_files = find_xml_files(xml_dir)
        
        if not xml_files:
            logging.error(f"No XML files found in {xml_dir}")
            sys.exit(1)
        
        # Process files
        char_counts = process_xml_files(xml_files, args.workers, args.verbose)
        
        # Optionally save detailed results to file
        if args.verbose:
            output_file = Path("raw_count_results.txt")
            with open(output_file, 'w') as f:
                f.write("File Path\tCharacter Count\n")
                for filepath, count in sorted(char_counts.items()):
                    f.write(f"{filepath}\t{count}\n")
            logging.info(f"Detailed results saved to {output_file}")
        
    except KeyboardInterrupt:
        logging.info("Processing interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()