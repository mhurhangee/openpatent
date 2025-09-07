#!/usr/bin/env python3
"""
raw_count_typer.py

A parallel XML character counting script using typer and joblib to process patent XML files
from the EPO publication server. Extracts patent IDs from XML and saves results as CSV.

This serves as a foundation for building robust patent processing pipelines that can 
handle 400k+ documents efficiently.

Example:
    python raw_count_typer.py --xml-dir ./xml --workers 8 --verbose
"""

import csv
import re
import sys
import time
from pathlib import Path
from typing import List, Tuple, Dict, Optional

import typer
from typing_extensions import Annotated
from joblib import Parallel, delayed
from tqdm import tqdm
from lxml import etree


app = typer.Typer(help="Count characters in patent XML files using parallel processing")


def extract_patent_id(xml_file: Path) -> Optional[str]:
    """
    Extract the patent ID from the ep-patent-document root element.
    
    Args:
        xml_file: Path to the XML file
        
    Returns:
        Patent ID string or None if not found
    """
    try:
        # Parse just enough of the XML to get the root element attributes
        with open(xml_file, 'rb') as f:
            # Read first few lines to find the ep-patent-document tag
            content = f.read(2048)  # Should be enough for the root tag
            
        # Use regex to extract the id attribute from ep-patent-document tag
        pattern = r'<ep-patent-document[^>]+id="([^"]+)"'
        match = re.search(pattern, content.decode('utf-8', errors='ignore'))
        
        if match:
            return match.group(1)
        else:
            typer.echo(f"Warning: No patent ID found in {xml_file}", err=True)
            return None
            
    except Exception as e:
        typer.echo(f"Error extracting patent ID from {xml_file}: {e}", err=True)
        return None


def count_file_characters(xml_file: Path) -> Tuple[Optional[str], str, int, bool]:
    """
    Count characters in a single XML file and extract patent ID.
    
    Args:
        xml_file: Path to the XML file
        
    Returns:
        Tuple of (patent_id, filename, character_count, success_flag)
    """
    try:
        # Extract patent ID
        patent_id = extract_patent_id(xml_file)
        
        # Count characters
        with open(xml_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            char_count = len(content)
        
        return (patent_id, str(xml_file), char_count, True)
    
    except Exception as e:
        typer.echo(f"Error processing {xml_file}: {e}", err=True)
        return (None, str(xml_file), 0, False)


def find_xml_files(xml_dir: Path) -> List[Path]:
    """
    Recursively find all XML files in the given directory.
    
    Args:
        xml_dir: Path to the XML directory
        
    Returns:
        List of Path objects for all XML files found
    """
    if not xml_dir.exists():
        typer.echo(f"Error: XML directory not found: {xml_dir}", err=True)
        raise typer.Exit(1)
    
    # Recursively find all .xml files
    xml_files = []
    for xml_file in xml_dir.rglob("*.xml"):
        if xml_file.is_file():
            xml_files.append(xml_file)
    
    return sorted(xml_files)


def save_results_to_csv(results: List[Tuple], output_file: Path) -> None:
    """
    Save results to CSV file.
    
    Args:
        results: List of tuples (patent_id, filename, char_count, success)
        output_file: Path to output CSV file
    """
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['patent_id', 'filename', 'character_count', 'success'])
        
        for patent_id, filename, char_count, success in results:
            writer.writerow([patent_id or 'UNKNOWN', filename, char_count, success])


def process_xml_files(
    xml_files: List[Path], 
    n_jobs: int = -1, 
    verbose: bool = False,
    output_file: Optional[Path] = None
) -> Dict[str, int]:
    """
    Process XML files in parallel to count characters and extract patent IDs.
    
    Args:
        xml_files: List of XML file paths
        n_jobs: Number of parallel jobs (-1 uses all cores)
        verbose: Enable verbose progress output
        output_file: Path to save CSV results
        
    Returns:
        Dictionary mapping patent IDs to character counts
    """
    if not xml_files:
        typer.echo("Warning: No XML files found to process", err=True)
        return {}
    
    typer.echo(f"Processing {len(xml_files)} XML files with {n_jobs if n_jobs > 0 else 'all available'} workers")
    
    start_time = time.time()
    
    # Use joblib to process files in parallel with progress bar
    results = Parallel(n_jobs=n_jobs, verbose=1 if verbose else 0)(
        delayed(count_file_characters)(xml_file) 
        for xml_file in tqdm(xml_files, desc="Processing XML files", disable=not verbose)
    )
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    # Save results to CSV if requested
    if output_file:
        save_results_to_csv(results, output_file)
        typer.echo(f"Results saved to {output_file}")
    
    # Collect results and statistics
    char_counts = {}
    successful_files = 0
    failed_files = 0
    total_characters = 0
    missing_ids = 0
    
    for patent_id, filename, char_count, success in results:
        if patent_id:
            char_counts[patent_id] = char_count
        else:
            missing_ids += 1
            
        total_characters += char_count
        
        if success:
            successful_files += 1
        else:
            failed_files += 1
    
    # Print summary statistics
    typer.echo("=" * 60)
    typer.echo("PROCESSING SUMMARY")
    typer.echo("=" * 60)
    typer.echo(f"Total files processed: {len(xml_files)}")
    typer.echo(f"Successful: {successful_files}")
    typer.echo(f"Failed: {failed_files}")
    typer.echo(f"Missing patent IDs: {missing_ids}")
    typer.echo(f"Total characters: {total_characters:,}")
    typer.echo(f"Average characters per file: {total_characters // successful_files if successful_files > 0 else 0:,}")
    typer.echo(f"Processing time: {processing_time:.2f} seconds")
    typer.echo(f"Files per second: {len(xml_files) / processing_time:.2f}")
    typer.echo(f"Characters per second: {total_characters / processing_time:,.0f}")
    typer.echo("=" * 60)
    
    return char_counts


@app.command()
def main(
    xml_dir: Annotated[str, typer.Option("--xml-dir", help="Directory containing XML files")] = "./xml",
    workers: Annotated[int, typer.Option("--workers", help="Number of parallel workers (-1 uses all cores)")] = -1,
    verbose: Annotated[bool, typer.Option("--verbose", help="Enable verbose output")] = False,
    output: Annotated[Optional[str], typer.Option("--output", help="Output CSV file path")] = "raw_count_results.csv"
):
    """
    Count characters in patent XML files using parallel processing.
    
    Extracts patent IDs from ep-patent-document tags and saves results as CSV.
    """
    try:
        # Find XML files
        xml_path = Path(xml_dir)
        xml_files = find_xml_files(xml_path)
        
        if not xml_files:
            typer.echo(f"No XML files found in {xml_path}", err=True)
            raise typer.Exit(1)
        
        # Set output file
        output_file = Path(output) if output else None
        
        # Process files
        char_counts = process_xml_files(xml_files, workers, verbose, output_file)
        
    except KeyboardInterrupt:
        typer.echo("Processing interrupted by user", err=True)
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()