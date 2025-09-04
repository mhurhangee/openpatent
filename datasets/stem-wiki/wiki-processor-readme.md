# Wikipedia STEM Corpus Processor

A fast, optimized tool for extracting STEM-focused text corpora from Wikipedia dumps using exclusion filtering.

## Features

- **Exclusion-based filtering**: Removes biographical, geographic, entertainment, and other non-STEM content
- **Multiprocessing**: Automatically uses available CPU cores for fast processing
- **Progress monitoring**: Logs processing status and statistics
- **Memory efficient**: Streams processing without loading entire dump into memory
- **Optimized output**: Matrix Market format for fast loading and space efficiency

## Requirements

```bash
pip install gensim
```

## Quick Start

### Download Wikipedia Dump

Get a Wikipedia dump from https://dumps.wikimedia.org/enwiki/

For testing, start with a smaller multistream chunk:
```bash
# Example: Download a ~278MB chunk for testing
wget https://dumps.wikimedia.org/enwiki/20250901/enwiki-20250901-pages-articles-multistream1.xml-p1p41242.bz2
```

### Basic Usage

```bash
# Process with default STEM filtering
python wiki_processor.py enwiki-20250901-pages-articles-multistream1.xml-p1p41242.bz2

# Specify custom output location
python wiki_processor.py dump.xml.bz2 my-stem-corpus.mm

# Disable filtering (process everything)
python wiki_processor.py dump.xml.bz2 --no-exclusion-filter
```

### Advanced Options

```bash
# Limit CPU cores (useful on shared systems)
python wiki_processor.py dump.xml.bz2 --processes 4

# Stricter article filtering (minimum 100 tokens)
python wiki_processor.py dump.xml.bz2 --min-tokens 100

# Run in background with logging
nohup python wiki_processor.py dump.xml.bz2 > processing.log 2>&1 &
```

## What Gets Filtered Out

The exclusion filter removes articles likely to be non-STEM:

- **Biographical**: Politicians, actors, musicians, athletes
- **Geographic**: Cities, countries, regions, locations
- **Organizations**: Companies, universities, schools, museums  
- **Entertainment**: Movies, TV shows, albums, books
- **Sports**: Teams, seasons, competitions, leagues
- **Historical**: Wars, battles, political events
- **Cultural**: Religion, mythology, literature, philosophy

## What Gets Kept

- Scientific concepts, theories, and methods
- Technical processes and systems
- Mathematical and computational topics
- Medical and biological subjects
- Engineering and materials science
- Research methodologies
- Any content not matching exclusion patterns

## Processing Times

Approximate processing times for different systems:

| Dump Size | Fast Server (32 cores) | Desktop (8 cores) | Laptop (4 cores) |
|-----------|------------------------|-------------------|-------------------|
| 278 MB chunk | 2-5 minutes | 5-15 minutes | 10-30 minutes |
| 5 GB language dump | 30-60 minutes | 1-3 hours | 2-6 hours |
| 75 GB English dump | 2-4 hours | 6-12 hours | 12-24+ hours |

## Output

The processor creates several files:

- `wiki-corpus.mm` - Main corpus in Matrix Market format
- `wiki-corpus.mm.index` - Index for fast random access
- `wiki_processing.log` - Processing log with statistics

## Loading the Processed Corpus

```python
from gensim.corpora import MmCorpus

# Load the corpus
corpus = MmCorpus('wiki-corpus.mm')

# Basic info
print(f"Documents: {corpus.num_docs}")
print(f"Vocabulary: {corpus.num_terms}")

# Iterate through documents
for doc in corpus:
    print(doc[:5])  # First 5 terms
    break
```

## Tips

- **Test first**: Start with a small multistream chunk to verify your setup
- **Monitor resources**: Check available disk space (need ~30% of dump size)
- **Background processing**: Use `screen` or `nohup` for large dumps
- **Multiple chunks**: Process chunks separately if needed, then combine

## Troubleshooting

**Out of memory**: Reduce `--processes` or process smaller chunks
**Disk space**: Ensure at least 30% of dump size available
**Slow processing**: Use SSD storage and more CPU cores
**No output**: Check file paths and permissions

## Customization

Modify the `skip_patterns` in `create_exclusion_filter()` to adjust filtering criteria for your specific needs.