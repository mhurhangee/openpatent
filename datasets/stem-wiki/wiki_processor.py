#!/usr/bin/env python3
"""
Optimized Wikipedia corpus processor with exclusion filtering and monitoring.
"""

import os
import sys
import time
import logging
from pathlib import Path
from gensim.corpora import WikiCorpus, MmCorpus

def setup_logging():
    """Setup logging to file and console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('wiki_processing.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def check_requirements(dump_path, output_path):
    """Check file existence and disk space."""
    dump_file = Path(dump_path)
    output_dir = Path(output_path).parent
    
    if not dump_file.exists():
        raise FileNotFoundError(f"Dump file not found: {dump_path}")
    
    # Check available disk space (rough estimate)
    dump_size = dump_file.stat().st_size
    free_space = os.statvfs(output_dir).f_frsize * os.statvfs(output_dir).f_bavail
    
    if free_space < dump_size * 0.3:  # Need ~30% of dump size for output
        logging.warning(f"Low disk space. Have {free_space//1e9:.1f}GB, may need {dump_size*0.3//1e9:.1f}GB")
    
    logging.info(f"Dump size: {dump_size//1e9:.1f}GB")
    logging.info(f"Available space: {free_space//1e9:.1f}GB")

def create_exclusion_filter():
    """Create filter to exclude definite non-STEM content."""
    import re
    
    skip_patterns = [
        # Administrative
        r'^List of', r'^Timeline of', r'disambiguation\)', r'^Category:',
        r'^Template:', r'^Wikipedia:', r'^User:', r'^Talk:', r'^File:', r'^Image:',
        
        # Biographical articles (very common in main namespace)
        r'\b(born|died)\b.*\d{4}', r'\(.*\d{4}.*\d{4}.*\)',  # Birth/death years
        r'\b\d{4}\s*births\b', r'\b\d{4}\s*deaths\b',  # "1985 births", "2010 deaths"
        r'is a.*(?:American|British|German|French|Chinese|Japanese|Russian)',  # Nationalities
        r'was a.*(?:politician|actor|singer|writer|artist|musician)',  # Biography indicators
        
        # Geographic articles
        r'is a (?:city|town|village|county|state|province|country|region|municipality)',
        r', (?:USA|United States|UK|England|Canada|Australia|Germany|France|China|India)',
        r'located in', r'capital of', r'population of', r'founded in \d{4}',
        
        # Organizations/Companies (common main articles)
        r'is a.*(?:company|corporation|organization|university|school|hospital)',
        r'headquartered in', r'founded in \d{4}', r'established in \d{4}',
        
        # Entertainment (very common)
        r'\b\d{4} film\b', r'\b\d{4} album\b', r'\btelevision series\b',
        r'is a.*(?:film|movie|album|song|book|novel|TV series)',
        r'American film', r'British film', r'Hollywood film',
        
        # Sports (common articles)
        r'\b\d{4} season\b', r'football club', r'basketball team',
        r'is a.*(?:footballer|basketball player|tennis player|athlete)',
        r'Olympic Games', r'World Cup', r'championship',
        
        # Historical events/periods
        r'World War', r'Civil War', r'\b\d{4} in\b', r'Battle of',
        r'is a.*(?:war|battle|conflict|revolution|treaty)',
        
        # Your category keywords adapted for titles
        r'people$', r'alumni$', r'faculty$',  # End of title patterns
        r'^.*by country$', r'^.*by region$', r'^.*by year$',  # Full title patterns
        r'^.*in China$', r'^.*in India$', r'^.*in America$', r'^.*in Europe$',
        r'companies$', r'organizations$', r'universities$', r'schools$',
        r'awards$', r'competitions$', r'museums$', r'manufacturers$',
        
        # Cultural/non-STEM
        r'religion', r'mythology', r'folklore', r'literature', r'poetry',
        r'politics', r'government', r'election', r'law', r'legal',
        r'philosophy', r'ethics', r'sociology', r'psychology',
        
        # Weapons/Military
        r'weapon', r'gun', r'rifle', r'pistol', r'ammunition', r'artillery',
        r'military', r'army', r'navy', r'air force', r'warfare'
    ]
    
    # Compile patterns
    skip_compiled = [re.compile(pattern, re.IGNORECASE) for pattern in skip_patterns]
    
    def filter_func(elem, text, *args, **kwargs):
        """Filter function - exclude definite junk, keep everything else."""
        title_elem = elem.find('.//title')
        if title_elem is None:
            return None
            
        title = title_elem.text or ""
        
        # Skip very short articles
        if len(text.strip()) < 500:
            return None
        
        # Check for exclusion patterns
        for pattern in skip_compiled:
            if pattern.search(title):
                return None
        
        # If no exclusion patterns match, keep the article
        return elem
    
    return filter_func

def process_wiki(dump_path, output_path, processes=None, min_article_tokens=50, 
                 exclusion_filter=True, custom_namespaces=None):
    """
    Process Wikipedia dump to corpus format.
    
    Args:
        dump_path: Path to Wikipedia XML dump
        output_path: Output path for processed corpus
        processes: Number of processes (None = auto-detect)
        min_article_tokens: Minimum tokens per article
        exclusion_filter: Whether to apply exclusion filtering
        custom_namespaces: Custom namespace filter (default: ['0'] for main articles)
    """
    setup_logging()
    
    try:
        check_requirements(dump_path, output_path)
        
        logging.info("Starting Wikipedia corpus processing...")
        logging.info(f"Input: {dump_path}")
        logging.info(f"Output: {output_path}")
        logging.info(f"Exclusion filtering: {'ON' if exclusion_filter else 'OFF'}")
        
        start_time = time.time()
        
        # Set up exclusion filtering
        article_filter = create_exclusion_filter() if exclusion_filter else None
        namespaces = custom_namespaces or ('0',)  # Default to main articles only
        
        # Create WikiCorpus with optimized settings
        wiki_corpus = WikiCorpus(
            dump_path,
            processes=processes,  # None = auto-detect cores
            article_min_tokens=min_article_tokens,
            filter_namespaces=namespaces,
            filter_articles=article_filter,  # Apply custom filter
            metadata=False,  # Set True if you need article titles
            lower=True,
            token_min_len=2,
            token_max_len=15
        )
        
        logging.info(f"Using {wiki_corpus.processes} processes")
        
        # Serialize to Matrix Market format (fastest)
        MmCorpus.serialize(output_path, wiki_corpus)
        
        elapsed = time.time() - start_time
        hours, remainder = divmod(elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        logging.info(f"Processing complete in {int(hours)}h {int(minutes)}m {int(seconds)}s")
        logging.info(f"Corpus saved to {output_path}")
        
        # Log corpus stats
        corpus = MmCorpus(output_path)
        logging.info(f"Corpus contains {corpus.num_docs} documents")
        logging.info(f"Vocabulary size: {corpus.num_terms}")
        
    except KeyboardInterrupt:
        logging.error("Processing interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Processing failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process Wikipedia dump to corpus')
    parser.add_argument('dump_path', help='Path to Wikipedia XML dump file')
    parser.add_argument('output_path', nargs='?', default='wiki-corpus.mm', 
                       help='Output corpus path (default: wiki-corpus.mm)')
    parser.add_argument('--processes', type=int, help='Number of processes (default: auto)')
    parser.add_argument('--min-tokens', type=int, default=50, 
                       help='Minimum tokens per article (default: 50)')
    parser.add_argument('--no-exclusion-filter', action='store_true',
                       help='Disable exclusion filtering')
    
    args = parser.parse_args()
    
    process_wiki(
        dump_path=args.dump_path,
        output_path=args.output_path,
        processes=args.processes,
        min_article_tokens=args.min_tokens,
        exclusion_filter=not args.no_exclusion_filter
    )