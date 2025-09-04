#!/usr/bin/env python3
"""
EPO Patent Publication Server Scraper

A comprehensive script to scrape patent data from the European Patent Office (EPO) 
publication server with SQLite-based tracking, flexible output formats, and robust 
error handling.

OVERVIEW:
This scraper operates in two phases:
1. DISCOVER: Finds all available document URLs for a date range and stores them in SQLite
2. SCRAPE: Downloads and processes the stored URLs (claims extraction or raw XML)

The database tracks claims and XML processing separately, allowing you to:
- Extract claims for some documents while saving raw XML for others
- Resume failed downloads independently for each mode
- Process specific date ranges without re-discovering URLs

WORKFLOW:
1. Discover documents: python epo_pub_scraper.py discover --start-date 20050101 --end-date 20051231
2. Check progress:     python epo_pub_scraper.py stats
3. Process data:       python epo_pub_scraper.py scrape claims --output-dir data/claims
4. Monitor progress:   python epo_pub_scraper.py stats

COMMANDS:

discover: Find and store document URLs in database
    --start-date YYYYMMDD    Start date for discovery (required)
    --end-date YYYYMMDD      End date for discovery (required)  
    --db-path PATH           SQLite database path (default: epo.db)
    --ends-with SUFFIX       Document type suffix (default: B1 for granted patents)
    --max-workers N          Concurrent workers (default: 12)

scrape: Process stored documents 
    MODE                     'claims' (extract to JSONL) or 'xml' (save raw XML files)
    --db-path PATH           SQLite database path (default: epo.db)
    --output-dir PATH        Output directory (required)
    --start-date YYYYMMDD    Only process documents from this date onward (optional)
    --end-date YYYYMMDD      Only process documents up to this date (optional)
    --max-workers N          Concurrent workers (default: 12)

stats: Show processing statistics
    --db-path PATH           SQLite database path (default: epo.db)

EXAMPLES:

# Discover all B1 documents (granted patents) for 2005
python epo_pub_scraper.py discover --start-date 20050101 --end-date 20051231

# Extract claims for Q1 2005 only  
python epo_pub_scraper.py scrape claims --output-dir claims_data --start-date 20050101 --end-date 20050331

# Save raw XML for all discovered documents
python epo_pub_scraper.py scrape xml --output-dir xml_data

# Check what's been processed
python epo_pub_scraper.py stats

# Resume failed downloads (automatically skips completed ones)
python epo_pub_scraper.py scrape claims --output-dir claims_data

OUTPUT FORMATS:

Claims mode (--mode claims):
- Creates JSONL files named by date: YYYYMMDD.jsonl
- Each line: {"pn": "EP1234567B1", "c": {"1": "claim text...", "2": "..."}}
- Only includes English claims
- Skips documents with no English claims

XML mode (--mode xml):
- Creates date subdirectories: xml_data/YYYYMMDD/
- Saves raw XML files: xml_data/YYYYMMDD/1.xml, xml_data/YYYYMMDD/2.xml, etc.
- Uses sequential numbering to avoid filename collisions
- Preserves complete patent document structure

DATABASE SCHEMA:

documents table tracks processing status independently:
- claims_status: pending/completed/failed
- xml_status: pending/completed/failed  
- Timestamps and error messages for debugging

This allows flexible processing - you can extract claims from some documents
while saving raw XML from others, resume failed jobs independently, and 
process specific date ranges efficiently.
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import requests
from bs4 import BeautifulSoup
from bs4.filter import SoupStrainer
from lxml import etree
from retry import retry
from tqdm import tqdm


class EPODatabase:
    """SQLite database manager for EPO scraper."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Initialize database tables."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dates (
                    date TEXT PRIMARY KEY,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending'
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    url TEXT PRIMARY KEY,
                    date TEXT,
                    doc_id TEXT,
                    doc_index INTEGER,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    claims_status TEXT DEFAULT 'pending',
                    claims_error TEXT,
                    claims_processed_at TIMESTAMP,
                    xml_status TEXT DEFAULT 'pending',
                    xml_error TEXT,
                    xml_processed_at TIMESTAMP,
                    FOREIGN KEY (date) REFERENCES dates (date)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_date ON documents (date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_claims_status ON documents (claims_status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_xml_status ON documents (xml_status)")
            
            # Migrate existing data if needed
            try:
                conn.execute("SELECT status FROM documents LIMIT 1")
                # Old schema exists, migrate it
                conn.execute("ALTER TABLE documents RENAME COLUMN status TO claims_status")
                conn.execute("ALTER TABLE documents RENAME COLUMN error_message TO claims_error")
                conn.execute("ALTER TABLE documents RENAME COLUMN processed_at TO claims_processed_at")
                conn.execute("ALTER TABLE documents ADD COLUMN xml_status TEXT DEFAULT 'pending'")
                conn.execute("ALTER TABLE documents ADD COLUMN xml_error TEXT")
                conn.execute("ALTER TABLE documents ADD COLUMN xml_processed_at TIMESTAMP")
                conn.execute("ALTER TABLE documents ADD COLUMN doc_index INTEGER")
            except sqlite3.OperationalError:
                # Check if doc_index column exists, add if missing
                try:
                    conn.execute("SELECT doc_index FROM documents LIMIT 1")
                except sqlite3.OperationalError:
                    conn.execute("ALTER TABLE documents ADD COLUMN doc_index INTEGER")
    
    def add_dates(self, dates: List[str]):
        """Add discovered dates to database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO dates (date) VALUES (?)",
                [(date,) for date in dates]
            )
    
    def add_documents(self, documents: List[Dict[str, str]]):
        """Add discovered document URLs to database with sequential indices."""
        with sqlite3.connect(self.db_path) as conn:
            # Group documents by date to assign sequential indices per date
            by_date = {}
            for doc in documents:
                date = doc['date']
                if date not in by_date:
                    by_date[date] = []
                by_date[date].append(doc)
            
            for date, date_docs in by_date.items():
                # Get the current max index for this date
                cursor = conn.execute(
                    "SELECT COALESCE(MAX(doc_index), 0) FROM documents WHERE date = ?",
                    (date,)
                )
                max_index = cursor.fetchone()[0]
                
                # Insert documents with sequential indices
                for i, doc in enumerate(date_docs, start=max_index + 1):
                    conn.execute(
                        "INSERT OR IGNORE INTO documents (url, date, doc_id, doc_index) VALUES (?, ?, ?, ?)",
                        (doc['url'], doc['date'], doc['doc_id'], i)
                    )
    
    def get_pending_documents(self, mode: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, str]]:
        """Get pending documents to process for specific mode (claims/xml)."""
        if mode == 'claims':
            status_col = 'claims_status'
        elif mode == 'xml':
            status_col = 'xml_status'
        else:
            raise ValueError(f"Invalid mode: {mode}")
        
        query = f"SELECT url, date, doc_id, doc_index FROM documents WHERE {status_col} = 'pending'"
        params = []
        
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        
        query += " ORDER BY date, doc_index"
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)
            return [{'url': row[0], 'date': row[1], 'doc_id': row[2], 'doc_index': row[3]} for row in cursor.fetchall()]
    
    def mark_document_processed(self, url: str, mode: str, status: str = 'completed', error: Optional[str] = None):
        """Mark document as processed for specific mode."""
        if mode == 'claims':
            query = "UPDATE documents SET claims_status = ?, claims_error = ?, claims_processed_at = CURRENT_TIMESTAMP WHERE url = ?"
        elif mode == 'xml':
            query = "UPDATE documents SET xml_status = ?, xml_error = ?, xml_processed_at = CURRENT_TIMESTAMP WHERE url = ?"
        else:
            raise ValueError(f"Invalid mode: {mode}")
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(query, (status, error, url))
    
    def get_stats(self) -> Dict[str, Dict[str, int]]:
        """Get processing statistics."""
        with sqlite3.connect(self.db_path) as conn:
            # Claims stats
            cursor = conn.execute("""
                SELECT claims_status, COUNT(*) FROM documents GROUP BY claims_status
                UNION ALL
                SELECT 'total', COUNT(*) FROM documents
            """)
            claims_stats = {row[0]: row[1] for row in cursor.fetchall()}
            
            # XML stats
            cursor = conn.execute("""
                SELECT xml_status, COUNT(*) FROM documents GROUP BY xml_status
                UNION ALL
                SELECT 'total', COUNT(*) FROM documents
            """)
            xml_stats = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Date range stats
            cursor = conn.execute("""
                SELECT MIN(date) as min_date, MAX(date) as max_date, COUNT(DISTINCT date) as date_count
                FROM documents
            """)
            date_stats = cursor.fetchone()
            
            return {
                'claims': claims_stats,
                'xml': xml_stats,
                'dates': {
                    'min_date': date_stats[0],
                    'max_date': date_stats[1],
                    'date_count': date_stats[2]
                }
            }


class EPOScraper:
    """EPO Publication Server Scraper."""
    
    def __init__(self, max_workers: int = 12, timeout: int = 5, retries: int = 10):
        self.max_workers = max_workers
        self.timeout = timeout
        self.retries = retries
        self.base_url = "https://data.epo.org"
        self._file_locks = {}
        self._global_lock = threading.Lock()
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('epo_scraper.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    @retry(tries=10, delay=1, backoff=1, jitter=(1, 3))
    def _get_response(self, url: str) -> Optional[requests.Response]:
        """Get HTTP response with retry logic."""
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            self.logger.warning(f"Request failed for {url}: {e}")
            return None
    
    def discover_dates(self, start_date: str, end_date: str) -> List[str]:
        """Discover available publication dates."""
        url = f"{self.base_url}/publication-server/rest/v1.2/publication-dates/"
        response = self._get_response(url)
        
        if not response:
            self.logger.error(f"Failed to fetch publication dates from {url}")
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser', parse_only=SoupStrainer('a'))
        links = [link.get('href') for link in soup if link and link.get('href')]
        
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        
        filtered_dates = []
        for href in links:
            try:
                date_str = href.split('/')[-2]
                link_date = datetime.strptime(date_str, '%Y%m%d')
                if start_dt <= link_date <= end_dt:
                    filtered_dates.append(date_str)
            except (ValueError, IndexError):
                continue
        
        self.logger.info(f"Discovered {len(filtered_dates)} dates between {start_date} and {end_date}")
        return filtered_dates
    
    def discover_documents(self, dates: List[str], ends_with: str = "B1") -> Dict[str, List[Dict[str, str]]]:
        """Discover document URLs for given dates."""
        results = {}
        
        def process_date(date: str):
            url = f"{self.base_url}/publication-server/rest/v1.2/publication-dates/{date}/patents"
            response = self._get_response(url)
            
            if not response:
                self.logger.error(f"Failed to fetch documents for date {date}")
                return date, []
            
            soup = BeautifulSoup(response.content, 'html.parser', parse_only=SoupStrainer('a'))
            documents = []
            
            for link in soup:
                href = link.get('href')
                if href and href.endswith(ends_with):
                    doc_id = href.split('/')[-2] if '/' in href else href
                    documents.append({
                        'url': f"{self.base_url}{href}/document.xml",
                        'date': date,
                        'doc_id': doc_id
                    })
            
            self.logger.info(f"Found {len(documents)} documents for date {date}")
            return date, documents
        
        with ThreadPoolExecutor(self.max_workers) as executor:
            future_to_date = {executor.submit(process_date, date): date for date in dates}
            
            for future in tqdm(as_completed(future_to_date), total=len(dates), desc="Discovering documents"):
                date, documents = future.result()
                results[date] = documents
        
        total_docs = sum(len(docs) for docs in results.values())
        self.logger.info(f"Discovered {total_docs} documents across {len(dates)} dates")
        return results
    
    def _get_file_lock(self, key: str):
        """Get thread-safe file lock."""
        with self._global_lock:
            if key not in self._file_locks:
                self._file_locks[key] = threading.Lock()
            return self._file_locks[key]
    
    def _extract_claims_json(self, xml_bytes: bytes) -> Optional[Dict]:
        """Extract claims from XML and return as JSON."""
        parser = etree.XMLParser(recover=True)
        try:
            root = etree.fromstring(xml_bytes, parser=parser)
        except Exception as e:
            self.logger.error(f"XML parsing error: {e}")
            return None
        
        country = root.get('country', '') or ''
        number = root.get('doc-number', '') or ''
        kind = root.get('kind', '') or ''
        pn = f"{country}{number}{kind}".strip()
        
        if not pn:
            return None
        
        claims_dict = {}
        for claim in root.xpath('//claims[@lang="en"]//claim'):
            num = (claim.get('num') or '').lstrip('0')
            if not num:
                continue
            
            texts = []
            for ctext in claim.xpath('.//claim-text'):
                text_content = " ".join(s.strip() for s in ctext.xpath('.//text()') if s and s.strip())
                if text_content:
                    texts.append(text_content)
            
            claim_text = "\n".join(texts)
            if claim_text:
                claims_dict[num] = claim_text.strip()
        
        return {"pn": pn, "c": claims_dict} if claims_dict else None
    
    def scrape_claims(self, db: EPODatabase, output_dir: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """Scrape claims and save as JSONL."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        pending_docs = db.get_pending_documents('claims', start_date, end_date)
        date_range_str = ""
        if start_date or end_date:
            date_range_str = f" (dates: {start_date or 'start'} to {end_date or 'end'})"
        self.logger.info(f"Processing {len(pending_docs)} pending documents for claims extraction{date_range_str}")
        
        def process_document(doc: Dict[str, str]) -> int:
            response = self._get_response(doc['url'])
            if not response:
                db.mark_document_processed(doc['url'], 'claims', 'failed', 'HTTP request failed')
                return 0
            
            claims_data = self._extract_claims_json(response.content)
            if not claims_data:
                db.mark_document_processed(doc['url'], 'claims', 'completed', 'No claims found')
                return 0
            
            # Write to date-specific JSONL file
            output_file = output_path / f"{doc['date']}.jsonl"
            lock = self._get_file_lock(doc['date'])
            
            try:
                with lock, open(output_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(claims_data, ensure_ascii=False) + '\n')
                
                db.mark_document_processed(doc['url'], 'claims', 'completed')
                return 1
            except Exception as e:
                db.mark_document_processed(doc['url'], 'claims', 'failed', str(e))
                self.logger.error(f"Error writing claims for {doc['url']}: {e}")
                return 0
        
        processed_count = 0
        with ThreadPoolExecutor(self.max_workers) as executor:
            futures = [executor.submit(process_document, doc) for doc in pending_docs]
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing claims"):
                try:
                    processed_count += future.result()
                except Exception as e:
                    self.logger.error(f"Document processing error: {e}")
        
        self.logger.info(f"Successfully processed {processed_count} documents for claims")
    
    def scrape_xml(self, db: EPODatabase, output_dir: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """Scrape raw XML documents."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        pending_docs = db.get_pending_documents('xml', start_date, end_date)
        date_range_str = ""
        if start_date or end_date:
            date_range_str = f" (dates: {start_date or 'start'} to {end_date or 'end'})"
        self.logger.info(f"Processing {len(pending_docs)} pending documents for XML extraction{date_range_str}")
        
        def process_document(doc: Dict[str, str]) -> int:
            response = self._get_response(doc['url'])
            if not response:
                db.mark_document_processed(doc['url'], 'xml', 'failed', 'HTTP request failed')
                return 0
            
            # Create date subdirectory
            date_dir = output_path / doc['date']
            date_dir.mkdir(exist_ok=True)
            
            # Save XML file using index to avoid collisions
            xml_file = date_dir / f"{doc['doc_index']}.xml"
            
            try:
                with open(xml_file, 'wb') as f:
                    f.write(response.content)
                
                db.mark_document_processed(doc['url'], 'xml', 'completed')
                return 1
            except Exception as e:
                db.mark_document_processed(doc['url'], 'xml', 'failed', str(e))
                self.logger.error(f"Error saving XML for {doc['url']}: {e}")
                return 0
        
        processed_count = 0
        with ThreadPoolExecutor(self.max_workers) as executor:
            futures = [executor.submit(process_document, doc) for doc in pending_docs]
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing XML"):
                try:
                    processed_count += future.result()
                except Exception as e:
                    self.logger.error(f"Document processing error: {e}")
        
        self.logger.info(f"Successfully processed {processed_count} documents for XML")


def main():
    parser = argparse.ArgumentParser(description="EPO Patent Publication Server Scraper")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Discover command
    discover_parser = subparsers.add_parser('discover', help='Discover publication dates and document URLs')
    discover_parser.add_argument('--start-date', required=True, help='Start date (YYYYMMDD)')
    discover_parser.add_argument('--end-date', required=True, help='End date (YYYYMMDD)')
    discover_parser.add_argument('--db-path', default='epo.db', help='SQLite database path')
    discover_parser.add_argument('--ends-with', default='B1', help='Document type suffix (default: B1)')
    discover_parser.add_argument('--max-workers', type=int, default=12, help='Max concurrent workers')
    
    # Scrape command
    scrape_parser = subparsers.add_parser('scrape', help='Scrape documents from database')
    scrape_parser.add_argument('mode', choices=['claims', 'xml'], help='Scraping mode')
    scrape_parser.add_argument('--db-path', default='epo.db', help='SQLite database path')
    scrape_parser.add_argument('--output-dir', required=True, help='Output directory')
    scrape_parser.add_argument('--start-date', help='Start date for scraping (YYYYMMDD)')
    scrape_parser.add_argument('--end-date', help='End date for scraping (YYYYMMDD)')
    scrape_parser.add_argument('--max-workers', type=int, default=12, help='Max concurrent workers')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show database statistics')
    stats_parser.add_argument('--db-path', default='epo.db', help='SQLite database path')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == 'discover':
        scraper = EPOScraper(max_workers=args.max_workers)
        db = EPODatabase(args.db_path)
        
        # Discover dates
        dates = scraper.discover_dates(args.start_date, args.end_date)
        if not dates:
            print("No dates found in the specified range")
            sys.exit(1)
        
        db.add_dates(dates)
        
        # Discover documents
        documents_by_date = scraper.discover_documents(dates, args.ends_with)
        
        # Add to database
        all_documents = []
        for date_docs in documents_by_date.values():
            all_documents.extend(date_docs)
        
        if all_documents:
            db.add_documents(all_documents)
            print(f"Added {len(all_documents)} documents to database")
        else:
            print("No documents found")
    
    elif args.command == 'scrape':
        scraper = EPOScraper(max_workers=args.max_workers)
        db = EPODatabase(args.db_path)
        
        if args.mode == 'claims':
            scraper.scrape_claims(db, args.output_dir, args.start_date, args.end_date)
        elif args.mode == 'xml':
            scraper.scrape_xml(db, args.output_dir, args.start_date, args.end_date)
    
    elif args.command == 'stats':
        db = EPODatabase(args.db_path)
        stats = db.get_stats()
        print("Database Statistics:")
        print(f"\nDate Range: {stats['dates']['min_date']} to {stats['dates']['max_date']} ({stats['dates']['date_count']} dates)")
        print(f"\nClaims Processing:")
        for status, count in stats['claims'].items():
            print(f"  {status}: {count}")
        print(f"\nXML Processing:")
        for status, count in stats['xml'].items():
            print(f"  {status}: {count}")


if __name__ == '__main__':
    main()