#!/usr/bin/env python3

import wikipediaapi
import json
import sys
import tty
import termios
from collections import deque

class CategoryExplorer:
    def __init__(self, auto_ignore_keywords=None):
        self.wiki = wikipediaapi.Wikipedia(
            language='en',
            user_agent='CategoryMapper/1.0'
        )
        self.queue = deque()
        self.ignore_list = set()  # Manually ignored
        self.auto_ignored = set()  # Auto-ignored by keywords
        self.explored = set()
        self.all_pages = set()  # All unique pages found
        self.category_pages = {}  # Just category -> page count mapping
        self.auto_ignore_keywords = auto_ignore_keywords or []
    
    def get_subcategories(self, category_name):
        """Get direct subcategories of a category"""
        cat_page = self.wiki.page(f"Category:{category_name}")
        if not cat_page.exists():
            return [], []
        
        subcategories = []
        pages = []
        
        for title, page in cat_page.categorymembers.items():
            if page.ns == 0:  # Article
                pages.append(title)
            elif page.ns == 14:  # Category
                subcat_name = title.replace('Category:', '')
                subcategories.append(subcat_name)
        
        return subcategories, pages
    
    def save_state(self, filename):
        """Save current exploration state"""
        state = {
            'queue': list(self.queue),
            'ignore_list': list(self.ignore_list),
            'auto_ignored': list(self.auto_ignored),
            'explored': list(self.explored),
            'all_pages': list(self.all_pages),
            'category_pages': dict(self.category_pages),
            'auto_ignore_keywords': self.auto_ignore_keywords
        }
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        print(f"State saved to {filename}")
    
    def load_state(self, filename):
        """Load exploration state from file"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                state = json.load(f)
            
            self.queue = deque(state['queue'])
            self.ignore_list = set(state['ignore_list'])
            self.auto_ignored = set(state['auto_ignored'])
            self.explored = set(state['explored'])
            self.all_pages = set(state['all_pages'])
            self.category_pages = state['category_pages']
            self.auto_ignore_keywords = state.get('auto_ignore_keywords', [])
            
            print(f"State loaded from {filename}")
            print(f"Resuming with {len(self.queue)} categories in queue, {len(self.explored)} explored, {len(self.all_pages)} pages")
            return True
        except FileNotFoundError:
            print(f"No saved state found at {filename}")
            return False
        except Exception as e:
            print(f"Error loading state: {e}")
            return False
    
    def should_auto_ignore(self, category_name):
        """Check if category should be automatically ignored based on keywords"""
        category_lower = category_name.lower()
        return any(keyword.lower() in category_lower for keyword in self.auto_ignore_keywords)
    
    def get_key(self):
        """Get a single keypress"""
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            key = sys.stdin.read(1)
            if key == '\x1b':  # ESC sequence
                key += sys.stdin.read(2)
            return key
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    
    def display_menu(self, subcategories, current_pos, ignored_set):
        """Display interactive menu"""
        print("\033[H\033[J")  # Clear screen
        
        # Running totals header
        total_ignored = len(self.ignore_list) + len(self.auto_ignored)
        print("=== Wikipedia Category Explorer ===")
        print(f"Explored: {len(self.explored)} | Queue: {len(self.queue)} | Ignored: {total_ignored} (manual: {len(self.ignore_list)}, auto: {len(self.auto_ignored)}) | Pages: {len(self.all_pages)}")
        print("=" * 80)
        
        print(f"\nFound {len(subcategories)} subcategories")
        print("Use ↑/↓ to navigate, SPACE to toggle ignore, ENTER to continue, q to quit\n")
        
        for i, subcat in enumerate(subcategories):
            prefix = "→ " if i == current_pos else "  "
            status = "[IGNORE]" if subcat in ignored_set else "[EXPLORE]"
            print(f"{prefix}{status} {subcat}")
        
        ignored_count = len([cat for cat in subcategories if cat in ignored_set])
        explore_count = len(subcategories) - ignored_count
        print(f"\nWill explore: {explore_count}, Will ignore: {ignored_count}")
    
    def get_user_selection(self, subcategories):
        """Interactive menu for selecting categories"""
        if not subcategories:
            return 'selected', []
        
        current_pos = 0
        local_ignored = set()
        
        while True:
            self.display_menu(subcategories, current_pos, local_ignored)
            key = self.get_key()
            
            if key == 'q':
                return 'quit', []
            elif key == '\r' or key == '\n':  # Enter
                # Add locally ignored to global ignore list
                self.ignore_list.update(local_ignored)
                # Return categories that aren't ignored
                selected = [cat for cat in subcategories if cat not in local_ignored]
                return 'selected', selected
            elif key == ' ':  # Space
                cat = subcategories[current_pos]
                if cat in local_ignored:
                    local_ignored.remove(cat)
                else:
                    local_ignored.add(cat)
            elif key == '\x1b[A':  # Up arrow
                current_pos = max(0, current_pos - 1)
            elif key == '\x1b[B':  # Down arrow
                current_pos = min(len(subcategories) - 1, current_pos + 1)
    
    def explore_category(self, category_name):
        """Explore a single category"""
        if category_name in self.ignore_list or category_name in self.explored:
            return
        
        print(f"\n{'='*50}")
        print(f"Exploring: {category_name}")
        print('='*50)
        
        subcategories, pages = self.get_subcategories(category_name)
        
        # Mark as explored
        self.explored.add(category_name)
        
        # Auto-ignore categories based on keywords
        newly_auto_ignored = []
        remaining_subcategories = []
        for cat in subcategories:
            if self.should_auto_ignore(cat):
                newly_auto_ignored.append(cat)
                self.auto_ignored.add(cat)
            else:
                remaining_subcategories.append(cat)
        
        # Filter out already explored/ignored/queued categories
        all_ignored = self.ignore_list | self.auto_ignored
        subcategories = [cat for cat in remaining_subcategories 
                        if cat not in self.explored 
                        and cat not in all_ignored
                        and cat not in self.queue]
        
        if newly_auto_ignored:
            print(f"Auto-ignored {len(newly_auto_ignored)} categories: {', '.join(newly_auto_ignored[:3])}{'...' if len(newly_auto_ignored) > 3 else ''}")
        
        # Store simplified data
        self.category_pages[category_name] = len(pages)
        self.all_pages.update(pages)
        
        print(f"Found {len(pages)} pages")
        
        if not subcategories:
            print("No subcategories found.")
            return
        
        action, selected = self.get_user_selection(subcategories)
        
        if action == 'quit':
            return 'quit'
        elif action == 'selected':
            # Add selected categories to queue (duplicates already filtered out)
            self.queue.extend(selected)
            print(f"Added {len(selected)} categories to exploration queue")
    
    def run(self, save_file='stem_exploration.json'):
        """Main exploration loop"""
        print("=== Interactive Wikipedia Category Explorer ===")
        
        # Try to load existing state
        if not self.load_state(save_file):
            # Get starting categories
            print("\nEnter starting categories (comma-separated):")
            print("Suggested STEM categories:")
            print("  Core Sciences: Chemistry, Physics, Biology, Mathematics, Computer science")
            print("  Applied: Engineering, Technology, Medicine, Materials science")
            print("  Interdisciplinary: Nanotechnology, Biotechnology, Environmental science")
            print("  Broader: Science, Technology, Applied sciences, Natural sciences")
            print()
            start_input = input("Your categories > ").strip()
            if not start_input:
                return
            
            start_categories = [cat.strip() for cat in start_input.split(',')]
            self.queue.extend(start_categories)
            print(f"Added {len(start_categories)} seed categories to queue")
        
        while self.queue:
            current = self.queue.popleft()
            print(f"\nQueue: {len(self.queue)} categories remaining")
            print(f"Explored: {len(self.explored)} categories")
            print(f"Ignored: {len(self.ignore_list)} categories")
            
            result = self.explore_category(current)
            if result == 'quit':
                break
            
            # Auto-save every 10 categories
            if len(self.explored) % 10 == 0:
                self.save_state(save_file)
        
        # Final summary and save
        print(f"\n{'='*50}")
        print("EXPLORATION COMPLETE")
        print('='*50)
        print(f"Categories explored: {len(self.explored)}")
        print(f"Categories ignored: {len(self.ignore_list)} manual + {len(self.auto_ignored)} auto")
        print(f"Unique pages found: {len(self.all_pages)}")
        
        # Save final state
        self.save_state(save_file)
        
        # Export final dataset
        export = input("\nExport final dataset? (y/N): ").strip().lower()
        if export == 'y':
            filename = 'stem_dataset.json'
            output = {
                'pages': list(self.all_pages),
                'categories_explored': list(self.explored),
                'categories_ignored_manual': list(self.ignore_list),
                'categories_ignored_auto': list(self.auto_ignored),
                'category_page_counts': self.category_pages,
                'total_unique_pages': len(self.all_pages)
            }
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            print(f"Dataset exported to {filename}")

if __name__ == "__main__":
    # Comprehensive auto-ignore keywords for STEM content focus
    auto_ignore_keywords = [
        # People and professions
        "astronauts", "people", "births", "deaths", "alumni", "faculty", "ists", "historians", "professorships", "mathematicians", "scientists", "engineers", "physicists", "chemists", "biologists", "writers", "engineers", "philosophers", "scientists",
        
        # Geographic/temporal
        "by country", "by continent", "by region", "by city", "by year", "by century", "by period", "by type", 'by nationality', 'by academic institution', "by age", "by common name",
        "mines in", "in china", "in india", "industry in", "in america", "in europe", "by dependent territory", "by decade", 'by war', 'by location', 'controversies',
        
        # Organizations and institutions
        "trade unions", "research vessels", "organizations", "museums", "research institutes", "facilities", "companies", "manufacturers", "universities", "schools", "societies", "databases", "competitions", "conferences", "brands", "politics", "ministries", "symbols", "law", "regulations", 'members of',
        
        # Cultural and social
        "puzzles", "coats of arms", "festivals", "awards", "culture", "fiction", "works about", "magic", "worship", "literature", "occupations", "ethics", "quotations", "quotes", 'individual', 'artworks', 'books', 'magazines', 'periodicals', 'journals',
        "folklore", "celebrity", "sculptures", "dishes", "history", "mythological","historians", "society", "philosophy", "religion", "women in", 'luddites', "sociology", "conspiracy", 'historical', "warfare", "weapons", "war", 'software by', 'military',
        
        # Incidents and events
        "accidents", "incidents", "disasters", "attacks",
        
        # Wikipedia specific
        "taxa named by", "stubs", "lists of", "wikipedia", 'images', "redirects", "journals", "education", 'biota by', 'organisms by', 'censuses', 'by classification', 'images of', 'featured pictures', 'glossaries'
    ]
    
    explorer = CategoryExplorer(auto_ignore_keywords)
    explorer.run('stem_exploration.json')