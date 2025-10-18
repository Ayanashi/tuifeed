import requests
import re
import time
import feedparser
import os
import json
import sqlite3
import hashlib
import shutil
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin
from collections import defaultdict, Counter
from bs4 import BeautifulSoup
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import pickle

# Enhanced multi-language RSS sources
LANGUAGE_SOURCES = {
    'english': {
        'technology': [
            'https://techcrunch.com/feed/',
            'https://feeds.arstechnica.com/arstechnica/index',
            'https://www.wired.com/feed/rss',
            'https://feeds.feedburner.com/TechRepublic',
            'https://www.theverge.com/rss/index.xml',
        ],
        'security': [
            'https://krebsonsecurity.com/feed/',
            'https://www.darkreading.com/rss.xml',
            'https://www.securityweek.com/rss',
            'https://www.helpnetsecurity.com/feed/',
            'https://www.bleepingcomputer.com/feed/',
        ],
        'programming': [
            'https://stackoverflow.blog/feed/',
            'https://blog.github.com/feed.xml',
            'https://blog.codinghorror.com/rss/',
        ],
        'ai_ml': [
            'https://openai.com/blog/rss/',
            'https://blog.google/technology/ai/rss/',
            'https://aws.amazon.com/blogs/machine-learning/feed/',
        ],
        'devops': [
            'https://cloud.google.com/blog/feeds/cloud',
            'https://aws.amazon.com/blogs/aws/feed/',
            'https://azure.microsoft.com/en-us/blog/feed/',
        ],
        'sports': [
            'https://www.espn.com/espn/rss/news',
            'https://www.skysports.com/rss/12040',
            'https://feeds.reuters.com/reuters/sportsNews',
        ],
        'news': [
            'https://feeds.reuters.com/reuters/topNews',
            'https://rss.cnn.com/rss/edition.rss',
            'https://feeds.bbci.co.uk/news/rss.xml',
        ],
        'business': [
            'https://feeds.reuters.com/reuters/businessNews',
            'https://www.bloomberg.com/feeds/podcasts/etf_report.xml',
        ],
        'science': [
            'https://www.science.org/rss/news_current.xml',
            'https://feeds.arstechnica.com/arstechnica/science',
        ]
    },
    'italian': {
        'technology': [
            'https://www.repubblica.it/rss/tecnologia/rss2.0.xml',
            'https://xml2.corriereobjects.it/rss/tecnologia.xml',
            'https://www.ilsole24ore.com/rss/tecnologia.xml',
        ],
        'security': [
            'https://www.cybersecurity360.it/feed/',
            'https://www.securityinfo.it/feed/',
            'https://www.cert-agid.gov.it/feed/',
        ],
        'programming': [
            'https://www.html.it/feed/',
        ],
        'sports': [
            'https://www.gazzetta.it/rss/primopiano.xml',
            'https://rss.corrieredellosport.it/rss/primopiano.xml',
            'https://www.tuttosport.com/rss/tuttosport.xml',
        ],
        'news': [
            'https://www.ansa.it/sito/ansait_rss.xml',
            'https://xml2.corriereobjects.it/rss/primopiano.xml',
            'https://www.repubblica.it/rss/homepage/rss2.0.xml',
        ],
        'business': [
            'https://www.ilsole24ore.com/rss/italia-mondo.xml',
        ]
    },
    'spanish': {
        'technology': [
            'https://www.xataka.com/index.rss',
            'https://www.genbeta.com/index.rss',
        ],
        'sports': [
            'https://e00-marca.uecdn.es/rss/portada.xml',
            'https://as.com/rss/tags/ultimas_noticias.xml',
        ],
        'news': [
            'https://e00-elmundo.uecdn.es/elmundo/rss/portada.xml',
            'https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/portada',
        ]
    },
    'french': {
        'technology': [
            'https://www.lesnumeriques.com/feed/',
            'https://www.clubic.com/rss/actualites.rss',
        ],
        'sports': [
            'https://www.lequipe.fr/rss/actu_rss.xml',
        ],
        'news': [
            'https://www.lemonde.fr/rss/une.xml',
            'https://www.lefigaro.fr/rss/figaro_actualites.xml',
        ]
    },
    'german': {
        'technology': [
            'https://www.heise.de/newsticker/heise.rdf',
            'https://rss.golem.de/rss.php?feed=RSS2.0',
        ],
        'news': [
            'https://www.spiegel.de/schlagzeilen/index.rss',
            'https://rss.faz.net/faz/aktuell',
        ]
    }
}

# GitHub repositories that contain RSS feed lists
GITHUB_FEED_REPOSITORIES = {
    'awesome_rss_feeds': {
        'owner': 'plenaryapp',
        'repo': 'awesome-rss-feeds',
        'description': 'Collection of RSS feeds'
    },
    'rss_feeds_curated': {
        'owner': 'rss-source', 
        'repo': 'rss-feeds',
        'description': 'Curated RSS feeds collection'
    },
    'security_feeds': {
        'owner': '0x4D31',
        'repo': 'fatt',
        'description': 'Security and threat intelligence feeds'
    },
    'italian_feeds': {
        'owner': 'matteobaccan',
        'repo': 'rss',
        'description': 'Italian RSS feeds collection'
    },
    'programming_feeds': {
        'owner': 'simevidas',
        'repo': 'web-feeds',
        'description': 'Web development and programming feeds'
    }
}

class RSSFeedManager:
    def __init__(self):
        self.db_path = "rss_feeds.db"
        self.init_database()
        
    def init_database(self):
        """Initialize SQLite database for feed management"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Feeds table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS feeds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                url TEXT UNIQUE,
                category TEXT,
                language TEXT,
                last_checked TIMESTAMP,
                last_status TEXT,
                article_count INTEGER,
                success_rate REAL,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Articles cache table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS articles_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                feed_url TEXT,
                article_hash TEXT,
                title TEXT,
                published TIMESTAMP,
                cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Feed statistics table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS feed_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                feed_url TEXT,
                check_date DATE,
                status TEXT,
                articles_found INTEGER
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def save_feed_result(self, name, url, category, language, status, article_count, success_rate=1.0):
        """Save feed test result to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO feeds 
            (name, url, category, language, last_checked, last_status, article_count, success_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (name, url, category, language, datetime.now(), status, article_count, success_rate))
        
        conn.commit()
        conn.close()
    
    def get_feed_stats(self):
        """Get overall feed statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                COUNT(*) as total_feeds,
                SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active_feeds,
                AVG(success_rate) as avg_success_rate,
                AVG(article_count) as avg_articles
            FROM feeds
        ''')
        stats = cursor.fetchone()
        
        conn.close()
        return stats

# Initialize feed manager
feed_manager = RSSFeedManager()

def test_feed(url):
    """Test if an RSS feed is accessible and has content"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; RSS-Reader/1.0)'
        }
        response = requests.get(url, timeout=10, headers=headers)
        if response.status_code == 200:
            content = response.text.lower()
            if any(tag in content for tag in ['<rss', '<feed', '<?xml', 'rss']):
                feed = feedparser.parse(url)
                if len(feed.entries) > 0:
                    return True, "ACTIVE", len(feed.entries), feed.feed.get('title', 'Unknown')
                else:
                    return True, "ACTIVE_EMPTY", 0, feed.feed.get('title', 'Unknown')
            else:
                return False, "NOT_RSS", 0, None
        else:
            return False, f"HTTP_{response.status_code}", 0, None
    except Exception as e:
        return False, f"ERROR: {str(e)}", 0, None

def advanced_feed_health_check(url, extensive_test=False):
    """Perform advanced health checking on RSS feeds"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; RSS-Health-Checker/1.0)'
        }
        
        start_time = time.time()
        response = requests.get(url, timeout=15, headers=headers)
        response_time = time.time() - start_time
        
        if response.status_code == 200:
            feed = feedparser.parse(url)
            
            health_data = {
                'response_time': response_time,
                'status_code': response.status_code,
                'feed_type': 'unknown',
                'entries_count': len(feed.entries),
                'bozo': feed.bozo,
                'encoding': feed.encoding,
                'version': getattr(feed, 'version', 'unknown'),
                'last_updated': None,
                'update_frequency': 'unknown'
            }
            
            # Determine feed type
            if hasattr(feed, 'version'):
                health_data['feed_type'] = feed.version
            
            # Calculate update frequency (if we have historical data)
            if len(feed.entries) > 1:
                dates = []
                for entry in feed.entries[:10]:
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        dates.append(datetime(*entry.published_parsed[:6]))
                
                if len(dates) > 1:
                    dates.sort()
                    time_diffs = [(dates[i] - dates[i+1]).days for i in range(len(dates)-1)]
                    avg_frequency = sum(time_diffs) / len(time_diffs) if time_diffs else 0
                    
                    if avg_frequency < 1:
                        health_data['update_frequency'] = 'multiple_daily'
                    elif avg_frequency < 2:
                        health_data['update_frequency'] = 'daily'
                    elif avg_frequency < 7:
                        health_data['update_frequency'] = 'weekly'
                    else:
                        health_data['update_frequency'] = 'monthly+'
            
            # Extensive testing for important feeds
            if extensive_test and len(feed.entries) > 0:
                # Check entry completeness
                complete_entries = 0
                for entry in feed.entries[:5]:
                    if (hasattr(entry, 'title') and entry.title and 
                        hasattr(entry, 'link') and entry.link):
                        complete_entries += 1
                
                health_data['content_quality'] = complete_entries / 5
            
            return True, "HEALTHY", health_data
        
        return False, f"HTTP_{response.status_code}", {'response_time': response_time}
    
    except Exception as e:
        return False, f"ERROR: {str(e)}", {}

def discover_rss_from_website(url):
    """Discover RSS feeds from a website URL"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; RSS-Discovery-Bot/1.0)'
        }
        response = requests.get(url, timeout=10, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        feeds = []
        # Look for RSS links
        for link in soup.find_all('link', type=['application/rss+xml', 'application/atom+xml']):
            href = link.get('href')
            if href:
                full_url = urljoin(url, href)
                title = link.get('title') or f"Feed from {urlparse(url).netloc}"
                feeds.append((title, full_url))
        
        # Also check for common RSS patterns in <a> tags
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            text = a_tag.get_text().lower()
            if any(keyword in text or keyword in href.lower() for keyword in ['rss', 'feed', 'atom', 'subscribe']):
                full_url = urljoin(url, href)
                if any(ext in full_url.lower() for ext in ['.rss', '.xml', '.atom']):
                    title = a_tag.get_text().strip() or f"Feed from {urlparse(url).netloc}"
                    feeds.append((title, full_url))
        
        return feeds
    except Exception as e:
        print(f"Error discovering feeds from {url}: {e}")
        return []

def get_github_file_content(owner, repo, filepath):
    """Get content of a file from GitHub repository"""
    try:
        url = f"https://raw.githubusercontent.com/{owner}/{repo}/main/{filepath}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.text
        # Try with master branch if main fails
        url = f"https://raw.githubusercontent.com/{owner}/{repo}/master/{filepath}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.text
        return None
    except Exception as e:
        print(f"Error getting file from GitHub: {e}")
        return None

def search_github_rss_repositories(query):
    """Search GitHub for repositories containing RSS feeds"""
    try:
        url = f"https://api.github.com/search/repositories?q={query}+rss+feeds&sort=stars&order=desc"
        headers = {'Accept': 'application/vnd.github.v3+json'}
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            repos = []
            for item in data.get('items', [])[:8]:  # Top 8 results
                repo_info = {
                    'name': item['full_name'],
                    'description': item.get('description', 'No description'),
                    'stars': item['stargazers_count'],
                    'url': item['html_url'],
                    'owner': item['owner']['login'],
                    'repo': item['name']
                }
                repos.append(repo_info)
            return repos
        return []
    except Exception as e:
        print(f"Error searching GitHub: {e}")
        return []

def extract_feeds_from_github_repo(owner, repo, existing_urls):
    """Extract RSS feeds from a GitHub repository containing feed lists"""
    print(f"Exploring repository: {owner}/{repo}")
    
    # Common files that might contain RSS feeds
    common_files = [
        'README.md',
        'feeds.md',
        'rss.md',
        'feeds.txt',
        'rss.txt',
        'feedlist.md',
        'OPML.xml',
        'feeds.opml',
        'feedlist.txt'
    ]
    
    all_feeds = {}
    
    for filename in common_files:
        content = get_github_file_content(owner, repo, filename)
        if content:
            print(f"  Found {filename}, extracting feeds...")
            feeds_from_file = extract_feeds_from_content(content, existing_urls)
            all_feeds.update(feeds_from_file)
    
    # Also try to get the repository description to understand its purpose
    try:
        url = f"https://api.github.com/repos/{owner}/{repo}"
        headers = {'Accept': 'application/vnd.github.v3+json'}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            repo_data = response.json()
            description = repo_data.get('description', '')
            print(f"  Repository description: {description}")
    except:
        pass
    
    return all_feeds

def extract_feeds_from_content(content, existing_urls):
    """Extract RSS feed URLs from text content"""
    feeds = {}
    
    # Pattern to find URLs that look like RSS feeds
    rss_patterns = [
        r'https?://[^\s<>"\'{}|\\^`\[\]]+\.(?:rss|xml|atom)[^\s<>"\'{}|\\^`]*',
        r'https?://[^\s<>"\'{}|\\^`\[\]]+/feed/[^\s<>"\'{}|\\^`]*',
        r'https?://[^\s<>"\'{}|\\^`\[\]]+/rss[^\s<>"\'{}|\\^`]*',
        r'https?://[^\s<>"\'{}|\\^`\[\]]+/atom[^\s<>"\'{}|\\^`]*'
    ]
    
    for pattern in rss_patterns:
        matches = re.findall(pattern, content, re.IGNORECASE)
        for url in matches:
            # Clean the URL
            url = url.rstrip('.,);:!?')
            if url not in existing_urls:
                print(f"    Testing: {urlparse(url).netloc}...", end=" ")
                is_active, status, entry_count, feed_title = test_feed(url)
                
                if is_active:
                    feed_name = get_feed_name(url, "github", "discovered", feed_title)
                    feeds[feed_name] = url
                    feed_manager.save_feed_result(feed_name, url, "github", "discovered", status, entry_count)
                    print(f"✓ ACTIVE ({entry_count} articles)")
                else:
                    print(f"✗ {status}")
                
                time.sleep(0.3)
    
    return feeds

def smart_feed_categorization(feed_url, feed_title, feed_content):
    """Automatically categorize feeds based on content analysis"""
    categories_weights = defaultdict(float)
    
    # Keyword-based categorization
    category_keywords = {
        'technology': ['tech', 'software', 'hardware', 'computer', 'digital', 'ai', 'machine learning'],
        'security': ['security', 'cyber', 'hack', 'malware', 'virus', 'firewall', 'privacy'],
        'programming': ['code', 'programming', 'developer', 'python', 'javascript', 'java'],
        'news': ['news', 'update', 'breaking', 'reports', 'journal'],
        'sports': ['sports', 'game', 'match', 'team', 'player', 'score'],
        'science': ['science', 'research', 'study', 'scientists', 'discovery'],
        'business': ['business', 'market', 'economy', 'finance', 'stock', 'investment']
    }
    
    text_to_analyze = f"{feed_title} {feed_content}".lower()
    
    for category, keywords in category_keywords.items():
        for keyword in keywords:
            if keyword in text_to_analyze:
                categories_weights[category] += 1
    
    # Return top category or 'general' if no strong match
    if categories_weights:
        return max(categories_weights.items(), key=lambda x: x[1])[0]
    return 'general'

def discover_feeds_from_sitemap(sitemap_url):
    """Extract potential RSS feeds from sitemap.xml"""
    try:
        response = requests.get(sitemap_url, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'xml')
            feeds = []
            
            # Look for sitemap entries that might indicate RSS feeds
            for url in soup.find_all('url'):
                loc = url.find('loc')
                if loc:
                    page_url = loc.text
                    # Common RSS feed patterns
                    rss_patterns = [
                        '/feed', '/rss', '/atom', '.rss', '.xml',
                        '/feed.xml', '/rss.xml', '/atom.xml'
                    ]
                    
                    if any(pattern in page_url.lower() for pattern in rss_patterns):
                        feeds.append(("Discovered from sitemap", page_url))
                    else:
                        # Also check the page for RSS links
                        try:
                            page_feeds = discover_rss_from_website(page_url)
                            feeds.extend(page_feeds)
                        except:
                            pass
            
            return feeds
    except Exception as e:
        print(f"Error processing sitemap: {e}")
    
    return []

def find_sitemap(domain_url):
    """Try to find sitemap for a domain"""
    sitemap_locations = [
        '/sitemap.xml',
        '/sitemap_index.xml',
        '/sitemap.php',
        '/sitemap.txt',
        '/sitemap/'
    ]
    
    for location in sitemap_locations:
        try:
            sitemap_url = urljoin(domain_url, location)
            response = requests.head(sitemap_url, timeout=5)
            if response.status_code == 200:
                return sitemap_url
        except:
            continue
    
    return None

def bulk_feed_testing(feed_urls, max_workers=5):
    """Test multiple feeds concurrently for better performance"""
    def test_single_feed(url):
        return url, test_feed(url)
    
    working_feeds = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(test_single_feed, url): url for url in feed_urls}
        
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                url, result = future.result()
                is_active, status, entry_count, feed_title = result
                
                if is_active:
                    feed_name = get_feed_name(url, "bulk_test", "discovered", feed_title)
                    working_feeds[feed_name] = url
            
            except Exception as e:
                print(f"Error testing {url}: {e}")
    
    return working_feeds

def get_feed_name(url, category, language, feed_title=None):
    """Generate a readable feed name from URL and metadata"""
    domain = urlparse(url).netloc.replace('www.', '').split('.')[0]
    if feed_title and feed_title != 'Unknown':
        clean_title = re.sub(r'[^\w\s]', '', feed_title)
        clean_title = re.sub(r'\s+', '_', clean_title.strip())
        return f"{clean_title}_{language}"
    else:
        return f"{domain}_{category}_{language}"

def load_existing_config():
    """Load existing feeds from config.toml to avoid duplicates"""
    existing_feeds = {}
    if os.path.exists('config.toml'):
        try:
            with open('config.toml', 'r', encoding='utf-8') as f:
                content = f.read()
                # Extract feed names and URLs
                matches = re.findall(r'(\w+)\s*=\s*"([^"]+)"', content)
                for name, url in matches:
                    existing_feeds[name] = url
        except Exception as e:
            print(f"Warning: Could not read existing config: {e}")
    return existing_feeds

def discover_and_add_feeds(language, category, existing_urls):
    """Discover and test feeds for a specific language and category"""
    if language not in LANGUAGE_SOURCES or category not in LANGUAGE_SOURCES[language]:
        print(f"No feeds found for {language} - {category}")
        return {}
    
    feeds = LANGUAGE_SOURCES[language][category]
    working_feeds = {}
    
    print(f"\nTesting {len(feeds)} feeds for {language} - {category}:")
    print("-" * 50)
    
    for i, url in enumerate(feeds, 1):
        if url in existing_urls.values():
            print(f"[{i}/{len(feeds)}] SKIPPED (already in config): {urlparse(url).netloc}")
            continue
            
        print(f"[{i}/{len(feeds)}] Testing: {urlparse(url).netloc}...", end=" ")
        
        is_active, status, entry_count, feed_title = test_feed(url)
        
        if is_active:
            feed_name = get_feed_name(url, category, language, feed_title)
            working_feeds[feed_name] = url
            feed_manager.save_feed_result(feed_name, url, category, language, status, entry_count)
            print(f"✓ ACTIVE ({entry_count} articles)")
        else:
            print(f"✗ {status}")
        
        time.sleep(0.5)
    
    return working_feeds

def discover_from_website_url(url, existing_urls):
    """Discover and test feeds from a custom website URL"""
    print(f"Discovering RSS feeds from: {url}")
    feeds = discover_rss_from_website(url)
    
    if not feeds:
        print("No RSS feeds found on this website.")
        return {}
    
    working_feeds = {}
    print(f"Found {len(feeds)} potential feeds. Testing...")
    
    for title, feed_url in feeds:
        if feed_url in existing_urls.values():
            print(f"SKIPPED (already in config): {title}")
            continue
            
        print(f"Testing: {title}...", end=" ")
        is_active, status, entry_count, feed_title = test_feed(feed_url)
        
        if is_active:
            feed_name = re.sub(r'[^\w]', '_', title)
            working_feeds[feed_name] = feed_url
            feed_manager.save_feed_result(feed_name, feed_url, "website", "discovered", status, entry_count)
            print(f"✓ ACTIVE ({entry_count} articles)")
        else:
            print(f"✗ {status}")
        
        time.sleep(0.5)
    
    return working_feeds

def github_rss_collections_menu():
    """Menu for discovering RSS feeds from GitHub collections"""
    print("\n" + "="*50)
    print("GITHUB RSS COLLECTIONS DISCOVERY")
    print("="*50)
    
    existing_urls = load_existing_config()
    all_working_feeds = {}
    
    while True:
        print("\nGitHub RSS Collections Options:")
        print("1. Browse known RSS feed repositories")
        print("2. Search GitHub for RSS feed collections")
        print("3. Enter custom GitHub repository (owner/repo)")
        print("0. Back to Main Menu")
        
        choice = input("\nSelect option: ").strip()
        
        if choice == '1':
            print("\nKnown RSS Feed Repositories:")
            repos = list(GITHUB_FEED_REPOSITORIES.items())
            for i, (repo_id, repo_info) in enumerate(repos, 1):
                print(f"{i}. {repo_info['owner']}/{repo_info['repo']}")
                print(f"   {repo_info['description']}")
            
            repo_choice = input("\nSelect repository number (or 'all' for all): ").strip()
            
            if repo_choice.lower() == 'all':
                for repo_id, repo_info in repos:
                    feeds = extract_feeds_from_github_repo(
                        repo_info['owner'], 
                        repo_info['repo'], 
                        existing_urls
                    )
                    all_working_feeds.update(feeds)
                    print(f"Found {len(feeds)} feeds from {repo_info['owner']}/{repo_info['repo']}")
            else:
                try:
                    idx = int(repo_choice) - 1
                    if 0 <= idx < len(repos):
                        repo_id, repo_info = repos[idx]
                        feeds = extract_feeds_from_github_repo(
                            repo_info['owner'], 
                            repo_info['repo'], 
                            existing_urls
                        )
                        all_working_feeds.update(feeds)
                        print(f"Found {len(feeds)} feeds from {repo_info['owner']}/{repo_info['repo']}")
                except ValueError:
                    print("Invalid selection")
        
        elif choice == '2':
            query = input("Enter search query (e.g., 'security rss', 'italian feeds'): ").strip()
            if query:
                print(f"Searching GitHub for: {query}")
                repos = search_github_rss_repositories(query)
                
                if repos:
                    print(f"\nFound {len(repos)} repositories:")
                    for i, repo in enumerate(repos, 1):
                        print(f"{i}. {repo['name']} ({repo['stars']} stars)")
                        print(f"   {repo['description']}")
                    
                    repo_choice = input("\nSelect repository number to explore: ").strip()
                    try:
                        idx = int(repo_choice) - 1
                        if 0 <= idx < len(repos):
                            repo = repos[idx]
                            feeds = extract_feeds_from_github_repo(
                                repo['owner'],
                                repo['repo'],
                                existing_urls
                            )
                            all_working_feeds.update(feeds)
                            print(f"Found {len(feeds)} feeds from {repo['name']}")
                    except ValueError:
                        print("Invalid selection")
                else:
                    print("No repositories found.")
        
        elif choice == '3':
            repo_input = input("Enter GitHub repository (format: owner/repo): ").strip()
            if '/' in repo_input:
                owner, repo_name = repo_input.split('/')
                feeds = extract_feeds_from_github_repo(owner, repo_name, existing_urls)
                all_working_feeds.update(feeds)
                print(f"Found {len(feeds)} feeds from {owner}/{repo_name}")
        
        elif choice == '0':
            break
        
        else:
            print("Invalid option")
    
    return all_working_feeds

def display_language_menu():
    """Display language selection menu"""
    print("\n" + "="*50)
    print("SELECT LANGUAGE")
    print("="*50)
    
    languages = {
        '1': ('English', 'english'),
        '2': ('Italian', 'italian'),
        '3': ('Spanish', 'spanish'),
        '4': ('French', 'french'),
        '5': ('German', 'german'),
        '6': ('All Languages', 'all')
    }
    
    for key, (name, code) in languages.items():
        if code == 'all':
            feed_count = sum(len(feeds) for lang in LANGUAGE_SOURCES.values() for feeds in lang.values())
        else:
            feed_count = sum(len(categories) for categories in LANGUAGE_SOURCES.get(code, {}).values())
        print(f"  {key}. {name} ({feed_count} feeds)")
    
    print("  0. Main Menu")
    
    return languages

def display_category_menu(language_name, language_code):
    """Display category selection menu for a specific language"""
    print(f"\nSELECT CATEGORY FOR {language_name.upper()}")
    print("-" * 40)
    
    if language_code == 'all':
        all_categories = set()
        for lang_categories in LANGUAGE_SOURCES.values():
            all_categories.update(lang_categories.keys())
        categories = sorted(all_categories)
    else:
        categories = list(LANGUAGE_SOURCES.get(language_code, {}).keys())
    
    category_menu = {}
    for i, category in enumerate(categories, 1):
        if language_code == 'all':
            feed_count = sum(len(LANGUAGE_SOURCES[lang].get(category, [])) for lang in LANGUAGE_SOURCES)
        else:
            feed_count = len(LANGUAGE_SOURCES[language_code].get(category, []))
        category_menu[str(i)] = (category.title().replace('_', ' '), category, feed_count)
    
    for key, (display_name, _, feed_count) in category_menu.items():
        print(f"  {key}. {display_name} ({feed_count} feeds)")
    
    print("  9. All Categories")
    print("  0. Back to Language Selection")
    
    return category_menu

def save_to_config(working_feeds, mode="append"):
    """Save working feeds to config.toml"""
    if not working_feeds:
        print("No working feeds to save.")
        return False
    
    organized_feeds = defaultdict(lambda: defaultdict(dict))
    
    for feed_name, url in working_feeds.items():
        if 'github' in feed_name.lower() or 'discovered' in feed_name.lower():
            organized_feeds['GitHub_Collections']['Discovered_Feeds'][feed_name] = url
        else:
            parts = feed_name.split('_')
            if len(parts) >= 2:
                language = parts[-1]
                category_source = '_'.join(parts[:-1])
                for cat in ['technology', 'security', 'programming', 'ai_ml', 'devops', 'sports', 'news', 'business', 'science']:
                    if cat in category_source.lower():
                        category = cat
                        break
                else:
                    category = 'general'
            else:
                language = 'unknown'
                category = 'general'
            
            organized_feeds[language.title()][category.title()][feed_name] = url
    
    if mode == "overwrite":
        config_content = """[article-title]
show-author = true
show-timestamp = true

[sources]
# Automatically discovered and tested feeds

"""
    else:
        if os.path.exists('config.toml'):
            with open('config.toml', 'r', encoding='utf-8') as f:
                config_content = f.read()
            if '[sources]' in config_content:
                config_content += "\n"
        else:
            config_content = """[article-title]
show-author = true
show-timestamp = true

[sources]
# Automatically discovered and tested feeds

"""
    
    for section in sorted(organized_feeds.keys()):
        config_content += f"\n# {section.upper()}\n"
        for category in sorted(organized_feeds[section].keys()):
            config_content += f"# {category.replace('_', ' ').title()}\n"
            for name, url in sorted(organized_feeds[section][category].items()):
                config_content += f'{name} = "{url}"\n'
    
    with open('config.toml', 'w', encoding='utf-8') as f:
        f.write(config_content)
    
    return True

def bulk_discovery_all():
    """Discover and add all feeds from all languages and categories"""
    print("\n" + "="*50)
    print("BULK DISCOVERY - ALL FEEDS")
    print("="*50)
    
    existing_urls = load_existing_config()
    all_working_feeds = {}
    total_tested = 0
    total_added = 0
    
    for language in LANGUAGE_SOURCES:
        print(f"\nProcessing: {language.upper()}")
        for category in LANGUAGE_SOURCES[language]:
            working_feeds = discover_and_add_feeds(language, category, existing_urls)
            all_working_feeds.update(working_feeds)
            total_tested += len(LANGUAGE_SOURCES[language][category])
            total_added += len(working_feeds)
    
    if all_working_feeds:
        save_to_config(all_working_feeds, "overwrite")
        print(f"\nBulk discovery completed!")
        print(f"Tested: {total_tested} feeds")
        print(f"Added: {total_added} working feeds")
        print(f"Skipped: {total_tested - total_added} feeds")
    else:
        print("No new working feeds found.")
    
    return all_working_feeds

def display_stats():
    """Display statistics about available feeds"""
    print("\n" + "="*50)
    print("FEED STATISTICS")
    print("="*50)
    
    total_feeds = 0
    for language in LANGUAGE_SOURCES:
        lang_total = sum(len(categories) for categories in LANGUAGE_SOURCES[language].values())
        total_feeds += lang_total
        print(f"{language.title():<12}: {lang_total} feeds")
        for category in LANGUAGE_SOURCES[language]:
            print(f"  - {category}: {len(LANGUAGE_SOURCES[language][category])} feeds")
    
    print(f"\n{'Total':<12}: {total_feeds} feeds")
    
    # Database statistics
    db_stats = feed_manager.get_feed_stats()
    if db_stats:
        total, active, avg_rate, avg_articles = db_stats
        print(f"\nDatabase Statistics:")
        print(f"Total feeds in DB: {total}")
        print(f"Active feeds: {active}")
        if avg_rate:
            print(f"Average success rate: {avg_rate:.1%}")
        if avg_articles:
            print(f"Average articles: {avg_articles:.1f}")
    
    existing_urls = load_existing_config()
    print(f"\nCurrent config.toml: {len(existing_urls)} feeds")

def export_opml(feeds, filename="feeds.opml"):
    """Export feeds to OPML format for easy import in other readers"""
    opml_template = '''<?xml version="1.0" encoding="UTF-8"?>
<opml version="1.0">
    <head>
        <title>RSS Feeds Export</title>
        <dateCreated>{date}</dateCreated>
    </head>
    <body>
        {outlines}
    </body>
</opml>'''
    
    outline_template = '<outline type="rss" text="{title}" title="{title}" xmlUrl="{url}"/>'
    
    outlines = []
    for feed_name, url in feeds.items():
        outlines.append(outline_template.format(title=feed_name, url=url))
    
    opml_content = opml_template.format(
        date=datetime.now().strftime("%a, %d %b %Y %H:%M:%S GMT"),
        outlines="\n        ".join(outlines)
    )
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(opml_content)
    
    print(f"OPML file exported: {filename}")

def import_opml(opml_file):
    """Import feeds from OPML file"""
    try:
        with open(opml_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        soup = BeautifulSoup(content, 'xml')
        feeds = {}
        
        for outline in soup.find_all('outline'):
            if outline.get('type') == 'rss' and outline.get('xmlUrl'):
                title = outline.get('title') or outline.get('text') or "Imported Feed"
                url = outline.get('xmlUrl')
                feed_name = re.sub(r'[^\w]', '_', title)
                feeds[feed_name] = url
        
        return feeds
    
    except Exception as e:
        print(f"Error importing OPML: {e}")
        return {}

def feed_analytics_dashboard():
    """Display analytics dashboard for feed performance"""
    conn = sqlite3.connect("rss_feeds.db")
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("FEED ANALYTICS DASHBOARD")
    print("="*60)
    
    # Overall statistics
    cursor.execute('''
        SELECT 
            COUNT(*) as total_feeds,
            SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active_feeds,
            AVG(success_rate) as avg_success_rate,
            AVG(article_count) as avg_articles
        FROM feeds
    ''')
    stats = cursor.fetchone()
    
    print(f"Total Feeds: {stats[0]}")
    print(f"Active Feeds: {stats[1]}")
    print(f"Average Success Rate: {stats[2]:.1%}" if stats[2] else "N/A")
    print(f"Average Articles: {stats[3]:.1f}" if stats[3] else "N/A")
    
    # Feed health status
    cursor.execute('''
        SELECT 
            CASE 
                WHEN success_rate > 0.9 THEN 'Excellent'
                WHEN success_rate > 0.7 THEN 'Good'
                WHEN success_rate > 0.5 THEN 'Fair'
                ELSE 'Poor'
            END as health,
            COUNT(*) as count
        FROM feeds 
        WHERE success_rate IS NOT NULL
        GROUP BY health
        ORDER BY count DESC
    ''')
    
    print("\nFeed Health Distribution:")
    for health, count in cursor.fetchall():
        print(f"  {health}: {count} feeds")
    
    # Most productive feeds
    cursor.execute('''
        SELECT name, article_count, success_rate
        FROM feeds 
        WHERE is_active = 1 AND article_count > 0
        ORDER BY article_count DESC 
        LIMIT 10
    ''')
    
    print("\nTop 10 Most Productive Feeds:")
    for name, count, rate in cursor.fetchall():
        print(f"  {name}: {count} articles ({rate:.1%} success)")
    
    conn.close()

def backup_config(backup_name=None):
    """Backup current configuration"""
    if not backup_name:
        backup_name = f"config_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    if os.path.exists('config.toml'):
        shutil.copy2('config.toml', f'{backup_name}.toml')
        print(f"Configuration backed up as: {backup_name}.toml")
    
    # Also backup database
    if os.path.exists('rss_feeds.db'):
        shutil.copy2('rss_feeds.db', f'{backup_name}.db')
        print(f"Database backed up as: {backup_name}.db")

def restore_config(backup_file):
    """Restore configuration from backup"""
    if backup_file.endswith('.toml'):
        shutil.copy2(backup_file, 'config.toml')
        print("Configuration restored successfully")
    elif backup_file.endswith('.db'):
        shutil.copy2(backup_file, 'rss_feeds.db')
        print("Database restored successfully")

def interactive_feed_cleanup():
    """Interactive tool to clean up inactive or low-quality feeds"""
    conn = sqlite3.connect("rss_feeds.db")
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT name, url, last_status, success_rate, article_count
        FROM feeds 
        WHERE is_active = 1
        ORDER BY success_rate ASC, article_count ASC
    ''')
    
    problematic_feeds = cursor.fetchall()
    
    if not problematic_feeds:
        print("No problematic feeds found!")
        return
    
    print("\n" + "="*60)
    print("FEED CLEANUP TOOL")
    print("="*60)
    print("The following feeds may need attention:\n")
    
    for i, (name, url, status, success_rate, articles) in enumerate(problematic_feeds[:20], 1):
        print(f"{i}. {name}")
        print(f"   URL: {urlparse(url).netloc}")
        print(f"   Status: {status}")
        print(f"   Success Rate: {success_rate:.1%}" if success_rate else "   Success Rate: N/A")
        print(f"   Articles: {articles}")
        print()
    
    choice = input("Select feed number to deactivate (or 'all' for all, 'none' to skip): ").strip()
    
    if choice.lower() == 'all':
        cursor.execute('UPDATE feeds SET is_active = 0 WHERE success_rate < 0.5')
        conn.commit()
        print("All low-success feeds deactivated")
    elif choice.lower() != 'none':
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(problematic_feeds):
                feed_url = problematic_feeds[idx][1]
                cursor.execute('UPDATE feeds SET is_active = 0 WHERE url = ?', (feed_url,))
                conn.commit()
                print(f"Feed deactivated: {problematic_feeds[idx][0]}")
        except ValueError:
            print("Invalid selection")
    
    conn.close()

def enhanced_main_menu():
    """Enhanced main menu with all features"""
    print("="*60)
    print("🚀 ENHANCED RSS FEED DISCOVERY & MANAGEMENT TOOL")
    print("="*60)
    
    # Check dependencies
    try:
        import requests
        import feedparser
        from bs4 import BeautifulSoup
    except ImportError:
        print("Installing dependencies...")
        import subprocess
        subprocess.check_call(['pip', 'install', 'requests', 'feedparser', 'beautifulsoup4'])
        print("Dependencies installed!")
    
    while True:
        print("\n📋 MAIN MENU")
        print("-" * 40)
        print("1. 🔍 Language & Category Discovery")
        print("2. 💻 GitHub RSS Collections")
        print("3. 🌐 Discover from Website URL")
        print("4. ⚡ Bulk Discovery (All Feeds)")
        print("5. 📊 Analytics Dashboard")
        print("6. 🧹 Feed Cleanup Tool")
        print("7. 💾 Backup/Restore Configuration")
        print("8. 📤 Export/Import (OPML)")
        print("9. 🗺 Sitemap Discovery")
        print("0. ❌ Exit")
        
        choice = input("\nSelect option: ").strip()
        
        if choice == '1':
            languages = display_language_menu()
            lang_choice = input("\nSelect language: ").strip()
            
            if lang_choice == '0':
                continue
                
            if lang_choice not in languages:
                print("Invalid selection")
                continue
                
            lang_name, lang_code = languages[lang_choice]
            
            category_menu = display_category_menu(lang_name, lang_code)
            cat_choice = input("\nSelect category: ").strip()
            
            if cat_choice == '0':
                continue
                
            existing_urls = load_existing_config()
            working_feeds = {}
            
            if cat_choice == '9':
                if lang_code == 'all':
                    for language in LANGUAGE_SOURCES:
                        for category in LANGUAGE_SOURCES[language]:
                            feeds = discover_and_add_feeds(language, category, existing_urls)
                            working_feeds.update(feeds)
                else:
                    for category in LANGUAGE_SOURCES[lang_code]:
                        feeds = discover_and_add_feeds(lang_code, category, existing_urls)
                        working_feeds.update(feeds)
            elif cat_choice in category_menu:
                _, category_code, _ = category_menu[cat_choice]
                if lang_code == 'all':
                    for language in LANGUAGE_SOURCES:
                        if category_code in LANGUAGE_SOURCES[language]:
                            feeds = discover_and_add_feeds(language, category_code, existing_urls)
                            working_feeds.update(feeds)
                else:
                    working_feeds = discover_and_add_feeds(lang_code, category_code, existing_urls)
            else:
                print("Invalid category selection")
                continue
            
            if working_feeds:
                save_choice = input(f"Add {len(working_feeds)} working feeds to config.toml? (y/n): ").strip().lower()
                if save_choice in ['y', 'yes']:
                    save_to_config(working_feeds, "append")
                    print(f"Successfully added {len(working_feeds)} feeds to config.toml")
                else:
                    print("Feeds not saved.")
            else:
                print("No working feeds found to add.")
                
        elif choice == '2':
            github_feeds = github_rss_collections_menu()
            if github_feeds:
                save_to_config(github_feeds, "append")
                print(f"Added {len(github_feeds)} feeds from GitHub collections to config.toml")
                
        elif choice == '3':
            website_url = input("Enter website URL to discover RSS feeds: ").strip()
            if website_url:
                if not website_url.startswith(('http://', 'https://')):
                    website_url = 'https://' + website_url
                
                existing_urls = load_existing_config()
                website_feeds = discover_from_website_url(website_url, existing_urls)
                
                if website_feeds:
                    save_to_config(website_feeds, "append")
                    print(f"Added {len(website_feeds)} feeds from website to config.toml")
                else:
                    print("No working feeds found on this website.")
        
        elif choice == '4':
            bulk_discovery_all()
            
        elif choice == '5':
            feed_analytics_dashboard()
            
        elif choice == '6':
            interactive_feed_cleanup()
            
        elif choice == '7':
            print("\n💾 Backup/Restore Options:")
            print("1. Create Backup")
            print("2. Restore from Backup")
            backup_choice = input("Select: ").strip()
            if backup_choice == '1':
                backup_name = input("Enter backup name (optional): ").strip()
                backup_config(backup_name if backup_name else None)
            elif backup_choice == '2':
                backup_file = input("Enter backup filename: ").strip()
                if os.path.exists(backup_file):
                    restore_config(backup_file)
                else:
                    print("Backup file not found")
        
        elif choice == '8':
            print("\n📤 Export/Import Options:")
            print("1. Export to OPML")
            print("2. Import from OPML")
            opml_choice = input("Select: ").strip()
            if opml_choice == '1':
                existing_urls = load_existing_config()
                export_opml(existing_urls)
            elif opml_choice == '2':
                opml_file = input("Enter OPML filename: ").strip()
                if os.path.exists(opml_file):
                    imported_feeds = import_opml(opml_file)
                    if imported_feeds:
                        save_to_config(imported_feeds, "append")
                        print(f"Imported {len(imported_feeds)} feeds from OPML")
                else:
                    print("OPML file not found")
        
        elif choice == '9':
            domain = input("Enter domain to discover sitemap (e.g., https://example.com): ").strip()
            if domain:
                sitemap_url = find_sitemap(domain)
                if sitemap_url:
                    print(f"Found sitemap: {sitemap_url}")
                    feeds = discover_feeds_from_sitemap(sitemap_url)
                    if feeds:
                        existing_urls = load_existing_config()
                        working_feeds = {}
                        for title, url in feeds:
                            if url not in existing_urls.values():
                                is_active, status, entry_count, feed_title = test_feed(url)
                                if is_active:
                                    feed_name = get_feed_name(url, "sitemap", "discovered", feed_title)
                                    working_feeds[feed_name] = url
                        
                        if working_feeds:
                            save_to_config(working_feeds, "append")
                            print(f"Added {len(working_feeds)} feeds from sitemap")
                else:
                    print("No sitemap found")
                    
        elif choice == '0':
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid choice. Please try again.")

if __name__ == "__main__":
    enhanced_main_menu()