import asyncio
import re
from datetime import datetime, timezone
import requests
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
from concurrent.futures import ThreadPoolExecutor
import time
import random
from typing import Optional, Dict, List, Set
import os
import json
from urllib.parse import urljoin, parse_qs


# Database Config - Cloud-ready with env vars
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "Nellis"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "Sophie0505!105"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}


LOCATION_MAP = {
    "delran_philly": 6,  # Use Delran's cookie for both zones
    "dallas": 5,
    "las_vegas": 1,
    "salt_lake": 7,
}


BASE_SEARCH_URL = "https://www.nellisauction.com/search?query=&sortBy=retail_price_desc&page={page}"


# Configuration
CONFIG = {
    "max_retries": 3,
    "retry_delay": 3,
    "request_delay": (2, 5),
    "batch_size": 25,
    "max_workers": 5,
    "timeout": 30,
    "page_delay": (5, 15),
    "max_items_per_location": 500,
}


# ---- NO PROXIES: all requests go direct ----


# ---- Database Manager ----
class DatabaseManager:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.connect()
        self.init_tables()
    
    def connect(self):
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            self.conn.autocommit = False
        except Exception as e:
            log(f"Database connection failed: {e}", "ERROR")
            raise
    
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def init_tables(self):
        """Create tables if not exist"""
        create_items = """
        CREATE TABLE IF NOT EXISTS auction_items (
            id SERIAL PRIMARY KEY,
            location_id INT,
            location_name VARCHAR(50),
            item_id VARCHAR(50),
            title TEXT,
            retail_price DECIMAL(10,2),
            current_price DECIMAL(10,2),
            url TEXT,
            image_url TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(location_id, item_id)
        );
        """
        try:
            self.cursor.execute(create_items)
            self.conn.commit()
            log("Tables ready", "SUCCESS")
        except Exception as e:
            self.conn.rollback()
            log(f"Table init failed: {e}", "ERROR")
    
    def insert_items_batch(self, items: List[Dict]):
        """Batch insert with conflict handling"""
        if not items:
            return
        
        insert_query = """
        INSERT INTO auction_items 
        (location_id, location_name, item_id, title, retail_price, current_price, url, image_url)
        VALUES (%(location_id)s, %(location_name)s, %(item_id)s, %(title)s, %(retail_price)s, 
                %(current_price)s, %(url)s, %(image_url)s)
        ON CONFLICT (location_id, item_id) DO UPDATE SET
            title = EXCLUDED.title,
            retail_price = EXCLUDED.retail_price,
            current_price = EXCLUDED.current_price,
            url = EXCLUDED.url,
            image_url = EXCLUDED.image_url,
            scraped_at = CURRENT_TIMESTAMP
        """
        try:
            execute_values(self.cursor, insert_query, items)
            self.conn.commit()
            log(f"Inserted/updated {len(items)} items", "SUCCESS")
        except Exception as e:
            self.conn.rollback()
            log(f"Batch insert failed: {e}", "ERROR")


# Global database manager
db = None


def refresh_db():
    global db
    try:
        if db:
            db.close()
    except Exception:
        pass
    db = DatabaseManager()


# Logging - FIXED: No emojis to avoid encoding issues
def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    levels = {
        "INFO": "[INFO]", 
        "SUCCESS": "[SUCCESS]", 
        "WARNING": "[WARNING]", 
        "ERROR": "[ERROR]"
    }
    icon = levels.get(level, "[INFO]")
    print(f"[{timestamp}] {icon} {msg}")


def fetch_location_cookie(location_id: int, location_name: str) -> Optional[str]:
    """POST to set shopping location and return __shopping-location cookie."""
    payloads = [
        {
            "shoppingLocationId": str(location_id),
            "referrer": "/search?sortBy=retail_price_desc&query="
        },
        {
            "shoppingLocationId": location_id
        }
    ]
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Content-Type': 'application/json',
        'Referer': 'https://www.nellisauction.com/',
        'Origin': 'https://www.nellisauction.com'
    }
    
    for payload in payloads:
        for attempt in range(CONFIG["max_retries"]):
            try:
                response = requests.post(
                    'https://www.nellisauction.com/api/shopping-location/set',  # FIXED: Verify this endpoint
                    json=payload,
                    headers=headers,
                    timeout=CONFIG["timeout"]
                )
                response.raise_for_status()
                cookies = response.cookies.get_dict()
                if '__shopping-location' in cookies:
                    log(f"Got cookie for {location_name} ({location_id})")
                    return cookies['__shopping-location']
            except requests.RequestException as e:
                log(f"Attempt {attempt+1} failed for {location_name}: {e}", "WARNING")
                time.sleep(random.uniform(*CONFIG["request_delay"]))
    
    log(f"Failed to get cookie for {location_name}", "ERROR")
    return None


async def parse_items(html_content: str, location_name: str, location_id: int) -> List[Dict]:
    """Parse auction items from HTML using selectors."""
    items = []
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Common selectors - inspect page to refine
    item_elements = soup.select('.product-item, .auction-item, [data-testid="product"], .search-result-item')
    
    for elem in item_elements[:CONFIG["batch_size"]]:
        try:
            title_elem = elem.select_one('.product-title, h3, .item-name, [class*="title"]')
            price_elem = elem.select_one('.current-bid, .price, [class*="price"]')
            retail_elem = elem.select_one('.retail-price, .msrp, [class*="retail"]')
            link_elem = elem.select_one('a')
            img_elem = elem.select_one('img')
            
            item = {
                'location_id': location_id,
                'location_name': location_name,
                'item_id': elem.get('data-item-id') or elem.get('id') or elem.get('data-id') or '',
                'title': title_elem.get_text(strip=True) if title_elem else '',
                'retail_price': 0.0,
                'current_price': 0.0,
                'url': urljoin('https://www.nellisauction.com', link_elem.get('href')) if link_elem else '',
                'image_url': img_elem.get('src') or img_elem.get('data-src') or '' if img_elem else ''
            }
            
            # Regex for prices
            if retail_elem:
                retail_match = re.search(r'[\d,]+\.?\d*', retail_elem.get_text())
                if retail_match:
                    item['retail_price'] = float(retail_match.group().replace(',', ''))
            
            if price_elem:
                price_match = re.search(r'[\d,]+\.?\d*', price_elem.get_text())
                if price_match:
                    item['current_price'] = float(price_match.group().replace(',', ''))
            
            if item['title'] and (item['item_id'] or item['url']):
                items.append(item)
                
        except Exception as e:
            log(f"Parse error for item in {location_name}: {e}", "WARNING")
            continue
    
    return items


async def crawl_location_pages(crawler: AsyncWebCrawler, location_name: str, location_id: int, cookie: str, max_items: int):
    """Crawl pages for one location until max_items reached."""
    all_items = []
    page = 1
    items_count = 0
    
    browser_config = BrowserConfig(headless=True)
    cookies_dict = {'__shopping-location': cookie}
    
    while items_count < max_items:
        run_config = CrawlerRunConfig(
            browser_config=browser_config,
            cookies=cookies_dict,
            wait_for="body",
            timeout=CONFIG["timeout"]
        )
        
        try:
            result = await crawler.arun(BASE_SEARCH_URL.format(page=page), run_config)
            if not result.success or not result.html:
                log(f"Crawl failed for {location_name} page {page}")
                break
            
            page_items = await parse_items(result.html, location_name, location_id)
            if not page_items:
                log(f"No items found on {location_name} page {page} - stopping")
                break
            
            all_items.extend(page_items)
            items_count += len(page_items)
            
            db.insert_items_batch(page_items)
            
            log(f"{location_name} page {page}: {len(page_items)} items (total: {items_count})")
            
            if len(page_items) < CONFIG["batch_size"]:
                break
                
        except Exception as e:
            log(f"Crawl error {location_name} page {page}: {e}", "ERROR")
            break
        
        page += 1
        time.sleep(random.uniform(*CONFIG["page_delay"]))
    
    return all_items


async def main():
    refresh_db()
    
    crawler = AsyncWebCrawler()
    
    for loc_name, loc_id in LOCATION_MAP.items():
        log(f"Starting {loc_name} (ID: {loc_id})")
        
        cookie = fetch_location_cookie(loc_id, loc_name)
        if not cookie:
            log(f"Skipping {loc_name} - no cookie", "WARNING")
            continue
        
        await crawl_location_pages(crawler, loc_name, loc_id, cookie, CONFIG["max_items_per_location"])
        
        time.sleep(random.uniform(10, 20))
    
    log("Scraping complete!")
    crawler.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Stopped by user")
    except Exception as e:
        log(f"Fatal error: {e}", "ERROR")
