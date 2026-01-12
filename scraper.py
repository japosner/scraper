import asyncio
import re
from datetime import datetime, timezone
import requests
import psycopg
from psycopg.rows import dict_row
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
from concurrent.futures import ThreadPoolExecutor
import time
import random
from typing import Optional, Dict, List, Set
import os
from urllib.parse import urlparse, unquote


# Database Config - Render-friendly
def _get_db_config():
    """Read from Render DATABASE_URL or individual env vars"""
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        u = urlparse(database_url)
        return {
            "host": u.hostname or "",
            "port": u.port or 5432,
            "dbname": (u.path or "").lstrip("/"),
            "user": unquote(u.username or ""),
            "password": unquote(u.password or ""),
            "sslmode": os.getenv("DB_SSLMODE", "require"),
        }
    
    return {
        "dbname": os.getenv("DB_NAME", "Nellis"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "Sophie0505!105"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "sslmode": os.getenv("DB_SSLMODE", "require"),
    }


DB_CONFIG = _get_db_config()


LOCATION_MAP = {
    "delran_philly": 6,
    "dallas": 5,
    "las_vegas": 1,
    "salt_lake": 7,
}


BASE_SEARCH_URL = (
    "https://www.nellisauction.com/search"
    "?query=&sortBy=retail_price_desc"
    "&page={page}"
)


CONFIG = {
    "max_retries": 3,
    "retry_delay": 3,
    "request_delay": (2, 5),
    "batch_size": 25,
    "max_workers": 3,
    "timeout": 30,
    "page_delay": (10, 25),
    "max_items_per_location": 500,
}


# ---- Database Manager ----

class DatabaseManager:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.connect()
    
    def connect(self):
        try:
            self.conn = psycopg.connect(**DB_CONFIG, row_factory=dict_row)
            self.conn.autocommit = False
        except Exception as e:
            log(f"Database connection failed: {e}", "ERROR")
            raise
    
    def close(self):
        if self.cursor:
            try:
                self.cursor.close()
            except:
                pass
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
    
    def execute_with_retry(self, query, params=None, max_retries=3):
        for attempt in range(max_retries):
            try:
                self.cursor.execute(query, params)
                return True
            except (psycopg.OperationalError, psycopg.InterfaceError) as e:
                log(f"DB connection error (attempt {attempt + 1}): {e}", "WARNING")
                if attempt < max_retries - 1:
                    self.connect()
                    time.sleep(CONFIG["retry_delay"])
                else:
                    raise
        return False


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


# Logging
def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    levels = {"INFO": "[INFO]", "SUCCESS": "[SUCCESS]", "WARNING": "[WARNING]", "ERROR": "[ERROR]"}
    icon = levels.get(level, "[INFO]")
    print(f"[{timestamp}] {icon} {msg}")


def fetch_location_cookie(location_id: int) -> Optional[str]:
    """POST to set shopping location and return __shopping-location cookie."""
    payloads = [
        {
            "shoppingLocationId": str(location_id),
            "referrer": "/search?sortBy=retail_price_desc&query="
        },
        {
            "shoppingLocationId": location_id,
            "referrer": "/search?sortBy=retail_price_desc&query="
        },
        {
            "locationId": str(location_id),
            "referrer": "/search"
        }
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nellisauction.com/search",
        "Origin": "https://www.nellisauction.com",
        "X-Requested-With": "XMLHttpRequest"
    }

    session = requests.Session()
    
    for payload_idx, payload in enumerate(payloads):
        for attempt in range(CONFIG["max_retries"]):
            try:
                time.sleep(random.uniform(*CONFIG["request_delay"]))
                
                urls_to_try = [
                    "https://www.nellisauction.com/change-shopping-location?_data=routes%2Fchange-shopping-location",
                    "https://www.nellisauction.com/change-shopping-location",
                    "https://www.nellisauction.com/api/location/change"
                ]
                
                url_to_use = urls_to_try[0] if payload_idx == 0 else urls_to_try[min(payload_idx, len(urls_to_try)-1)]
                
                response = session.post(
                    url_to_use,
                    data=payload,
                    headers=headers,
                    timeout=CONFIG["timeout"],
                )
                
                if response.status_code == 200:
                    cookie = session.cookies.get("__shopping-location")
                    if cookie:
                        log(f"Cookie fetched for location {location_id} (payload #{payload_idx + 1})", "SUCCESS")
                        return cookie
                    else:
                        log(f"No cookie in response for location {location_id} (payload #{payload_idx + 1})", "WARNING")
                elif response.status_code == 400:
                    log(f"400 error for location {location_id} with payload #{payload_idx + 1}", "WARNING")
                    break
                else:
                    response.raise_for_status()
                    
            except requests.exceptions.RequestException as e:
                log(f"Attempt {attempt + 1} failed for location {location_id} (payload #{payload_idx + 1}): {e}", "WARNING")
                if attempt < CONFIG["max_retries"] - 1:
                    time.sleep(CONFIG["retry_delay"] * (attempt + 1))
                else:
                    break
    
    log(f"All methods failed for location {location_id}", "ERROR")
    return None


async def fetch_with_requests_fallback(url: str, cookie_value: str) -> Optional[str]:
    """Fallback method using requests with session cookies."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        
        cookies = {"__shopping-location": cookie_value}
        
        session = requests.Session()
        session.headers.update(headers)
        session.cookies.update(cookies)
        
        await asyncio.sleep(random.uniform(2, 4))
        
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        if response.text and len(response.text) > 1000:
            log("Requests fallback succeeded", "SUCCESS")
            return response.text
        else:
            log("Requests fallback returned minimal content", "WARNING")
            return None
            
    except Exception as e:
        log(f"Requests fallback failed: {e}", "ERROR")
        return None


async def fetch_search_page_html(location_key: str, page: int) -> Optional[str]:
    """Use requests only - bypassing crawl4ai completely for now."""
    loc_id = LOCATION_MAP[location_key]
    url = BASE_SEARCH_URL.format(page=page)
    log(f"Loading page {page} for {location_key}: {url}")

    cookie_value = fetch_location_cookie(loc_id)
    if not cookie_value:
        log(f"No cookie for location '{location_key}' - skipping", "ERROR")
        return None

    log(f"Using requests-only method for page {page} (crawl4ai bypassed)", "INFO")
    return await fetch_with_requests_fallback(url, cookie_value)


def parse_search_cards(html: str, location_key: str) -> List[Dict]:
    """Parse auction cards from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("div[data-ax='item-card-container']")
    items = []
    location_id = LOCATION_MAP.get(location_key)
    location = location_key.replace("_", " ").title()

    log(f"Found {len(cards)} cards to parse for {location}")

    for i, card in enumerate(cards):
        try:
            title_selectors = [
                "a[data-ax='item-card-title-link'] h6",
                "h6",
                ".item-title",
                "[data-testid='item-title']"
            ]
            link_selectors = [
                "a[data-ax='item-card-title-link']",
                "a[href*='/p/']",
                ".item-link"
            ]
            
            title_el = None
            for sel in title_selectors:
                title_el = card.select_one(sel)
                if title_el:
                    break
            
            link_el = None
            for sel in link_selectors:
                link_el = card.select_one(sel)
                if link_el and link_el.has_attr("href"):
                    break

            if not title_el or not link_el:
                continue

            title = title_el.get_text(strip=True)
            href = link_el["href"]
            link = f"https://www.nellisauction.com{href}" if href else None
            
            img_el = card.select_one("img")
            image = img_el.get("src") or img_el.get("data-src") if img_el else None
            if image and not image.startswith("http"):
                image = f"https://www.nellisauction.com{image}"

            lot_id, category = None, None
            if href:
                parts = [p for p in href.strip("/").split("/") if p]
                if len(parts) >= 2:
                    category = parts[1].replace("-", " ").title()
                
                lot_patterns = [
                    r"/p/.+?/(\d+)",
                    r"lot[_-]?(\d+)",
                    r"item[_-]?(\d+)"
                ]
                for pattern in lot_patterns:
                    m = re.search(pattern, href, re.I)
                    if m:
                        lot_id = int(m.group(1))
                        break

            text = card.get_text(" ", strip=True)
            retail, bid = 0.0, 0.0
            
            price_patterns = [
                (r"EST\.?\s*RETAIL[:\s]*\$?([\d,]+\.?\d*)", "retail"),
                (r"RETAIL[:\s]*\$?([\d,]+\.?\d*)", "retail"),
                (r"CURRENT\s*PRICE[:\s]*\$?([\d,]+\.?\d*)", "bid"),
                (r"CURRENT\s*BID[:\s]*\$?([\d,]+\.?\d*)", "bid"),
                (r"BID[:\s]*\$?([\d,]+\.?\d*)", "bid"),
            ]
            
            for pattern, price_type in price_patterns:
                m = re.search(pattern, text, re.I)
                if m:
                    try:
                        price_val = float(m.group(1).replace(",", ""))
                        if price_type == "retail":
                            retail = price_val
                        else:
                            bid = price_val
                    except ValueError:
                        continue

            cond_selectors = [
                "span.px-3.whitespace-nowrap",
                ".condition-tag",
                ".badge",
                "span[class*='condition']"
            ]
            cond_tags = []
            for sel in cond_selectors:
                tags = card.select(sel)
                if tags:
                    cond_tags.extend([tag.get_text(strip=True) for tag in tags])
                    break
            
            condition = ", ".join(c for c in cond_tags if c and len(c.strip()) > 1) if cond_tags else None

            stars = 0
            star_selectors = [
                "button[data-ax='item-card-hide-star-rating-button'] svg[class*='fill-starRating-']",
                ".star-rating .filled",
                "[data-rating] .star.filled"
            ]
            for sel in star_selectors:
                filled = card.select(sel)
                if filled:
                    stars = min(len(filled), 5)
                    break

            time_selectors = [
                "p.text-gray-900.font-semibold",
                ".time-left",
                "[data-time-left]"
            ]
            time_left = None
            for sel in time_selectors:
                tl = card.select_one(sel)
                if tl:
                    time_left = tl.get_text(strip=True)
                    break

            if not title or not link:
                continue

            items.append({
                "title": title,
                "lot_id": lot_id,
                "retail_price": retail,
                "current_bid": bid,
                "location": location,
                "location_id": location_id,
                "condition": condition,
                "category": category,
                "star_rating": stars,
                "time_left": time_left,
                "link": link,
                "image_url": image
            })

        except Exception as e:
            log(f"Error parsing card {i}: {e}", "ERROR")
            continue

    log(f"Successfully parsed {len(items)} valid items for location '{location}'", "SUCCESS")
    return items


def insert_basic_items(items: List[Dict]) -> int:
    """Insert items with bid history tracking."""
    if not items:
        return 0

    count = 0
    history_count = 0
    failed = 0

    for item in items:
        try:
            item["link"] = item["link"].rstrip("/")

            db.cursor.execute("""
                INSERT INTO auction_bid_history (
                    link, title, retail_price, current_bid,
                    location, condition, category, star_rating, scraped_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
                )
            """, (
                item["link"],
                item["title"],
                item.get("retail_price", 0),
                item.get("current_bid", 0),
                item["location"],
                item.get("condition"),
                None,
                item.get("star_rating", 0)
            ))

            db.cursor.execute("""
                INSERT INTO auction_listings (
                    title, lot_id, retail_price, current_bid,
                    location, location_id, condition,
                    star_rating, time_left, link, image_url, scraped_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, CURRENT_TIMESTAMP
                )
                ON CONFLICT (link) DO UPDATE SET
                    current_bid = EXCLUDED.current_bid,
                    retail_price = EXCLUDED.retail_price,
                    time_left = EXCLUDED.time_left,
                    star_rating = EXCLUDED.star_rating,
                    condition = EXCLUDED.condition,
                    scraped_at = CURRENT_TIMESTAMP
            """, (
                item["title"],
                item.get("lot_id"),
                item.get("retail_price", 0),
                item.get("current_bid", 0),
                item["location"],
                item.get("location_id"),
                item.get("condition"),
                item.get("star_rating", 0),
                item.get("time_left"),
                item["link"],
                item.get("image_url")
            ))

            db.conn.commit()
            count += 1
            history_count += 1

        except Exception as e:
            db.conn.rollback()
            failed += 1
            log(f"Insert failed for '{item.get('title', '?')[:50]}...': {e}", "ERROR")
            continue

    if history_count > 0:
        try:
            db.cursor.execute("""
                SELECT COUNT(*) FROM auction_bid_history
                WHERE scraped_at >= NOW() - INTERVAL '5 minutes'
            """)
            recent_history = db.cursor.fetchone()[0]
            log(f"Verification: {recent_history} recent history records in database")
        except Exception as e:
            log(f"Verification error: {e}", "WARNING")

    if failed > 0:
        log(f"Results: {count} items processed, {history_count} history records, {failed} failed", "WARNING")
    else:
        log(f"Results: {count} items and {history_count} history records successfully inserted", "SUCCESS")

    return count


def enrich_listing(item: dict) -> dict:
    """Fetch structured detail data via JSON."""
    try:
        normalized_link = item["link"].rstrip("/")
        api_url = normalized_link + "?_data=routes%2Fp.%24title.%24productId._index"
        headers = {"User-Agent": "Mozilla/5.0"}

        time.sleep(random.uniform(1.5, 4.0))

        backoff = [1, 2, 4, 8]
        res = None
        for delay in backoff:
            try:
                res = requests.get(api_url, headers=headers, timeout=15)

                if res.status_code == 429:
                    log(f"429 from JSON API, sleeping {delay}s", "WARNING")
                    time.sleep(delay)
                    continue

                if res.status_code >= 400:
                    log(f"[ENRICH] JSON API {res.status_code} for {normalized_link}", "WARNING")
                break

            except requests.exceptions.RequestException as e:
                log(f"Error calling JSON API: {e}", "WARNING")
                time.sleep(delay)

        if not res:
            raise RuntimeError("Failed to fetch JSON API after retries")

        res.raise_for_status()

        data = res.json()
        product = data.get("product", {})
        grade = product.get("grade", {})
        location = product.get("location", {})

        def parse_iso_datetime(val: Optional[str]) -> Optional[datetime]:
            if not val:
                return None
            try:
                if isinstance(val, str) and val.endswith("Z"):
                    val = val.replace("Z", "+00:00")
                return datetime.fromisoformat(val)
            except Exception:
                return None

        open_info = product.get("openTime") or {}
        close_info = product.get("closeTime") or {}

        open_iso = open_info.get("value") or open_info.get("iso")
        close_iso = close_info.get("value") or close_info.get("iso")

        open_dt = parse_iso_datetime(open_iso)
        close_dt = parse_iso_datetime(close_iso)

        time_left_str = None
        if close_dt is not None:
            if close_dt.tzinfo is None:
                now = datetime.now()
            else:
                now = datetime.now(timezone.utc)
                close_dt = close_dt.astimezone(timezone.utc)

            if now >= close_dt:
                time_left_str = "Ended"
            else:
                delta = close_dt - now
                days = delta.days
                seconds = delta.seconds
                hours = seconds // 3600
                minutes = (seconds % 3600) // 60

                parts = []
                if days:
                    parts.append(f"{days}d")
                parts.append(f"{hours:02d}h")
                parts.append(f"{minutes:02d}m")
                time_left_str = " ".join(parts)

        enriched = {
            "link": normalized_link,
            "brand": product.get("brand"),
            "color": product.get("color"),
            "size": product.get("size"),
            "subcategory": product.get("taxonomyLevel2"),
            "condition_grade": grade.get("conditionType", {}).get("description"),
            "damage_grade": grade.get("damageType", {}).get("description"),
            "functional": grade.get("functionalType", {}).get("description") == "Yes",
            "missing_parts": grade.get("missingPartsType", {}).get("description") == "Yes",
            "in_package": grade.get("packageType", {}).get("description") == "Yes",
            "assembly_required": grade.get("assemblyType", {}).get("description") == "Yes",
            "pickup_city": location.get("city"),
            "pickup_zip": location.get("zipCode"),
            "open_time": open_dt,
            "close_time": close_dt,
            "time_left": time_left_str,
        }

        kwargs = {"headers": headers, "timeout": 15}
        html_res = requests.get(normalized_link, **kwargs)
        if html_res.status_code == 200:
            soup = BeautifulSoup(html_res.text, "html.parser")
            found_text = next((s for s in soup.stripped_strings if s.startswith("Found in ")), None)
            if found_text:
                parts = found_text.replace("Found in", "").strip().split(" ", 1)
                if len(parts) == 2:
                    enriched["category"] = parts[0].strip()
                    enriched["subcategory"] = parts[1].strip()
                else:
                    enriched["category"] = found_text.replace("Found in", "").strip()
        else:
            enriched["category"] = None

        if not enriched.get("brand"):
            log(f"[NOTE] Brand missing for: {normalized_link}", "WARNING")

        return enriched

    except Exception as e:
        log(f"[ENRICH] Error for '{item.get('title','?')[:40]}': {e}", "ERROR")
        return {"link": item["link"].rstrip("/")}


def batch_enrich(items: List[Dict]) -> List[Dict]:
    """Enrich listings in parallel."""
    if not items:
        log("No items to enrich.", "INFO")
        return []

    to_enrich = items[:]

    log(f"Enriching {len(to_enrich)} items with {CONFIG['max_workers']} workers")

    with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as executor:
        enriched = list(executor.map(enrich_listing, to_enrich))

    valid = [
        e for e in enriched
        if e.get("link") and (
            e.get("brand") or e.get("category") or e.get("subcategory") or e.get("condition_grade")
        )
    ]

    skipped = [e for e in enriched if e.get("link") and e not in valid]
    for item in skipped:
        log(f"[SKIP] Enrichment failed or incomplete for: {item['link']}", "WARNING")

    log(f"Successfully enriched {len(valid)}/{len(to_enrich)} items", "SUCCESS")
    return valid


def update_enriched_items(items: list) -> int:
    """Update enriched items in database."""
    if not items:
        log("No items to update.", "INFO")
        return 0

    updated = 0
    failed = 0
    
    for item in items:
        try:
            db.cursor.execute("""
                UPDATE auction_listings
                SET brand = %s,
                    color = %s,
                    size = %s,
                    condition_grade = %s,
                    damage_grade = %s,
                    functional = %s,
                    missing_parts = %s,
                    in_package = %s,
                    assembly_required = %s,
                    category = %s,
                    subcategory = %s,
                    pickup_city = %s,
                    pickup_zip = %s,
                    open_time  = COALESCE(%s, open_time),
                    close_time = COALESCE(%s, close_time),
                    time_left  = COALESCE(%s, time_left),
                    enriched_at = NOW()
                WHERE link = %s
            """, (
                item.get("brand"),
                item.get("color"),
                item.get("size"),
                item.get("condition_grade"),
                item.get("damage_grade"),
                item.get("functional"),
                item.get("missing_parts"),
                item.get("in_package"),
                item.get("assembly_required"),
                item.get("category"),
                item.get("subcategory"),
                item.get("pickup_city"),
                item.get("pickup_zip"),
                item.get("open_time"),
                item.get("close_time"),
                item.get("time_left"),
                item["link"],
            ))
            
            if db.cursor.rowcount > 0:
                updated += 1
            else:
                log(f"[WARNING] No rows updated for: {item['link']}", "WARNING")
                
        except Exception as e:
            log(f"Failed to update {item['link']}: {e}", "ERROR")
            db.conn.rollback()
            failed += 1
            continue

    try:
        db.conn.commit()
        log(f"Successfully updated {updated}/{len(items)} enriched items ({failed} failed)", "SUCCESS")
        return updated
    except Exception as e:
        log(f"Commit failed: {e}", "ERROR")
        db.conn.rollback()
        return 0


async def crawl_location(location_key: str, max_pages: Optional[int] = None):
    """Crawl a given location across pages, inserting basic items and enriching."""
    page = 1
    total_inserted = 0

    while True:
        if max_pages is not None and page > max_pages:
            break

        html = await fetch_search_page_html(location_key, page)
        if not html:
            log(f"No HTML for {location_key} page {page} – stopping.", "WARNING")
            break

        items = parse_search_cards(html, location_key)
        if not items:
            log(f"No items for {location_key} page {page} – stopping.", "INFO")
            break

        inserted = insert_basic_items(items)
        total_inserted += inserted
        log(f"Inserted {inserted} items for {location_key} page {page} (total {total_inserted})")

        enriched_items = batch_enrich(items)
        if enriched_items:
            update_enriched_items(enriched_items)

        page += 1
        delay = random.uniform(*CONFIG["page_delay"])
        log(f"Waiting {delay:.1f}s before next page...", "INFO")
        await asyncio.sleep(delay)

    log(f"Finished crawling location {location_key} with {total_inserted} items inserted.", "SUCCESS")


async def main():
    refresh_db()
    """Main function with bid history tracking and improved error handling."""
    start_time = datetime.now()
    log("Starting Nellis Auction Scraper with Bid History Tracking")

    try:
        db.cursor.execute("SELECT COUNT(*) FROM auction_listings")
        initial_listings = db.cursor.fetchone()[0]

        db.cursor.execute("SELECT COUNT(*) FROM auction_bid_history")
        initial_history = db.cursor.fetchone()[0]

        log(f"Starting counts - Listings: {initial_listings:,}, History: {initial_history:,}")
    except Exception as e:
        log(f"Error getting initial counts: {e}", "WARNING")
        initial_listings = initial_history = 0

    try:
        for i, location_key in enumerate(LOCATION_MAP.keys(), 1):
            location_start = datetime.now()
            log(f"Processing location {i}/{len(LOCATION_MAP)}: {location_key}")
            await crawl_location(location_key, max_pages=None)
            
            location_runtime = datetime.now() - location_start
            log(f"Completed {location_key} in {location_runtime}")
            
            if i < len(LOCATION_MAP):
                delay = random.uniform(10, 20)
                log(f"Waiting {delay:.1f}s before next location...")
                await asyncio.sleep(delay)

    except KeyboardInterrupt:
        log("Scraping interrupted by user", "WARNING")
    except Exception as e:
        log(f"Unexpected error in main: {e}", "ERROR")
    finally:
        try:
            db.cursor.execute("SELECT COUNT(*) FROM auction_listings")
            final_listings = db.cursor.fetchone()[0]
            
            db.cursor.execute("SELECT COUNT(*) FROM auction_bid_history")
            final_history = db.cursor.fetchone()[0]
            
            new_listings = final_listings - initial_listings
            new_history = final_history - initial_history
            
            log(f"Final counts - Listings: {final_listings:,} (+{new_listings:,}), History: {final_history:,} (+{new_history:,})")
            
            db.cursor.execute("""
                SELECT location, COUNT(*) as count
                FROM auction_listings 
                GROUP BY location 
                ORDER BY count DESC
            """)
            location_stats = db.cursor.fetchall()
            
            log("Items by location:")
            for location, count in location_stats:
                log(f"   {location}: {count:,} items")
                
            db.cursor.execute("""
                WITH price_changes AS (
                    SELECT 
                        link, current_bid,
                        LAG(current_bid) OVER (PARTITION BY link ORDER BY scraped_at) as prev_bid
                    FROM auction_bid_history
                    WHERE scraped_at >= NOW() - INTERVAL '24 hours'
                )
                SELECT COUNT(*) as changed_items
                FROM price_changes 
                WHERE current_bid != prev_bid AND prev_bid IS NOT NULL
            """)
            price_changes_result = db.cursor.fetchone()
            if price_changes_result:
                price_changes = price_changes_result[0]
                log(f"Price changes in last 24h: {price_changes:,} items")
            
        except Exception as e:
            log(f"Error generating final stats: {e}", "ERROR")
        
        try:
            db.close()
        except:
            pass
        
        runtime = datetime.now() - start_time
        log(f"Scraping completed in {runtime}", "SUCCESS")


if __name__ == "__main__":
    asyncio.run(main())
