import asyncio
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, unquote

import psycopg
from psycopg.rows import dict_row
import requests
from bs4 import BeautifulSoup

from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig


# ---------------------------
# Static config
# ---------------------------

LOCATION_MAP: Dict[str, int] = {
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
    "timeout": 30,
    "request_delay": (1.5, 3.5),
    "page_delay": (6, 12),
    "max_pages_per_location": 40,
    "max_items_per_location": 400,
    "min_items_per_page_stop": 3,
    "max_retries": 3,
}

SHOPPING_LOCATION_ENDPOINT = "https://www.nellisauction.com/api/shopping-location/set"
SHOPPING_LOCATION_COOKIE_NAME = "__shopping-location"


# ---------------------------
# Logging
# ---------------------------

def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level}] {msg}")


# ---------------------------
# DB config helpers
# ---------------------------

def _db_config_from_env() -> Dict[str, str]:
    """
    Supports either:
      - DATABASE_URL (preferred, from Render Postgres)
      - DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
    """
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        u = urlparse(database_url)
        return {
            "host": u.hostname or "",
            "port": str(u.port or 5432),
            "dbname": (u.path or "").lstrip("/"),
            "user": unquote(u.username or ""),
            "password": unquote(u.password or ""),
            "sslmode": os.getenv("DB_SSLMODE", "require"),
        }

    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "dbname": os.getenv("DB_NAME", "Nellis"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
        "sslmode": os.getenv("DB_SSLMODE", "require"),
    }


@dataclass
class AuctionItem:
    location_id: int
    location_name: str
    item_id: str
    title: str
    retail_price: Optional[float]
    current_price: Optional[float]
    url: str
    image_url: Optional[str]


class DatabaseManager:
    def __init__(self) -> None:
        self.db_cfg = _db_config_from_env()
        self.conn = psycopg.connect(**self.db_cfg, row_factory=dict_row)
        self.conn.autocommit = False
        log(f"Connected DB host={self.db_cfg.get('host')} db={self.db_cfg.get('dbname')}", "SUCCESS")
        self._init_schema()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def _init_schema(self) -> None:
        """
        Drop and recreate auction_items to guarantee correct schema.
        WARNING: This will delete any existing data in auction_items.
        """
        ddl = """
        DROP TABLE IF EXISTS auction_items;

        CREATE TABLE auction_items (
            id BIGSERIAL PRIMARY KEY,
            location_id INTEGER NOT NULL,
            location_name VARCHAR(50) NOT NULL,
            item_id VARCHAR(120) NOT NULL,
            title TEXT,
            retail_price NUMERIC(12,2),
            current_price NUMERIC(12,2),
            url TEXT,
            image_url TEXT,
            scraped_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(location_id, item_id)
        );

        CREATE INDEX IF NOT EXISTS idx_auction_items_location_scraped
            ON auction_items (location_id, scraped_at DESC);
        """
        with self.conn.cursor() as cur:
            cur.execute(ddl)
        self.conn.commit()
        log("DB schema dropped & recreated (auction_items)", "SUCCESS")

    def upsert_items(self, items: List[AuctionItem]) -> None:
        if not items:
            return

        sql = """
        INSERT INTO auction_items
            (location_id, location_name, item_id, title, retail_price, current_price, url, image_url)
        VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (location_id, item_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            retail_price = EXCLUDED.retail_price,
            current_price = EXCLUDED.current_price,
            url = EXCLUDED.url,
            image_url = EXCLUDED.image_url,
            scraped_at = CURRENT_TIMESTAMP;
        """

        rows: List[Tuple] = [
            (
                it.location_id,
                it.location_name,
                it.item_id,
                it.title,
                it.retail_price,
                it.current_price,
                it.url,
                it.image_url,
            )
            for it in items
        ]

        with self.conn.cursor() as cur:
            cur.executemany(sql, rows)
        self.conn.commit()
        log(f"Upserted {len(items)} items", "SUCCESS")


# ---------------------------
# Location cookie
# ---------------------------

def fetch_location_cookie(location_id: int, location_name: str) -> Optional[str]:
    """
    Calls the location endpoint and returns the __shopping-location cookie.
    """
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/html,*/*",
            "Content-Type": "application/json",
            "Referer": "https://www.nellisauction.com/",
            "Origin": "https://www.nellisauction.com",
        }
    )

    payloads = [
        {"shoppingLocationId": str(location_id), "referrer": "/search?sortBy=retail_price_desc&query="},
        {"shoppingLocationId": location_id},
    ]

    for payload in payloads:
        for attempt in range(1, CONFIG["max_retries"] + 1):
            try:
                r = session.post(
                    SHOPPING_LOCATION_ENDPOINT,
                    json=payload,
                    timeout=CONFIG["timeout"],
                )
                if not r.ok:
                    log(f"{location_name}: location POST failed (status={r.status_code})", "WARNING")
                cookie_val = session.cookies.get(SHOPPING_LOCATION_COOKIE_NAME)
                if cookie_val:
                    log(f"{location_name}: got location cookie", "SUCCESS")
                    return cookie_val
            except Exception as e:
                log(f"{location_name}: cookie error attempt {attempt}: {e}", "WARNING")

            time.sleep(random.uniform(*CONFIG["request_delay"]))

    log(f"{location_name}: failed to get location cookie", "ERROR")
    return None


# ---------------------------
# HTML parsing
# ---------------------------

_price_re = re.compile(r"\$?\s*([\d,]+(?:\.\d+)?)")


def _parse_money(text: str) -> Optional[float]:
    if not text:
        return None
    m = _price_re.search(text)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except Exception:
        return None


def parse_items_from_html(html: str, location_name: str, location_id: int) -> List[AuctionItem]:
    soup = BeautifulSoup(html, "html.parser")

    candidates = (
        soup.select("[data-testid='product']")
        or soup.select(".product-item")
        or soup.select(".search-result-item")
        or soup.select(".auction-item")
        or soup.select(".product-card")
    )

    items: List[AuctionItem] = []

    for card in candidates:
        try:
            # URL
            a = card.select_one("a[href]")
            href = a.get("href") if a else None
            url = urljoin("https://www.nellisauction.com", href) if href else ""

            # Item id
            item_id = (
                card.get("data-id")
                or card.get("data-item-id")
                or card.get("id")
                or ""
            )
            if not item_id and url:
                m = re.search(r"/(\d+)(?:\D|$)", url)
                if m:
                    item_id = m.group(1)

            if not item_id:
                continue

            # Title
            title_el = (
                card.select_one("h3")
                or card.select_one("[class*='title']")
                or card.select_one("[class*='name']")
            )
            title = title_el.get_text(strip=True) if title_el else ""
            if not title:
                title = " ".join(list(card.stripped_strings)[:12])[:200]
            if not title:
                continue

            text_blob = " ".join(card.stripped_strings).lower()
            retail_price = None
            current_price = None

            for line in (s.strip() for s in text_blob.split("\n")):
                low = line.lower()
                if retail_price is None and ("retail" in low or "msrp" in low):
                    retail_price = _parse_money(line)
                if current_price is None and ("current" in low or "bid" in low or "now" in low):
                    current_price = _parse_money(line)

            img = card.select_one("img")
            image_url = None
            if img:
                image_url = img.get("src") or img.get("data-src")

            items.append(
                AuctionItem(
                    location_id=location_id,
                    location_name=location_name,
                    item_id=str(item_id),
                    title=title,
                    retail_price=retail_price,
                    current_price=current_price,
                    url=url,
                    image_url=image_url,
                )
            )
        except Exception:
            continue

    return items


# ---------------------------
# Crawl loop
# ---------------------------

async def scrape_location(
    crawler: AsyncWebCrawler,
    db: DatabaseManager,
    location_name: str,
    location_id: int,
    cookie_val: str,
) -> None:
    browser_cfg = BrowserConfig(headless=True, java_script_enabled=True)

    total = 0
    for page in range(1, CONFIG["max_pages_per_location"] + 1):
        run_cfg = CrawlerRunConfig(
            browser_config=browser_cfg,
            cookies={SHOPPING_LOCATION_COOKIE_NAME: cookie_val},
            wait_for="body",
            timeout=CONFIG["timeout"],
        )

        url = BASE_SEARCH_URL.format(page=page)
        log(f"{location_name}: crawling page {page} -> {url}")

        try:
            result = await crawler.arun(url=url, config=run_cfg)
        except Exception as e:
            log(f"{location_name}: crawl error page {page}: {e}", "ERROR")
            break

        if not getattr(result, "success", False) or not getattr(result, "html", ""):
            log(f"{location_name}: empty/failed result on page {page}", "WARNING")
            break

        items = parse_items_from_html(result.html, location_name, location_id)
        if len(items) < CONFIG["min_items_per_page_stop"]:
            log(f"{location_name}: too few items ({len(items)}). stopping.", "INFO")
            break

        db.upsert_items(items)
        total += len(items)
        log(f"{location_name}: page {page} stored {len(items)} (total {total})", "SUCCESS")

        if total >= CONFIG["max_items_per_location"]:
            log(f"{location_name}: reached max_items_per_location={CONFIG['max_items_per_location']}", "INFO")
            break

        await asyncio.sleep(random.uniform(*CONFIG["page_delay"]))

    log(f"{location_name}: done (total stored: {total})", "SUCCESS")


async def main() -> None:
    log("Starting scraper", "INFO")
    db = DatabaseManager()

    async with AsyncWebCrawler() as crawler:
        for location_name, location_id in LOCATION_MAP.items():
            log(f"Preparing location {location_name} (id={location_id})", "INFO")

            cookie_val = fetch_location_cookie(location_id, location_name)
            if not cookie_val:
                continue

            await scrape_location(crawler, db, location_name, location_id, cookie_val)
            await asyncio.sleep(random.uniform(10, 20))

    db.close()
    log("Scraper finished", "SUCCESS")


if __name__ == "__main__":
    asyncio.run(main())
