from utils.time_utils import get_ny_time_millis
import feedparser
import time
import hashlib
from typing import List, Dict, Any, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError


def stable_uid(source: str, url: str, title: str, ts_bucket: int) -> str:
    """Generate stable UID for deduplication"""
    h = hashlib.sha1(f"{source}|{url}|{title}|{ts_bucket}".encode("utf-8")).hexdigest()
    return h


def ts_bucket_sec(epoch_sec: int, bucket_sec: int = 300) -> int:
    """Round timestamp to bucket for stable deduplication"""
    return (epoch_sec // bucket_sec) * bucket_sec


def fetch_rss_feed(url: str, timeout: int = 10) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch RSS feed and return normalized items.
    Returns None on error (fail-open).
    """
    try:
        req = Request(url, headers={'User-Agent': 'trade-news-ingestor/1.0'})
        with urlopen(req, timeout=timeout) as response:
            feed = feedparser.parse(response.read())

        if feed.get('bozo', 0) or not feed.get('entries'):
            return None

        items = []
        now_ms = get_ny_time_millis()

        for entry in feed.entries[:50]:  # Limit to prevent spam
            title = (entry.get('title') or '').strip()
            link = (entry.get('link') or '').strip()

            if not title or not link:
                continue

            # Parse published time
            published_ts = now_ms // 1000  # fallback to now
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                published_ts = int(time.mktime(entry.published_parsed))
            elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                published_ts = int(time.mktime(entry.updated_parsed))

            # Bucket for deduplication stability (5 minutes)
            bucket = ts_bucket_sec(published_ts, 300)
            uid = stable_uid(f"rss:{url}", link, title, bucket)

            items.append({
                'uid': uid
                'source': f'rss:{url}'
                'title': title
                'url': link
                'ts_ms': published_ts * 1000
                'symbol': '',  # Will be filled by analyzer
                'asset_class': '',  # Will be filled by analyzer
                'summary': (entry.get('summary') or '')[:500]
                'published_ts_ms': published_ts * 1000
            })

        return items

    except Exception as e:
        # fail-open: log and return None
        print(f"RSS fetch error for {url}: {e}")
        return None


def fetch_rss_sources(urls: List[str]) -> List[Dict[str, Any]]:
    """Fetch from multiple RSS sources"""
    all_items = []

    for url in urls:
        items = fetch_rss_feed(url.strip())
        if items:
            all_items.extend(items)

    return all_items
