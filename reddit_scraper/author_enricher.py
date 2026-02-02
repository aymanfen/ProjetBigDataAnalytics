"""
Author Enricher
===============
Enriches Reddit posts with detailed author information.
Fetches karma, account age, verification status, etc.
"""

import requests
import time
import random
from typing import List, Dict, Any, Set
from datetime import datetime


class AuthorEnricher:
    """Fetch and enrich posts with detailed author information."""
    
    BASE_URL = "https://old.reddit.com"
    
    def __init__(self, user_agent: str = None):
        """
        Initialize the enricher.
        
        Args:
            user_agent: Custom User-Agent string (optional)
        """
        self.session = requests.Session()
        
        if user_agent is None:
            user_agent = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        
        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        })
        
        # Cache for author info to avoid duplicate requests
        self._author_cache: Dict[str, Dict[str, Any]] = {}
    
    def _make_request(self, url: str, max_retries: int = 3) -> dict:
        """
        Make a request with retry logic.
        
        Args:
            url: URL to request
            max_retries: Maximum retry attempts
            
        Returns:
            JSON response data or empty dict on failure
        """
        for attempt in range(max_retries):
            try:
                time.sleep(random.uniform(0.5, 1.5))
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 403:
                    wait_time = (2 ** attempt) * 10
                    print(f" Blocked. Waiting {wait_time}s...", end="", flush=True)
                    time.sleep(wait_time)
                    continue
                
                if response.status_code == 429:
                    wait_time = (2 ** attempt) * 30
                    print(f" Rate limited. Waiting {wait_time}s...", end="", flush=True)
                    time.sleep(wait_time)
                    continue
                
                if response.status_code == 404:
                    return {}
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException:
                if attempt < max_retries - 1:
                    time.sleep((2 ** attempt) * 5)
                else:
                    return {}
            except ValueError:
                return {}
        
        return {}
    
    def fetch_author_info(self, username: str) -> Dict[str, Any]:
        """
        Fetch detailed author information from Reddit.
        
        Args:
            username: Reddit username
            
        Returns:
            Dictionary with author info (karma, account age, etc.)
        """
        # Skip deleted/removed users
        if not username or username in ["[deleted]", "[removed]", "AutoModerator"]:
            return {}
        
        # Check cache first
        if username in self._author_cache:
            return self._author_cache[username]
        
        url = f"{self.BASE_URL}/user/{username}/about.json"
        
        data = self._make_request(url)
        
        if not data or "data" not in data:
            self._author_cache[username] = {}
            return {}
        
        user_data = data.get("data", {})
        created_utc = user_data.get("created_utc", 0)
        
        # Calculate account age in days
        if created_utc:
            account_age_days = (datetime.utcnow() - datetime.utcfromtimestamp(created_utc)).days
        else:
            account_age_days = None
        
        author_info = {
            "author_link_karma": user_data.get("link_karma", 0),
            "author_comment_karma": user_data.get("comment_karma", 0),
            "author_total_karma": user_data.get("total_karma", 0),
            "author_created_utc": created_utc,
            "author_account_age_days": account_age_days,
            "author_verified": user_data.get("verified", False),
            "author_has_verified_email": user_data.get("has_verified_email", False),
            "author_is_gold": user_data.get("is_gold", False),
            "author_is_mod": user_data.get("is_mod", False),
        }
        
        # Cache the result
        self._author_cache[username] = author_info
        
        return author_info
    
    def enrich_posts(
        self,
        posts: List[Dict[str, Any]],
        delay_between_authors: float = 1.5
    ) -> List[Dict[str, Any]]:
        """
        Enrich posts with detailed author information.
        
        Args:
            posts: List of post dictionaries
            delay_between_authors: Delay between author requests
            
        Returns:
            Posts with added author info fields
        """
        # Get unique authors
        authors: Set[str] = set()
        for post in posts:
            author = post.get("author")
            if author and author not in ["[deleted]", "[removed]", "AutoModerator"]:
                authors.add(author)
        
        print(f"\n{'=' * 60}")
        print(f"Fetching author info for {len(authors)} unique authors...")
        print(f"{'=' * 60}")
        
        # Fetch author info for each unique author
        fetched = 0
        cached = 0
        failed = 0
        
        for i, author in enumerate(authors, 1):
            if author in self._author_cache:
                cached += 1
                continue
            
            print(f"  [{i}/{len(authors)}] Fetching u/{author}...", end=" ", flush=True)
            
            info = self.fetch_author_info(author)
            
            if info:
                print(f"Karma: {info.get('author_total_karma', 0)}")
                fetched += 1
            else:
                print("Not found/deleted")
                failed += 1
            
            # Delay between requests
            time.sleep(delay_between_authors)
        
        print(f"\n  Fetched: {fetched} | Cached: {cached} | Failed: {failed}")
        
        # Merge author info into posts
        enriched_posts = []
        for post in posts:
            author = post.get("author", "")
            author_info = self._author_cache.get(author, {})
            enriched_post = {**post, **author_info}
            enriched_posts.append(enriched_post)
        
        print(f"  Enriched {len(enriched_posts)} posts with author data")
        print(f"{'=' * 60}\n")
        
        return enriched_posts
    
    def enrich_from_file(
        self,
        input_file: str,
        output_file: str = None,
        delay_between_authors: float = 1.5
    ) -> List[Dict[str, Any]]:
        """
        Load posts from a JSON file, enrich with author info, and save.
        
        Args:
            input_file: Path to input JSON file with posts
            output_file: Path to output file (default: adds _enriched suffix)
            delay_between_authors: Delay between author requests
            
        Returns:
            List of enriched posts
        """
        import json
        import os
        
        # Load posts
        print(f"Loading posts from: {input_file}")
        with open(input_file, "r", encoding="utf-8") as f:
            posts = json.load(f)
        print(f"Loaded {len(posts)} posts")
        
        # Enrich
        enriched = self.enrich_posts(posts, delay_between_authors)
        
        # Save
        if output_file is None:
            base, ext = os.path.splitext(input_file)
            output_file = f"{base}_enriched{ext}"
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(enriched, f, indent=2, ensure_ascii=False)
        print(f"Saved enriched posts to: {output_file}")
        
        return enriched


# ============================================================================
# STANDALONE USAGE
# ============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python author_enricher.py <input_file.json> [output_file.json]")
        print("\nExample:")
        print("  python author_enricher.py output/bitcoin_posts_latest.json")
        print("  python author_enricher.py output/bitcoin_posts_latest.json output/enriched.json")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    enricher = AuthorEnricher()
    enricher.enrich_from_file(input_file, output_file)

