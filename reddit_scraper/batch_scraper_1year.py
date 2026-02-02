"""
Reddit Batch Scraper
====================
Scrapes posts from specified subreddits within a date range with author information.

Usage:
    python batch_scraper_1year.py --start-date 2023-01-01 --end-date 2024-01-01
"""

import json
import os
import time
from datetime import datetime, timedelta
from scraper import RedditBitcoinScraper
from author_enricher import AuthorEnricher
from mongodb_helper import insert_reddit_posts


SUBREDDITS = [
    "trading",
    "Daytrading",
    "investing",
    "StockMarket",
    "wallstreetbets",
    "CryptoCurrency",
    "CryptoMarkets",
    "CryptoTechnology",
    "altcoin",
    "Bitcoin",
    "BitcoinMarkets",
    "btc",
    "finance",
    "economics",
    "market_sentiment",
    "StocksAndTrading",
]

OUTPUT_DIR = "output"


# ============================================================================
# MAIN SCRAPER
# ============================================================================

def scrape_subreddit_date_range(scraper, subreddit, start_date, end_date):
    """
    Scrape posts from a subreddit within the specified date range.
    
    Args:
        scraper: RedditBitcoinScraper instance
        subreddit: Subreddit name
        start_date: Start date (datetime)
        end_date: End date (datetime)
    
    Returns:
        List of posts
    """
    posts = []
    after = None
    max_attempts = 1000
    attempt = 0
    
    print(f"\n  Scraping r/{subreddit}...")
    
    while attempt < max_attempts:
        attempt += 1
        
        try:
            url = f"{scraper.BASE_URL}/r/{subreddit}/new.json"
            params = {"limit": 100}
            if after:
                params["after"] = after
            
            data = scraper._make_request(url, params)
            
            if not data:
                print(f"    No more data available")
                break
            
            children = data.get("data", {}).get("children", [])
            
            if not children:
                print(f"    No more posts found")
                break
            
            found_new = False
            should_stop = False
            
            for child in children:
                post_data = child.get("data", {})
                
                if post_data.get("stickied", False):
                    continue
                
                created_utc = post_data.get("created_utc", 0)
                if not created_utc:
                    continue
                
                post_date = datetime.utcfromtimestamp(created_utc)
                
                # Stop if we've gone past the start date
                if post_date < start_date:
                    print(f"    Reached start date: {post_date.strftime('%Y-%m-%d')}")
                    should_stop = True
                    break
                
                # Only include posts within the date range
                if start_date <= post_date <= end_date:
                    extracted = scraper._extract_post_data(post_data)
                    posts.append(extracted)
                    found_new = True
            
            if should_stop:
                break
            
            after = data.get("data", {}).get("after")
            if not after:
                print(f"    No more pages")
                break
            
            if len(posts) % 100 == 0:
                if posts:
                    latest_date = datetime.utcfromtimestamp(posts[0]["created_utc"])
                    print(f"    Collected {len(posts)} posts (latest: {latest_date.strftime('%Y-%m-%d')})")
            
            time.sleep(2)
            
        except Exception as e:
            print(f"    Error: {e}")
            break
    
    # Final filter to ensure all posts are within date range
    filtered_posts = []
    for post in posts:
        post_date = datetime.utcfromtimestamp(post["created_utc"])
        if start_date <= post_date <= end_date:
            filtered_posts.append(post)
    
    return filtered_posts


def main():
    """Main function to scrape data from all subreddits for specified date range."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Scrape Reddit posts")
    parser.add_argument("--start-date", type=str, required=True,
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, required=True,
                        help="End date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    
    print("=" * 60)
    print("Reddit Batch Scraper")
    print("=" * 60)
    print(f"Subreddits: {len(SUBREDDITS)}")
    print(f"Start date: {start_date.strftime('%Y-%m-%d')}")
    print(f"End date: {end_date.strftime('%Y-%m-%d')}")
    print("=" * 60)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    scraper = RedditBitcoinScraper()
    enricher = AuthorEnricher()
    
    all_posts = []
    results = {}
    
    for i, subreddit in enumerate(SUBREDDITS, 1):
        print(f"\n[{i}/{len(SUBREDDITS)}] Processing r/{subreddit}...")
        
        try:
            posts = scrape_subreddit_date_range(scraper, subreddit, start_date, end_date)
            results[subreddit] = posts
            all_posts.extend(posts)
            
            if posts:
                oldest_date = datetime.utcfromtimestamp(posts[-1]["created_utc"])
                newest_date = datetime.utcfromtimestamp(posts[0]["created_utc"])
                print(f"  ✓ Collected {len(posts)} posts")
                print(f"  Date range: {oldest_date.strftime('%Y-%m-%d')} to {newest_date.strftime('%Y-%m-%d')}")
            else:
                print(f"  ✗ No posts found")
        
        except Exception as e:
            print(f"  ✗ Error: {e}")
            results[subreddit] = []
        
        if i < len(SUBREDDITS):
            wait_time = 5
            print(f"  Waiting {wait_time}s before next subreddit...")
            time.sleep(wait_time)
    
    all_posts.sort(key=lambda x: x.get("created_utc", 0), reverse=True)
    
    print("\n" + "=" * 60)
    print("Enriching posts with author information...")
    print("=" * 60)
    
    enriched_posts = enricher.enrich_posts(all_posts, delay_between_authors=1.5)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("\n" + "=" * 60)
    print("Inserting posts into MongoDB...")
    print("=" * 60)
    try:
        inserted_count = insert_reddit_posts(
            posts=enriched_posts,
            collection_name="reddit_bronze_batch"
        )
        print(f"  Total inserted: {inserted_count}")
    except Exception as e:
        print(f"  ✗ Error inserting into MongoDB: {e}")
        print("  Continuing with file save...")
    
    output_file = os.path.join(OUTPUT_DIR, f"reddit_posts_{timestamp}.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(enriched_posts, f, indent=2, ensure_ascii=False)
    print(f"\n✓ Saved {len(enriched_posts)} posts to: {output_file}")
    
    summary = {
        "scraped_at": datetime.now().isoformat(),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "total_posts": len(enriched_posts),
        "subreddits": {sub: len(posts) for sub, posts in results.items()}
    }
    
    summary_file = os.path.join(OUTPUT_DIR, f"reddit_summary_{timestamp}.json")
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved summary to: {summary_file}")
    
    print("\n" + "=" * 60)
    print("Scraping Complete!")
    print("=" * 60)
    print(f"Total posts: {len(enriched_posts)}")
    print("\nPosts per subreddit:")
    for sub, count in summary["subreddits"].items():
        print(f"  r/{sub}: {count}")
    print("=" * 60)
    
    return enriched_posts


if __name__ == "__main__":
    main()

