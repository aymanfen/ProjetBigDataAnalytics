"""
Streaming Pipeline Monitor
===========================
Monitor the health and performance of the streaming pipeline.

Usage:
    python monitor_streaming.py           # One-time stats
    python monitor_streaming.py --watch   # Continuous monitoring
"""

import os
import time
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from project root
project_root = Path(__file__).parent.parent
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)


def get_mongodb_client():
    """Get MongoDB client."""
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    return MongoClient(uri)


def monitor_streaming_pipeline():
    """Monitor the streaming pipeline."""
    
    # Connect to MongoDB
    client = get_mongodb_client()
    db_name = os.getenv("BRONZE_DATABASE", "bronze_db")
    db = client[db_name]
    
    market_col = db["btc_bronze_stream"]
    reddit_col = db["reddit_bronze_stream"]
    
    print("=" * 70)
    print("STREAMING PIPELINE MONITOR")
    print("=" * 70)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # Market data stats
    print("\n📊 MARKET DATA STREAM (btc_bronze_stream)")
    print("-" * 70)
    
    market_total = market_col.count_documents({})
    print(f"Total records: {market_total:,}")
    
    if market_total > 0:
        # Latest record
        latest = market_col.find_one(sort=[("scraped_at", -1)])
        print(f"\nLatest record:")
        print(f"  Time: {latest.get('datetime')}")
        print(f"  Price: ${latest.get('close'):,.2f}")
        print(f"  Volume: {latest.get('volume'):,}")
        print(f"  Scraped: {latest.get('scraped_at')}")
        
        # Recent activity
        intervals = [5, 15, 60]
        print(f"\nRecent activity:")
        for mins in intervals:
            cutoff = (datetime.now() - timedelta(minutes=mins)).isoformat()
            count = market_col.count_documents({"scraped_at": {"$gte": cutoff}})
            print(f"  Last {mins:>3} min: {count:>4} records")
        
        # Price stats (last hour)
        one_hour_ago = (datetime.now() - timedelta(hours=1)).isoformat()
        recent_prices = list(market_col.find(
            {"scraped_at": {"$gte": one_hour_ago}},
            {"close": 1}
        ).sort("scraped_at", 1))
        
        if len(recent_prices) > 1:
            prices = [p['close'] for p in recent_prices]
            price_change = prices[-1] - prices[0]
            price_change_pct = (price_change / prices[0]) * 100
            
            print(f"\nPrice movement (last hour):")
            print(f"  Start: ${prices[0]:,.2f}")
            print(f"  End: ${prices[-1]:,.2f}")
            print(f"  Change: ${price_change:+,.2f} ({price_change_pct:+.2f}%)")
            print(f"  High: ${max(prices):,.2f}")
            print(f"  Low: ${min(prices):,.2f}")
    
    # Reddit data stats
    print("\n\n💬 REDDIT DATA STREAM (reddit_bronze_stream)")
    print("-" * 70)
    
    reddit_total = reddit_col.count_documents({})
    print(f"Total posts: {reddit_total:,}")
    
    if reddit_total > 0:
        # Latest post
        latest = reddit_col.find_one(sort=[("scraped_at", -1)])
        print(f"\nLatest post:")
        print(f"  Subreddit: r/{latest.get('subreddit')}")
        print(f"  Title: {latest.get('title', '')[:60]}...")
        print(f"  Score: {latest.get('score')}")
        print(f"  Scraped: {latest.get('scraped_at')}")
        
        # Recent activity
        intervals = [5, 15, 60]
        print(f"\nRecent activity:")
        for mins in intervals:
            cutoff = (datetime.now() - timedelta(minutes=mins)).isoformat()
            count = reddit_col.count_documents({"scraped_at": {"$gte": cutoff}})
            print(f"  Last {mins:>3} min: {count:>4} posts")
        
        # Top subreddits (last hour)
        one_hour_ago = (datetime.now() - timedelta(hours=1)).isoformat()
        pipeline = [
            {"$match": {"scraped_at": {"$gte": one_hour_ago}}},
            {"$group": {"_id": "$subreddit", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        
        print(f"\nTop subreddits (last hour):")
        for result in reddit_col.aggregate(pipeline):
            print(f"  r/{result['_id']:<20} {result['count']:>3} posts")
    
    # Pipeline health
    print("\n\n🏥 PIPELINE HEALTH")
    print("-" * 70)
    
    # Check data freshness
    if market_total > 0:
        latest_market = market_col.find_one(sort=[("scraped_at", -1)])
        market_age = (datetime.now() - datetime.fromisoformat(latest_market['scraped_at'])).seconds / 60
        market_status = "✓ OK" if market_age < 10 else "⚠ STALE"
        print(f"Market data: {market_status} (last update {market_age:.1f} min ago)")
    else:
        print(f"Market data: ⚠ NO DATA")
    
    if reddit_total > 0:
        latest_reddit = reddit_col.find_one(sort=[("scraped_at", -1)])
        reddit_age = (datetime.now() - datetime.fromisoformat(latest_reddit['scraped_at'])).seconds / 60
        reddit_status = "✓ OK" if reddit_age < 10 else "⚠ STALE"
        print(f"Reddit data: {reddit_status} (last update {reddit_age:.1f} min ago)")
    else:
        print(f"Reddit data: ⚠ NO DATA")
    
    print("\n" + "=" * 70)
    client.close()


def main():
    """Main function."""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--watch":
        print("Monitoring mode (updates every 30 seconds)")
        print("Press Ctrl+C to stop\n")
        try:
            while True:
                monitor_streaming_pipeline()
                time.sleep(30)
                print("\n" + "🔄 Refreshing...\n")
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped.")
    else:
        monitor_streaming_pipeline()


if __name__ == "__main__":
    main()

