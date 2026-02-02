"""
BTC Real-Time Kafka Producer
=============================
Scrapes BTC market data and Reddit posts, then publishes to Kafka topics.

Usage:
    python btc_realtime_kafka_producer.py --interval 5
"""

import yfinance as yf
import sys
import os
import time
import json
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any
import signal
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Add reddit_scraper to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'reddit_scraper'))

try:
    from scraper import RedditBitcoinScraper
    from author_enricher import AuthorEnricher
    REDDIT_AVAILABLE = True
except ImportError:
    print("Warning: Reddit scraper not available. Will only scrape market data.")
    REDDIT_AVAILABLE = False


class KafkaJSONSerializer:
    """JSON serializer for Kafka messages."""
    
    @staticmethod
    def serialize(data):
        """Serialize data to JSON bytes."""
        return json.dumps(data, default=str).encode('utf-8')


class RealtimeBTCKafkaProducer:
    """Real-time Bitcoin data scraper that publishes to Kafka."""
    
    TICKER = "BTC-USD"
    
    # Kafka topics
    MARKET_TOPIC = "btc-market-data"
    REDDIT_TOPIC = "reddit-posts"
    
    # Subreddits to monitor
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
    
    def __init__(
        self, 
        interval_minutes: int = 5,
        max_reddit_posts: int = 20,
        kafka_bootstrap_servers: str = "localhost:9092",
        enrich_authors: bool = True
    ):
        """
        Initialize the Kafka producer.
        
        Args:
            interval_minutes: Minutes between each scrape cycle
            max_reddit_posts: Maximum Reddit posts to fetch per subreddit
            kafka_bootstrap_servers: Kafka broker addresses
            enrich_authors: Whether to fetch author karma info (slower but more data)
        """
        self.interval_minutes = interval_minutes
        self.max_reddit_posts = max_reddit_posts
        self.enrich_authors = enrich_authors
        self.btc_ticker = yf.Ticker(self.TICKER)
        self.reddit_scraper = RedditBitcoinScraper() if REDDIT_AVAILABLE else None
        self.author_enricher = AuthorEnricher() if REDDIT_AVAILABLE and enrich_authors else None
        self.running = True
        self.cycle_count = 0
        
        # Initialize Kafka producer
        print(f"Connecting to Kafka at {kafka_bootstrap_servers}...")
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers.split(','),
            value_serializer=KafkaJSONSerializer.serialize,
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,
            max_in_flight_requests_per_connection=1,
            compression_type='gzip'
        )
        print("✓ Connected to Kafka")
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print("\n\n" + "=" * 60)
        print("Shutdown signal received. Stopping scraper...")
        print("=" * 60)
        self.running = False
    
    def scrape_market_data(self) -> Dict[str, Any]:
        """
        Scrape latest BTC market data from yfinance.
        
        Returns:
            Dictionary with latest market data or None on error
        """
        try:
            print("\n  Fetching latest BTC market data...")
            
            # Get recent data (last 2 days at 1-minute interval)
            df = self.btc_ticker.history(period="2d", interval="1m")
            
            if df.empty:
                print("    ✗ No market data available")
                return None
            
            # Get the most recent record
            latest = df.iloc[-1]
            timestamp = df.index[-1]
            
            record = {
                "datetime": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "timestamp": timestamp.isoformat(),
                "open": round(float(latest["Open"]), 2),
                "high": round(float(latest["High"]), 2),
                "low": round(float(latest["Low"]), 2),
                "close": round(float(latest["Close"]), 2),
                "volume": int(latest["Volume"]),
                "scraped_at": datetime.now().isoformat(),
                "data_source": "yfinance_realtime",
                "ticker": self.TICKER,
                "cycle": self.cycle_count,
                "interval_minutes": self.interval_minutes
            }
            
            print(f"    ✓ Latest: {record['datetime']} | Close: ${record['close']:,.2f} | Vol: {record['volume']:,}")
            return record
            
        except Exception as e:
            print(f"    ✗ Error scraping market data: {e}")
            return None
    
    def scrape_reddit_posts(self) -> List[Dict]:
        """
        Scrape latest Reddit posts from configured subreddits.
        
        Returns:
            List of posts
        """
        if not REDDIT_AVAILABLE or not self.reddit_scraper:
            print("    ⚠ Reddit scraping not available")
            return []
        
        try:
            print("\n  Fetching latest Reddit posts...")
            all_posts = []
            
            for i, subreddit in enumerate(self.SUBREDDITS, 1):
                try:
                    # Determine if Bitcoin-only subreddit
                    is_bitcoin_sub = subreddit in self.reddit_scraper.BITCOIN_ONLY_SUBREDDITS
                    
                    if is_bitcoin_sub:
                        posts = self.reddit_scraper.get_new_posts(
                            subreddit=subreddit,
                            max_posts=self.max_reddit_posts
                        )
                    else:
                        posts = self.reddit_scraper.search_posts(
                            subreddit=subreddit,
                            query=self.reddit_scraper.BITCOIN_SEARCH_QUERY,
                            max_posts=self.max_reddit_posts,
                            sort="new"
                        )
                    
                    # Add metadata to posts (no time filter - MongoDB will dedupe)
                    for post in posts:
                        post['cycle'] = self.cycle_count
                        post['interval_minutes'] = self.interval_minutes
                        post['scraped_at'] = datetime.now().isoformat()
                    
                    all_posts.extend(posts)
                    
                    if posts:
                        print(f"    [{i}/{len(self.SUBREDDITS)}] r/{subreddit}: {len(posts)} posts")
                    
                    # Small delay between subreddits
                    if i < len(self.SUBREDDITS):
                        time.sleep(2)
                        
                except Exception as e:
                    print(f"    ✗ Error scraping r/{subreddit}: {e}")
            
            print(f"    ✓ Total posts collected: {len(all_posts)}")
            
            # Enrich posts with author karma info
            if all_posts and self.enrich_authors and self.author_enricher:
                print("\n  Enriching posts with author info...")
                try:
                    all_posts = self.author_enricher.enrich_posts(
                        posts=all_posts,
                        delay_between_authors=1.0  # 1 second delay to avoid rate limits
                    )
                    print(f"    ✓ Author enrichment complete")
                except Exception as e:
                    print(f"    ⚠ Author enrichment failed: {e}")
                    print(f"    Continuing without author karma info...")
            
            return all_posts
            
        except Exception as e:
            print(f"    ✗ Error during Reddit scraping: {e}")
            return []
    
    def publish_to_kafka(self, market_data: Dict, reddit_posts: List[Dict]):
        """
        Publish scraped data to Kafka topics.
        
        Args:
            market_data: BTC market data dictionary
            reddit_posts: List of Reddit posts
        """
        print("\n  Publishing to Kafka...")
        
        # Publish market data
        if market_data:
            try:
                future = self.producer.send(self.MARKET_TOPIC, market_data)
                record_metadata = future.get(timeout=10)
                print(f"    ✓ Market data published to topic '{self.MARKET_TOPIC}'")
                print(f"      Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            except KafkaError as e:
                print(f"    ✗ Error publishing market data: {e}")
        
        # Publish Reddit posts
        if reddit_posts:
            try:
                published_count = 0
                failed_count = 0
                
                for post in reddit_posts:
                    try:
                        future = self.producer.send(self.REDDIT_TOPIC, post)
                        future.get(timeout=10)
                        published_count += 1
                    except KafkaError as e:
                        print(f"    ✗ Error publishing post: {e}")
                        failed_count += 1
                
                print(f"    ✓ Published {published_count} Reddit posts to topic '{self.REDDIT_TOPIC}'")
                if failed_count > 0:
                    print(f"    ⚠ Failed to publish {failed_count} posts")
                    
            except Exception as e:
                print(f"    ✗ Error publishing Reddit posts: {e}")
        
        # Flush to ensure all messages are sent
        self.producer.flush()
    
    def run_single_cycle(self):
        """Run a single scraping cycle."""
        self.cycle_count += 1
        
        print("\n" + "=" * 60)
        print(f"SCRAPING CYCLE #{self.cycle_count}")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        # Scrape market data
        market_data = self.scrape_market_data()
        
        # Scrape Reddit data
        reddit_posts = self.scrape_reddit_posts()
        
        # Publish to Kafka
        self.publish_to_kafka(market_data, reddit_posts)
        
        print("\n" + "=" * 60)
        print(f"CYCLE #{self.cycle_count} COMPLETE")
        print("=" * 60)
    
    def run(self):
        """Run the scraper continuously."""
        print("=" * 60)
        print("BTC REAL-TIME KAFKA PRODUCER")
        print("=" * 60)
        print(f"Interval: {self.interval_minutes} minutes")
        print(f"Reddit posts per subreddit: {self.max_reddit_posts}")
        print(f"Subreddits: {len(self.SUBREDDITS)}")
        print(f"Author enrichment: {'✓ Enabled (includes karma)' if self.enrich_authors else '✗ Disabled'}")
        print(f"Kafka Topics: {self.MARKET_TOPIC}, {self.REDDIT_TOPIC}")
        print("=" * 60)
        print("\nPress Ctrl+C to stop\n")
        
        try:
            while self.running:
                try:
                    # Run scraping cycle
                    self.run_single_cycle()
                    
                    if not self.running:
                        break
                    
                    # Wait for next cycle
                    next_run = datetime.now() + timedelta(minutes=self.interval_minutes)
                    print(f"\n⏱️  Next scrape at: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"   Waiting {self.interval_minutes} minutes...")
                    
                    # Sleep in smaller chunks for responsive shutdown
                    sleep_seconds = self.interval_minutes * 60
                    for _ in range(sleep_seconds):
                        if not self.running:
                            break
                        time.sleep(1)
                        
                except Exception as e:
                    print(f"\n✗ Error during cycle: {e}")
                    print("  Continuing to next cycle...")
                    time.sleep(60)
        
        finally:
            print("\n" + "=" * 60)
            print("CLOSING KAFKA PRODUCER")
            print("=" * 60)
            self.producer.close()
            print(f"Total cycles completed: {self.cycle_count}")
            print("=" * 60)


def main():
    """Main function to run the Kafka producer."""
    parser = argparse.ArgumentParser(
        description="Real-time BTC Kafka producer"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Scraping interval in minutes (default: 5)"
    )
    parser.add_argument(
        "--max-reddit-posts",
        type=int,
        default=20,
        help="Maximum Reddit posts per subreddit (default: 20)"
    )
    parser.add_argument(
        "--kafka-brokers",
        type=str,
        default="localhost:9092",
        help="Kafka broker addresses (comma-separated, default: localhost:9092)"
    )
    parser.add_argument(
        "--enrich-authors",
        action="store_true",
        default=True,
        help="Enrich posts with author karma info (default: True)"
    )
    parser.add_argument(
        "--no-enrich-authors",
        action="store_true",
        help="Disable author enrichment (faster scraping)"
    )
    
    args = parser.parse_args()
    
    # Handle enrich authors flag
    enrich_authors = args.enrich_authors and not args.no_enrich_authors
    
    # Validate interval
    if args.interval < 1:
        print("Error: Interval must be at least 1 minute")
        return
    
    if args.interval < 5:
        print("Warning: Short intervals may trigger rate limits. Recommended: 5+ minutes")
    
    # Create and run producer
    producer = RealtimeBTCKafkaProducer(
        interval_minutes=args.interval,
        max_reddit_posts=args.max_reddit_posts,
        kafka_bootstrap_servers=args.kafka_brokers,
        enrich_authors=enrich_authors
    )
    
    producer.run()


if __name__ == "__main__":
    main()

