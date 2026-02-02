"""
BTC Kafka Consumer - MongoDB Writer
====================================
Consumes messages from Kafka topics and writes to MongoDB.

Usage:
    python btc_kafka_consumer.py
    python btc_kafka_consumer.py --kafka-brokers localhost:9092 --consumer-group btc-group-1
"""

import os
import json
import argparse
import signal
from datetime import datetime
from typing import Dict, List, Any
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from project root
project_root = Path(__file__).parent.parent
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)


class MongoDBWriter:
    """MongoDB writer for streaming data."""
    
    def __init__(
        self,
        market_collection: str = "btc_bronze_stream",
        reddit_collection: str = "reddit_bronze_stream",
        database_name: str = None,
        connection_string: str = None
    ):
        """
        Initialize MongoDB writer.
        
        Args:
            market_collection: Collection name for market data
            reddit_collection: Collection name for Reddit data
            database_name: MongoDB database name
            connection_string: MongoDB connection string
        """
        if connection_string is None:
            connection_string = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
        
        if database_name is None:
            database_name = os.getenv("BRONZE_DATABASE", "bronze_db")
        
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        self.market_collection = self.db[market_collection]
        self.reddit_collection = self.db[reddit_collection]
        
        # Create indexes for better performance
        self._create_indexes()
        
        print(f"✓ Connected to MongoDB: {database_name}")
        print(f"  Market collection: {market_collection}")
        print(f"  Reddit collection: {reddit_collection}")
    
    def _create_indexes(self):
        """Create indexes for collections."""
        try:
            # Market data indexes
            self.market_collection.create_index("datetime", unique=True)
            self.market_collection.create_index("scraped_at")
            self.market_collection.create_index("timestamp")
            
            # Reddit data indexes
            self.reddit_collection.create_index("id", unique=True)
            self.reddit_collection.create_index("created_utc")
            self.reddit_collection.create_index("subreddit")
            self.reddit_collection.create_index("scraped_at")
            
            print("✓ Database indexes created/verified")
        except Exception as e:
            print(f"⚠ Warning creating indexes: {e}")
    
    def insert_market_data(self, record: Dict) -> bool:
        """
        Insert market data record.
        
        Args:
            record: Market data dictionary
            
        Returns:
            True if inserted, False if duplicate or error
        """
        try:
            self.market_collection.insert_one(record)
            return True
        except DuplicateKeyError:
            # Skip duplicates silently
            return False
        except PyMongoError as e:
            print(f"✗ Error inserting market data: {e}")
            return False
    
    def insert_reddit_post(self, post: Dict) -> bool:
        """
        Insert Reddit post.
        
        Args:
            post: Reddit post dictionary
            
        Returns:
            True if inserted, False if duplicate or error
        """
        try:
            self.reddit_collection.insert_one(post)
            return True
        except DuplicateKeyError:
            # Skip duplicates silently
            return False
        except PyMongoError as e:
            print(f"✗ Error inserting Reddit post: {e}")
            return False
    
    def close(self):
        """Close MongoDB connection."""
        self.client.close()
        print("✓ MongoDB connection closed")


class BTCKafkaConsumer:
    """Kafka consumer that writes to MongoDB."""
    
    MARKET_TOPIC = "btc-market-data"
    REDDIT_TOPIC = "reddit-posts"
    
    def __init__(
        self,
        kafka_bootstrap_servers: str = "localhost:9092",
        consumer_group: str = "btc-mongodb-writers",
        market_collection: str = "btc_bronze_stream",
        reddit_collection: str = "reddit_bronze_stream"
    ):
        """
        Initialize Kafka consumer.
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            consumer_group: Consumer group ID
            market_collection: MongoDB collection for market data
            reddit_collection: MongoDB collection for Reddit data
        """
        self.running = True
        self.stats = {
            'market_consumed': 0,
            'market_inserted': 0,
            'reddit_consumed': 0,
            'reddit_inserted': 0,
            'errors': 0
        }
        
        # Initialize MongoDB writer
        self.mongo_writer = MongoDBWriter(
            market_collection=market_collection,
            reddit_collection=reddit_collection
        )
        
        # Initialize Kafka consumer
        print(f"Connecting to Kafka at {kafka_bootstrap_servers}...")
        self.consumer = KafkaConsumer(
            self.MARKET_TOPIC,
            self.REDDIT_TOPIC,
            bootstrap_servers=kafka_bootstrap_servers.split(','),
            group_id=consumer_group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',  # Start from beginning if no offset
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            max_poll_records=100
        )
        print("✓ Connected to Kafka")
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print("\n\n" + "=" * 60)
        print("Shutdown signal received. Stopping consumer...")
        print("=" * 60)
        self.running = False
    
    def process_message(self, message):
        """
        Process a single Kafka message.
        
        Args:
            message: Kafka message
        """
        try:
            topic = message.topic
            data = message.value
            
            if topic == self.MARKET_TOPIC:
                self.stats['market_consumed'] += 1
                if self.mongo_writer.insert_market_data(data):
                    self.stats['market_inserted'] += 1
                    print(f"✓ Market: {data.get('datetime')} | ${data.get('close'):,.2f}")
                    
            elif topic == self.REDDIT_TOPIC:
                self.stats['reddit_consumed'] += 1
                if self.mongo_writer.insert_reddit_post(data):
                    self.stats['reddit_inserted'] += 1
                    subreddit = data.get('subreddit', 'unknown')
                    title = data.get('title', '')[:50]
                    print(f"✓ Reddit: r/{subreddit} | {title}...")
            
        except Exception as e:
            self.stats['errors'] += 1
            print(f"✗ Error processing message: {e}")
    
    def print_stats(self):
        """Print consumer statistics."""
        print("\n" + "-" * 60)
        print("STATISTICS:")
        print(f"  Market data: {self.stats['market_inserted']}/{self.stats['market_consumed']} inserted")
        print(f"  Reddit posts: {self.stats['reddit_inserted']}/{self.stats['reddit_consumed']} inserted")
        print(f"  Errors: {self.stats['errors']}")
        print("-" * 60)
    
    def run(self):
        """Run the consumer."""
        print("=" * 60)
        print("BTC KAFKA CONSUMER - MongoDB Writer")
        print("=" * 60)
        print(f"Topics: {self.MARKET_TOPIC}, {self.REDDIT_TOPIC}")
        print(f"Collections: {self.mongo_writer.market_collection.name}, {self.mongo_writer.reddit_collection.name}")
        print("=" * 60)
        print("\nListening for messages... (Press Ctrl+C to stop)\n")
        
        try:
            last_stats_time = datetime.now()
            
            for message in self.consumer:
                if not self.running:
                    break
                
                self.process_message(message)
                
                # Print stats every 60 seconds
                if (datetime.now() - last_stats_time).seconds >= 60:
                    self.print_stats()
                    last_stats_time = datetime.now()
        
        except KeyboardInterrupt:
            pass
        
        finally:
            print("\n" + "=" * 60)
            print("SHUTTING DOWN")
            print("=" * 60)
            
            self.print_stats()
            
            print("\nClosing connections...")
            self.consumer.close()
            self.mongo_writer.close()
            
            print("\n✓ Consumer stopped")
            print("=" * 60)


def main():
    """Main function to run the Kafka consumer."""
    parser = argparse.ArgumentParser(
        description="Kafka consumer for BTC streaming data"
    )
    parser.add_argument(
        "--kafka-brokers",
        type=str,
        default="localhost:9092",
        help="Kafka broker addresses (comma-separated)"
    )
    parser.add_argument(
        "--consumer-group",
        type=str,
        default="btc-mongodb-writers",
        help="Kafka consumer group ID"
    )
    parser.add_argument(
        "--market-collection",
        type=str,
        default="btc_bronze_stream",
        help="MongoDB collection for market data"
    )
    parser.add_argument(
        "--reddit-collection",
        type=str,
        default="reddit_bronze_stream",
        help="MongoDB collection for Reddit data"
    )
    
    args = parser.parse_args()
    
    # Create and run consumer
    consumer = BTCKafkaConsumer(
        kafka_bootstrap_servers=args.kafka_brokers,
        consumer_group=args.consumer_group,
        market_collection=args.market_collection,
        reddit_collection=args.reddit_collection
    )
    
    consumer.run()


if __name__ == "__main__":
    main()

