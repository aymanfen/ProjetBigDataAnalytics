"""
BTC Market Scraper
==================
Scrapes BTC market data for a specified date range and interval.

Usage:
    python btc_scraper_1year_15min.py --start-date 2023-01-01 --end-date 2024-01-01 --interval 15m
"""

import yfinance as yf
import json
import os
import pandas as pd
import time
from datetime import datetime, timedelta
from bson import ObjectId
from mongodb_helper import insert_btc_records


class JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles ObjectId and other non-serializable types."""
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class BTCScraper:
    """Scrape BTC market data for specified date range and interval."""
    
    TICKER = "BTC-USD"
    
    def __init__(self):
        """Initialize the scraper."""
        self.btc = yf.Ticker(self.TICKER)
    
    def get_data(self, start_date: datetime, end_date: datetime, interval: str = "15m"):
        """
        Get BTC data for specified date range and interval.
        
        Args:
            start_date: Start date for data collection
            end_date: End date for data collection
            interval: Data interval (1m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
        
        Returns:
            List of records with OHLCV data
        """
        print(f"Fetching BTC data ({interval} interval)...")
        print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        all_data = []
        
        # Determine chunk size based on interval
        if interval in ["1m", "5m", "15m", "30m", "60m", "90m"]:
            chunk_days = 60
            print("Note: Making multiple requests due to API limitations...")
        else:
            chunk_days = 365
        
        current_start = start_date
        chunk_num = 1
        
        while current_start < end_date:
            current_end = min(current_start + timedelta(days=chunk_days), end_date)
            
            print(f"\n  Chunk {chunk_num}: {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")
            
            try:
                df = self.btc.history(
                    start=current_start.strftime('%Y-%m-%d'),
                    end=current_end.strftime('%Y-%m-%d'),
                    interval=interval
                )
                
                if not df.empty:
                    records = self._df_to_records(df)
                    all_data.extend(records)
                    print(f"    ✓ Collected {len(records)} records")
                else:
                    print(f"    ✗ No data for this period")
                
            except Exception as e:
                print(f"    ✗ Error: {e}")
            
            current_start = current_end
            chunk_num += 1
            time.sleep(1)
        
        # Remove duplicates and sort by datetime
        seen = set()
        unique_data = []
        for record in all_data:
            dt_key = record.get("datetime")
            if dt_key and dt_key not in seen:
                seen.add(dt_key)
                unique_data.append(record)
        
        unique_data.sort(key=lambda x: x.get("datetime", ""))
        
        print(f"\n✓ Total records collected: {len(unique_data)}")
        
        return unique_data
    
    def _df_to_records(self, df):
        """
        Convert DataFrame to list of dictionaries.
        
        Args:
            df: pandas DataFrame with OHLCV data
        
        Returns:
            List of record dictionaries
        """
        if df.empty:
            return []
        
        # Reset index to make datetime a column
        if df.index.name is None:
            df.index.name = "Date"
        
        df = df.reset_index()
        
        # Find datetime column
        datetime_col = None
        if "Date" in df.columns:
            datetime_col = "Date"
        else:
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    datetime_col = col
                    break
        
        if datetime_col is None:
            raise ValueError(f"Could not find datetime column. Columns: {df.columns.tolist()}")
        
        # Format datetime
        df[datetime_col] = df[datetime_col].dt.strftime("%Y-%m-%d %H:%M:%S")
        
        # Convert to records
        records = df.to_dict(orient="records")
        
        # Clean up records
        for r in records:
            # Rename datetime column
            if datetime_col in r:
                r["datetime"] = r.pop(datetime_col)
            
            # Rename and round price columns
            if "Open" in r:
                r["open"] = round(r.pop("Open"), 2)
            if "High" in r:
                r["high"] = round(r.pop("High"), 2)
            if "Low" in r:
                r["low"] = round(r.pop("Low"), 2)
            if "Close" in r:
                r["close"] = round(r.pop("Close"), 2)
            if "Volume" in r:
                r["volume"] = int(r.pop("Volume"))
            
            # Remove unused columns
            r.pop("Dividends", None)
            r.pop("Stock Splits", None)
        
        return records
    
    def save_to_json(self, data, filename):
        """
        Save data to JSON file.
        
        Args:
            data: Data to save
            filename: Output filename
        """
        os.makedirs("output", exist_ok=True)
        filepath = os.path.join("output", filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, cls=JSONEncoder)
        
        print(f"✓ Saved to: {filepath}")
        return filepath


def main():
    """Main function to scrape and save BTC data."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Scrape BTC market data")
    parser.add_argument("--start-date", type=str, required=True,
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, required=True,
                        help="End date (YYYY-MM-DD)")
    parser.add_argument("--interval", type=str, default="15m",
                        help="Data interval (1m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)")
    
    args = parser.parse_args()
    
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    
    print("=" * 60)
    print("BTC Market Scraper")
    print("=" * 60)
    print(f"Start date: {start_date.strftime('%Y-%m-%d')}")
    print(f"End date: {end_date.strftime('%Y-%m-%d')}")
    print(f"Interval: {args.interval}")
    print("=" * 60)
    
    scraper = BTCScraper()
    
    history = scraper.get_data(start_date, end_date, args.interval)
    
    if not history:
        print("\n✗ No data collected. Exiting.")
        return
    
    # Insert into MongoDB
    print("\n" + "=" * 60)
    print("Inserting records into MongoDB...")
    print("=" * 60)
    try:
        metadata = {
            "scraped_at": datetime.now().isoformat(),
            "ticker": "BTC-USD",
            "interval": args.interval
        }
        inserted_count = insert_btc_records(
            records=history,
            metadata=metadata,
            collection_name="btc_bronze_batch"
        )
        print(f"  Total inserted: {inserted_count}")
    except Exception as e:
        print(f"  ✗ Error inserting into MongoDB: {e}")
        print("  Continuing with file save...")
    
    # Save with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_data = {
        "scraped_at": datetime.now().isoformat(),
        "ticker": "BTC-USD",
        "interval": args.interval,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "total_records": len(history),
        "history": history
    }
    scraper.save_to_json(output_data, f"btc_market_{timestamp}.json")
    
    # Print summary
    print("\n" + "=" * 60)
    print("Scraping Complete!")
    print("=" * 60)
    print(f"Total records: {len(history)}")
    
    if history:
        first_record = history[0]
        last_record = history[-1]
        print(f"Date range: {first_record.get('datetime')} to {last_record.get('datetime')}")
    
    print("=" * 60)


if __name__ == "__main__":
    main()

