import yfinance as yf
import json
import os
import pandas as pd
from datetime import datetime, timedelta


class BTCMarketScraper:
    
    TICKER = "BTC-USD"
    
    def __init__(self):
        self.btc = yf.Ticker(self.TICKER)
    
    def get_history(self, period="60d", interval="1h"):
        """Get historical OHLCV data. Period: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, max
        Interval: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo"""
        df = self.btc.history(period=period, interval=interval)
        return self._df_to_records(df)
    
    def get_range(self, start_date, end_date, interval="1h"):
        """Get OHLCV data for a specific date range. Dates as 'YYYY-MM-DD'"""
        df = self.btc.history(start=start_date, end=end_date, interval=interval)
        return self._df_to_records(df)
    
    def get_info(self):
        """Get current market info"""
        info = self.btc.info
        return {
            "symbol": info.get("symbol"),
            "price": info.get("regularMarketPrice"),
            "previous_close": info.get("previousClose"),
            "open": info.get("open"),
            "day_high": info.get("dayHigh"),
            "day_low": info.get("dayLow"),
            "volume": info.get("volume"),
            "market_cap": info.get("marketCap"),
            "52_week_high": info.get("fiftyTwoWeekHigh"),
            "52_week_low": info.get("fiftyTwoWeekLow"),
        }
    
    def _df_to_records(self, df):
        """Convert DataFrame to list of dicts"""
        if df.empty:
            return []
        
        # Ensure the index has a name before resetting
        if df.index.name is None:
            df.index.name = "Date"
        
        df = df.reset_index()
        
        # Find the datetime column (should be the first column or named "Date")
        datetime_col = None
        if "Date" in df.columns:
            datetime_col = "Date"
        else:
            # Find first datetime column
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    datetime_col = col
                    break
        
        if datetime_col is None:
            raise ValueError(f"Could not find datetime column. Available columns: {df.columns.tolist()}")
        
        # Format datetime to include both date and time
        df[datetime_col] = df[datetime_col].dt.strftime("%Y-%m-%d %H:%M:%S")
        records = df.to_dict(orient="records")
        
        for r in records:
            # Rename datetime column to "datetime"
            if datetime_col in r:
                r["datetime"] = r.pop(datetime_col)
            r["open"] = round(r.pop("Open"), 2)
            r["high"] = round(r.pop("High"), 2)
            r["low"] = round(r.pop("Low"), 2)
            r["close"] = round(r.pop("Close"), 2)
            r["volume"] = int(r.pop("Volume"))
            r.pop("Dividends", None)
            r.pop("Stock Splits", None)
        return records
    
    def save_to_json(self, data, filename):
        """Save data to JSON file"""
        os.makedirs("output", exist_ok=True)
        filepath = os.path.join("output", filename)
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Saved to: {filepath}")
        return filepath


def main():
    scraper = BTCMarketScraper()
    
    print("=" * 50)
    print("BTC Market Scraper (Hourly Data)")
    print("=" * 50)
    
    # Get last 60 days of hourly data (adjust period as needed)
    # Note: yfinance limits hourly data to ~60 days max
    print("\nFetching 60 days of hourly BTC data...")
    history = scraper.get_history(period="60d", interval="1h")
    
    # Get current info
    print("Fetching current market info...")
    info = scraper.get_info()
    
    # Combine
    data = {
        "scraped_at": datetime.now().isoformat(),
        "ticker": "BTC-USD",
        "current": info,
        "history": history
    }
    
    # Save
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    scraper.save_to_json(data, f"btc_market_{timestamp}.json")
    scraper.save_to_json(data, "btc_market_latest.json")
    
    # Print summary
    print(f"\nCurrent BTC Price: ${info['price']:,.2f}")
    print(f"24h Range: ${info['day_low']:,.2f} - ${info['day_high']:,.2f}")
    print(f"Market Cap: ${info['market_cap']:,.0f}")
    print(f"Historical records: {len(history)} hours")
    print("=" * 50)


if __name__ == "__main__":
    main()