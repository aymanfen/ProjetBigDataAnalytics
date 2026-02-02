import yfinance as yf
import json
import os
from datetime import datetime, timedelta


class BTCMarketScraper:
    
    TICKER = "BTC-USD"
    
    def __init__(self):
        self.btc = yf.Ticker(self.TICKER)
    
    def get_history(self, period="3mo", interval="1d"):
        """Get historical OHLCV data. Period: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, max"""
        df = self.btc.history(period=period, interval=interval)
        return self._df_to_records(df)
    
    def get_range(self, start_date, end_date, interval="1d"):
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
        df = df.reset_index()
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
        records = df.to_dict(orient="records")
        for r in records:
            r["date"] = r.pop("Date")
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
    print("BTC Market Scraper")
    print("=" * 50)
    
    # Get last 3 months of daily data
    print("\nFetching 3 months of BTC data...")
    history = scraper.get_history(period="3mo")
    
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
    print(f"Historical records: {len(history)} days")
    print("=" * 50)


if __name__ == "__main__":
    main()

