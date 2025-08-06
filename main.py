import yfinance as yf
import time
import os
from supabase import create_client, Client
from datetime import datetime
import json
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta
import pytz

load_dotenv()

# Environment variables
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
JAKARTA_TZ = pytz.timezone("Asia/Jakarta")

def is_within_trading_hours(now):
    return 9 <= now.hour < 16

def sleep_until_next_active_window(now):
    # Calculate when to wake up next day at 9AM WIB
    if now.hour >= 16:
        # Next day
        next_start = (now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
    elif now.hour < 9:
        # Later this morning
        next_start = now.replace(hour=9, minute=0, second=0, microsecond=0)
    else:
        return 0  # we're within trading hours, no sleep needed

    sleep_seconds = (next_start - now).total_seconds()
    print(f"🌙 Outside trading hours. Sleeping until next window at {next_start.strftime('%H:%M')} WIB ({int(sleep_seconds)}s)")
    return sleep_seconds

print(SUPABASE_KEY)

# IDX stocks (append .JK) and ^JKSE
STOCKS = {
    "^JKSE": "IHSG",
    "BBCA.JK": "BBCA",
    "TLKM.JK": "TLKM",
    "UNVR.JK": "UNVR",
    "GGRM.JK": "GGRM",
    "BBRI.JK": "BBRI",
    "ICBP.JK": "ICBP",
    "INTP.JK": "INTP",
    "BMRI.JK": "BMRI",
    "KLBF.JK": "KLBF",
    "ASII.JK": "ASII",
}

# Connect to Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def safe_convert(value, target_type):
    """Safely convert value to target type, return None if conversion fails"""
    if value is None or value == "":
        return None
    try:
        if target_type == int:
            return int(float(value)) if isinstance(value, (int, float, str)) else None
        elif target_type == float:
            return float(value) if isinstance(value, (int, float, str)) else None
        elif target_type == str:
            return str(value)
        elif target_type == bool:
            return bool(value)
        else:
            return value
    except (ValueError, TypeError):
        return None

def parse_stock_info(info):

    """Parse yfinance info dict into structured data with proper types"""
    
    currentPrice = safe_convert(info.get("currentPrice"), str)
    if currentPrice is None:
        currentPrice = safe_convert(info.get("regularMarketPrice"), str)

    return {
        # Basic Info
        "symbol": safe_convert(info.get("symbol"), str),
        "short_name": safe_convert(info.get("shortName"), str),
        "long_name": safe_convert(info.get("longName"), str),
        "exchange": safe_convert(info.get("exchange"), str),
        "currency": safe_convert(info.get("currency"), str),
        "quote_type": safe_convert(info.get("quoteType"), str),
        "market": safe_convert(info.get("market"), str),
        "time_zone": safe_convert(info.get("timeZoneFullName"), str),
        
        # Company Info
        "sector": safe_convert(info.get("sector"), str),
        "industry": safe_convert(info.get("industry"), str),
        "full_time_employees": safe_convert(info.get("fullTimeEmployees"), int),
        "business_summary": safe_convert(info.get("longBusinessSummary"), str),
        "website": safe_convert(info.get("website"), str),
        "address1": safe_convert(info.get("address1"), str),
        "city": safe_convert(info.get("city"), str),
        "zip_code": safe_convert(info.get("zip"), str),
        "country": safe_convert(info.get("country"), str),
        "phone": safe_convert(info.get("phone"), str),
        
        # Financial Metrics
        "market_cap": safe_convert(info.get("marketCap"), int),
        "enterprise_value": safe_convert(info.get("enterpriseValue"), int),
        "shares_outstanding": safe_convert(info.get("sharesOutstanding"), int),
        "float_shares": safe_convert(info.get("floatShares"), int),
        "shares_short": safe_convert(info.get("sharesShort"), int),
        "book_value": safe_convert(info.get("bookValue"), float),
        "price_to_book": safe_convert(info.get("priceToBook"), float),
        
        # Price Metrics
        "current_price": currentPrice,
        "previous_close": safe_convert(info.get("previousClose"), float),
        "open_price": safe_convert(info.get("open"), float),
        "day_low": safe_convert(info.get("dayLow"), float),
        "day_high": safe_convert(info.get("dayHigh"), float),
        "fifty_two_week_low": safe_convert(info.get("fiftyTwoWeekLow"), float),
        "fifty_two_week_high": safe_convert(info.get("fiftyTwoWeekHigh"), float),
        "fifty_day_average": safe_convert(info.get("fiftyDayAverage"), float),
        "two_hundred_day_average": safe_convert(info.get("twoHundredDayAverage"), float),
        
        # Valuation Ratios
        "pe_ratio": safe_convert(info.get("trailingPE"), float),
        "forward_pe": safe_convert(info.get("forwardPE"), float),
        "peg_ratio": safe_convert(info.get("trailingPegRatio"), float),
        "price_to_sales": safe_convert(info.get("priceToSalesTrailing12Months"), float),
        "enterprise_to_revenue": safe_convert(info.get("enterpriseToRevenue"), float),
        "enterprise_to_ebitda": safe_convert(info.get("enterpriseToEbitda"), float),
        
        # Profitability Metrics
        "profit_margin": safe_convert(info.get("profitMargins"), float),
        "operating_margin": safe_convert(info.get("operatingMargins"), float),
        "return_on_assets": safe_convert(info.get("returnOnAssets"), float),
        "return_on_equity": safe_convert(info.get("returnOnEquity"), float),
        "revenue_growth": safe_convert(info.get("revenueGrowth"), float),
        "earnings_growth": safe_convert(info.get("earningsGrowth"), float),
        
        # Financial Statements Data
        "total_revenue": safe_convert(info.get("totalRevenue"), int),
        "revenue_per_share": safe_convert(info.get("revenuePerShare"), float),
        "total_cash": safe_convert(info.get("totalCash"), int),
        "total_cash_per_share": safe_convert(info.get("totalCashPerShare"), float),
        "total_debt": safe_convert(info.get("totalDebt"), int),
        "debt_to_equity": safe_convert(info.get("debtToEquity"), float),
        "current_ratio": safe_convert(info.get("currentRatio"), float),
        "quick_ratio": safe_convert(info.get("quickRatio"), float),
        
        # Earnings Data
        "trailing_eps": safe_convert(info.get("trailingEps"), float),
        "forward_eps": safe_convert(info.get("forwardEps"), float),
        "earnings_quarterly_growth": safe_convert(info.get("earningsQuarterlyGrowth"), float),
        
        # Dividend Info
        "dividend_rate": safe_convert(info.get("dividendRate"), float),
        "dividend_yield": safe_convert(info.get("dividendYield"), float),
        "payout_ratio": safe_convert(info.get("payoutRatio"), float),
        "five_year_avg_dividend_yield": safe_convert(info.get("fiveYearAvgDividendYield"), float),
        
        # Volume and Trading
        "volume": safe_convert(info.get("volume"), int),
        "regular_market_volume": safe_convert(info.get("regularMarketVolume"), int),
        "average_volume": safe_convert(info.get("averageVolume"), int),
        "average_volume_10days": safe_convert(info.get("averageVolume10days"), int),
        "average_daily_volume_10day": safe_convert(info.get("averageDailyVolume10Day"), int),
        
        # Beta and Risk
        "beta": safe_convert(info.get("beta"), float),
        
        # Recommendations
        "recommendation_mean": safe_convert(info.get("recommendationMean"), float),
        "recommendation_key": safe_convert(info.get("recommendationKey"), str),
        "number_of_analyst_opinions": safe_convert(info.get("numberOfAnalystOpinions"), int),
        
        # Target Prices
        "target_high_price": safe_convert(info.get("targetHighPrice"), float),
        "target_low_price": safe_convert(info.get("targetLowPrice"), float),
        "target_mean_price": safe_convert(info.get("targetMeanPrice"), float),
        "target_median_price": safe_convert(info.get("targetMedianPrice"), float),
        
        # Timestamps
        "last_split_date": safe_convert(info.get("lastSplitDate"), int),
        "last_dividend_date": safe_convert(info.get("lastDividendDate"), int),
        "ex_dividend_date": safe_convert(info.get("exDividendDate"), int),
        
        # Additional Fields
        "governance_epoch_date": safe_convert(info.get("governanceEpochDate"), int),
        "compensation_risk": safe_convert(info.get("compensationRisk"), int),
        "shareholder_rights_risk": safe_convert(info.get("shareHolderRightsRisk"), int),
        "overall_risk": safe_convert(info.get("overallRisk"), int),
        "board_risk": safe_convert(info.get("boardRisk"), int),
        "audit_risk": safe_convert(info.get("auditRisk"), int),
        
        # Metadata
        "updated_at": datetime.now().isoformat()
    }

def fetch_metadata(symbol):
    """Fetch basic metadata for stocks table"""
    t = yf.Ticker(symbol)
    hist = t.history(period="1d", interval="1m")
    
    current_price = float(hist["Close"].iloc[-1]) if not hist.empty else None

    info = t.info
    name = info.get("longName", "Unknown")
    sector = info.get("sector", "Unknown")
    previousClose = safe_convert(info.get("previousClose"), float)
    
    return {
        "current_price": current_price,
        "name": name,
        "sector": sector,
        "previous_close": previousClose,
        "info": info  # Return full info for stock-detail table
    }

def upsert_stock_detail(symbol_clean, info):
    """Insert or update stock detail information"""
    stock_detail_data = parse_stock_info(info)
    stock_detail_data["stock_symbol"] = symbol_clean  # Foreign key reference
    
    # Check if record exists
    existing = supabase.table("stock-detail").select("id").eq("stock_symbol", symbol_clean).execute()
    
    if existing.data:
        # Update existing record
        supabase.table("stock-detail").update(stock_detail_data).eq("stock_symbol", symbol_clean).execute()
        print(f"📊 Updated stock details for {symbol_clean}")
    else:
        # Insert new record
        supabase.table("stock-detail").insert(stock_detail_data).execute()
        print(f"📊 Inserted stock details for {symbol_clean}")

def upsert_stock(symbol_yahoo, symbol_clean):
    """Main function to update both stocks and stock-detail tables"""
    data = fetch_metadata(symbol_yahoo)
    if data["current_price"] is None:
        print(f"Skipping {symbol_clean}, no price data.")
        return

    # Update stocks table (original functionality)
    existing = supabase.table("stocks").select("id").eq("symbol", symbol_clean).execute()

    if existing.data:
        # Update existing row
        supabase.table("stocks").update({
            "current_price": data["current_price"],
            "previous_close": data["previous_close"]
        }).eq("symbol", symbol_clean).execute()
        print(f"✅ Updated {symbol_clean}: {data['current_price']}")
    else:
        # Insert new row
        supabase.table("stocks").insert({
            "symbol": symbol_clean,
            "name": data["name"],
            "sector": data["sector"],
            "previous_close": data["previous_close"],
            "current_price": data["current_price"]
        }).execute()
        print(f"➕ Inserted {symbol_clean}: {data['current_price']}")
    
    # Update stock-detail table (new functionality)
    try:
        upsert_stock_detail(symbol_clean, data["info"])
    except Exception as e:
        print(f"❌ Error updating stock details for {symbol_clean}: {str(e)}")

# Main loop
while True:
    now = datetime.now(JAKARTA_TZ)

    if is_within_trading_hours(now):
        for symbol_yahoo, symbol_clean in STOCKS.items():
            upsert_stock(symbol_yahoo, symbol_clean)
        print("⏱️ Sleeping for 5 minutes...")
        time.sleep(5 * 60)
    else:
        sleep_duration = sleep_until_next_active_window(now)
        time.sleep(sleep_duration)