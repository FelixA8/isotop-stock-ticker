import yfinance as yf
import os
from supabase import create_client, Client
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
        next_start = (now + timedelta(days=1)).replace(hour=9, minute=10, second=0, microsecond=0)
    elif now.hour < 9:
        # Later this morning
        next_start = now.replace(hour=9, minute=10, second=0, microsecond=0)
    else:
        return 0  # we're within trading hours, no sleep needed

    sleep_seconds = (next_start - now).total_seconds()
    print(f"🌙 Outside trading hours. Sleeping until next window at {next_start.strftime('%H:%M')} WIB ({int(sleep_seconds)}s)")
    return sleep_seconds

print(SUPABASE_KEY)

# Separated: Index and individual stocks
INDEX_SYMBOLS = {
    "^JKSE": "IHSG"
}

STOCK_SYMBOLS = {
    "ADRO.JK": "ADRO",
    "ASII.JK": "ASII",
    "AKRA.JK": "AKRA",
    "ANTM.JK": "ANTM",
    "ASII.JK": "ASII",
    "BBCA.JK": "BBCA",
    "BBNI.JK": "BBNI",
    "BBRI.JK": "BBRI",
    "BMRI.JK": "BMRI",
    "ITMG.JK": "ITMG",
    "ICBP.JK": "ICBP",
    "INDF.JK": "INDF",
    "ISAT.JK": "ISAT",
    "KLBF.JK": "KLBF",
    "MEDC.JK": "MEDC",
    "SMGR.JK": "SMGR",
    "PTBA.JK": "PTBA",
    "TLKM.JK": "TLKM",
    "UNTR.JK": "UNTR",
    "UNVR.JK": "UNVR",
    "TPIA.JK": "TPIA",
    "EXCL.JK": "EXCL",
    "GOTO.JK": "GOTO"
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
    """Fetch basic metadata for stocks/index table"""
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
        "info": info  # Return full info for stock_details table
    }

def calculate_change(current_price, previous_close):
    """Calculate change value and percentage"""
    if current_price is None or previous_close is None:
        return None, None
    
    change_value = current_price - previous_close
    change_percent = (change_value / previous_close) * 100 if previous_close != 0 else 0
    
    return change_value, change_percent

def bulk_upsert(table_name, data, key="symbol"):
    """Perform bulk upsert (insert/update) with manual conflict handling"""
    if not data:
        return

    # Get list of symbols to check
    symbols_to_check = [row[key] for row in data]
    
    # Check which records already exist
    existing_response = supabase.table(table_name).select(key).in_(key, symbols_to_check).execute()
    existing_symbols = set(row[key] for row in existing_response.data or [])
    
    # Split data into updates and inserts
    updates = [row for row in data if row[key] in existing_symbols]
    inserts = [row for row in data if row[key] not in existing_symbols]
    
    # Perform updates
    for row in updates:
        symbol_value = row[key]
        supabase.table(table_name).update(row).eq(key, symbol_value).execute()
    
    # Perform inserts
    if inserts:
        supabase.table(table_name).insert(inserts).execute()

def collect_all_data():
    """Collect all stock and index data in one batch"""
    stock_rows = []
    detail_rows = []
    index_rows = []
    index_history_rows = []
    
    current_timestamp = datetime.now()

    # Process individual stocks
    for symbol_yahoo, symbol_clean in STOCK_SYMBOLS.items():
        try:
            metadata = fetch_metadata(symbol_yahoo)
            if metadata["current_price"] is None:
                print(f"Skipping stock {symbol_clean}, no price.")
                continue

            info = metadata["info"]
            
            # Build stock row for stocks table
            stock_rows.append({
                "symbol": symbol_clean,
                "name": metadata["name"],
                "sector": metadata["sector"],
                "current_price": metadata["current_price"],
                "previous_close": metadata["previous_close"],
            })

            # Build stock_details row
            detail = parse_stock_info(info)
            detail["stock_symbol"] = symbol_clean
            detail_rows.append(detail)

        except Exception as e:
            print(f"⚠️ Error fetching stock {symbol_clean}: {str(e)}")

    # Process index data (IHSG)
    for symbol_yahoo, symbol_clean in INDEX_SYMBOLS.items():
        try:
            metadata = fetch_metadata(symbol_yahoo)
            if metadata["current_price"] is None:
                print(f"Skipping index {symbol_clean}, no price.")
                continue

            # Build index row for index_prices table
            # Note: change_value and change_percent are generated columns in DB
            index_rows.append({
                "symbol": symbol_clean,
                "name": metadata["name"],
                "last_price": metadata["current_price"],
                "previous_close": metadata["previous_close"],
                "updated_at": current_timestamp.isoformat()
            })

            # Build index history row for index_price_history table
            index_history_rows.append({
                "symbol": symbol_clean,
                "price": metadata["current_price"],
                "timestamp": current_timestamp.isoformat()
            })

        except Exception as e:
            print(f"⚠️ Error fetching index {symbol_clean}: {str(e)}")

    return stock_rows, detail_rows, index_rows, index_history_rows

def perform_bulk_updates():
    """Perform all database updates in one batch operation"""
    try:
        # Collect all data
        stock_data, detail_data, index_data, index_history_data = collect_all_data()

        # Perform all upserts in sequence
        if stock_data:
            bulk_upsert("stocks", stock_data, key="symbol")
            print(f"📈 Updated {len(stock_data)} stocks")

        if detail_data:
            bulk_upsert("stock_details", detail_data, key="stock_symbol")
            print(f"📊 Updated {len(detail_data)} stock details")

        if index_data:
            bulk_upsert("index_prices", index_data, key="symbol")
            print(f"📉 Updated {len(index_data)} indices")

        if index_history_data:
            # For history, we always insert new records (no duplicates expected)
            supabase.table("index_price_history").insert(index_history_data).execute()
            print(f"📅 Inserted {len(index_history_data)} index history records")

        return True

    except Exception as e:
        print(f"❌ Error during bulk update: {str(e)}")
        return False

# Main loop
while True:
    now = datetime.now(JAKARTA_TZ)
    if is_within_trading_hours(now):
        # Perform single bulk operation to minimize API calls
        success = perform_bulk_updates()
        
        if success:
            print(f"✅ Bulk update completed at {now.strftime('%H:%M:%S')} WIB")
        else:
            print(f"❌ Bulk update failed at {now.strftime('%H:%M:%S')} WIB")
            
        print(f"⏱️ Sleeping for 5 minutes...")
        time.sleep(5 * 60)
    else:
        sleep_duration = sleep_until_next_active_window(now)
        time.sleep(sleep_duration)
