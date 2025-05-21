"""
ETF Data Collector

This script collects ETF data from Yahoo Finance and stores it in Firebase.
It can be run in three modes:
1. Scheduled (via GitHub Actions): Runs every 5 minutes during market hours (4:00 AM - 4:00 PM EST)
2. Manual (via GitHub Actions): Can be triggered manually, bypassing market hours check
3. Local: Can be run locally with --manual flag to bypass market hours check

Environment Variables Required:
    GCP_SA_KEY: Firebase service account key (JSON)
    FIREBASE_PROJECT_ID: Firebase project ID
    FIREBASE_ETF_COLLECTION: Firebase collection name for ETF data
    TRIGGER_TYPE: Set by GitHub Actions ('schedule' or 'workflow_dispatch')

Usage:
    # Run in local mode (checks market hours)
    python etf_crawler.py

    # Run in manual mode (bypasses market hours check)
    python etf_crawler.py --manual

Data Collected:
    - Price and volume data
    - Market metrics (beta, market cap)
    - Daily statistics (high, low, open, close)
    - Category and leverage information

ETF Categories:
    - High Volatility Pairs (e.g., TQQQ/SQQQ)
    - Major Market ETFs (e.g., SPY, QQQ)
    - Sector ETFs (e.g., XLK, XLF)
    - International/Regional (e.g., YINN/YANG)
    - Volatility ETFs (e.g., VIX, UVXY)
    - Other Active Options (e.g., TLT, GLD)

Author: Your Name
Date: 2024
"""

import os
import sys
import json
import yfinance as yf
import pandas as pd
from datetime import datetime, time
import pytz
import firebase_admin
from firebase_admin import credentials, firestore
import logging
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from enum import Enum
import argparse
import time as time_module
from functools import wraps

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
MARKET_OPEN_TIME = time(4, 0)  # 4:00 AM EST
MARKET_CLOSE_TIME = time(16, 0)  # 4:00 PM EST
TIMEZONE = 'US/Eastern'
MAX_RETRIES = 3
RETRY_DELAY = 1

def retry_on_failure(max_retries: int = MAX_RETRIES, delay: int = RETRY_DELAY) -> Callable:
    """Decorator for retrying functions on failure."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Failed after {max_retries} attempts: {str(e)}")
                        raise
                    logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying...")
                    time_module.sleep(delay)
            return None
        return wrapper
    return decorator

class ETFCategory(Enum):
    """Categories of ETFs for better organization."""
    HIGH_VOLATILITY = "High Volatility Pairs"
    MAJOR_MARKET = "Major Market ETFs"
    SECTOR = "Sector ETFs"
    INTERNATIONAL = "International/Regional"
    VOLATILITY = "Volatility ETFs"
    OTHER = "Other Active Options"

@dataclass
class ETF:
    """ETF data structure with metadata."""
    symbol: str
    category: ETFCategory
    description: str
    leverage: Optional[float] = None
    inverse: Optional[str] = None

class TriggerType(Enum):
    """Types of workflow triggers."""
    SCHEDULE = "schedule"
    MANUAL = "workflow_dispatch"
    LOCAL = "local"

# ETF definitions
ETF_DEFINITIONS: List[ETF] = [
    # High Volatility Pairs
    ETF('TQQQ', ETFCategory.HIGH_VOLATILITY, '3x NASDAQ-100 - Tech bull', 3.0, 'SQQQ'),
    ETF('SQQQ', ETFCategory.HIGH_VOLATILITY, '-3x NASDAQ-100 - Tech bear', -3.0, 'TQQQ'),
    ETF('SOXL', ETFCategory.HIGH_VOLATILITY, '3x Semiconductor - Semi bull', 3.0, 'SOXS'),
    ETF('SOXS', ETFCategory.HIGH_VOLATILITY, '-3x Semiconductor - Semi bear', -3.0, 'SOXL'),
    ETF('LABU', ETFCategory.HIGH_VOLATILITY, '3x Biotech - Biotech bull', 3.0, 'LABD'),
    ETF('LABD', ETFCategory.HIGH_VOLATILITY, '-3x Biotech - Biotech bear', -3.0, 'LABU'),
    ETF('BOIL', ETFCategory.HIGH_VOLATILITY, '2x Natural Gas - Nat gas bull', 2.0, 'KOLD'),
    ETF('KOLD', ETFCategory.HIGH_VOLATILITY, '-2x Natural Gas - Nat gas bear', -2.0, 'BOIL'),
    ETF('BITX', ETFCategory.HIGH_VOLATILITY, '2x Bitcoin Strategy - Crypto bull', 2.0, 'BITI'),
    ETF('BITI', ETFCategory.HIGH_VOLATILITY, '-2x Bitcoin Strategy - Crypto bear', -2.0, 'BITX'),
    
    # Major Market ETFs
    ETF('SPY', ETFCategory.MAJOR_MARKET, 'S&P 500 - Most liquid ETF'),
    ETF('QQQ', ETFCategory.MAJOR_MARKET, 'NASDAQ-100 - Tech heavy'),
    ETF('IWM', ETFCategory.MAJOR_MARKET, 'Russell 2000 - Small caps'),
    ETF('DIA', ETFCategory.MAJOR_MARKET, 'Dow Jones - Blue chips'),
    
    # Sector ETFs
    ETF('XLK', ETFCategory.SECTOR, 'Technology Sector'),
    ETF('XLF', ETFCategory.SECTOR, 'Financial Sector'),
    ETF('XLE', ETFCategory.SECTOR, 'Energy Sector'),
    ETF('XLV', ETFCategory.SECTOR, 'Healthcare Sector'),
    
    # International/Regional
    ETF('YINN', ETFCategory.INTERNATIONAL, '3x China - China bull', 3.0, 'YANG'),
    ETF('YANG', ETFCategory.INTERNATIONAL, '-3x China - China bear', -3.0, 'YINN'),
    
    # Volatility ETFs
    ETF('UVXY', ETFCategory.VOLATILITY, 'Short-term VIX'),
    
    # Other Active Options
    ETF('TLT', ETFCategory.OTHER, '20+ Year Treasury'),
    ETF('GLD', ETFCategory.OTHER, 'Gold'),
    ETF('TZA', ETFCategory.OTHER, '-3x Russell 2000', -3.0)
]

class MarketHours:
    """Handles market hours checking and timezone management."""
    def __init__(self, open_time: time = MARKET_OPEN_TIME, close_time: time = MARKET_CLOSE_TIME):
        self.open_time = open_time
        self.close_time = close_time
        self.timezone = self._get_timezone()

    def _get_timezone(self) -> pytz.timezone:
        """Get the appropriate timezone with fallback."""
        try:
            return pytz.timezone(TIMEZONE)
        except pytz.exceptions.UnknownTimeZoneError:
            logger.error(f"Failed to set timezone to {TIMEZONE}. Using UTC.")
            return pytz.UTC

    def is_market_open(self) -> bool:
        """Check if current time is within market hours."""
        try:
            current_time = datetime.now(self.timezone).time()
            return self.open_time <= current_time <= self.close_time
        except Exception as e:
            logger.error(f"Error checking market hours: {str(e)}")
            return False

class FirebaseManager:
    """Manages Firebase operations and data validation."""
    def __init__(self):
        self.db = None
        self.required_env_vars = ['GCP_SA_KEY', 'FIREBASE_PROJECT_ID', 'FIREBASE_ETF_COLLECTION']

    @retry_on_failure(max_retries=MAX_RETRIES, delay=RETRY_DELAY)
    def initialize(self) -> firestore.Client:
        """Initialize Firebase with proper error handling."""
        try:
            self._validate_environment()
            service_account_info = self._get_service_account_info()
            cred = credentials.Certificate(service_account_info)
            firebase_admin.initialize_app(cred)
            self.db = firestore.client()
            logger.info("Successfully initialized Firebase")
            return self.db
        except Exception as e:
            logger.error(f"Error initializing Firebase: {str(e)}")
            raise

    def _validate_environment(self) -> None:
        """Validate required environment variables."""
        missing_vars = [var for var in self.required_env_vars if not os.environ.get(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    def _get_service_account_info(self) -> Dict:
        """Get and parse the GCP service account key."""
        gcp_sa_key = os.environ.get('GCP_SA_KEY')
        if not gcp_sa_key:
            raise ValueError("GCP_SA_KEY environment variable is not set or is empty.")
        try:
            return json.loads(gcp_sa_key)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid GCP_SA_KEY JSON format: {str(e)}")

    def _validate_etf_data(self, data: Dict[str, Any]) -> bool:
        """Validate ETF data before saving."""
        required_fields = ['symbol', 'price', 'timestamp']
        if not all(field in data for field in required_fields):
            logger.error(f"Missing required fields in ETF data: {required_fields}")
            return False
        if not isinstance(data['price'], (int, float)) or data['price'] <= 0:
            logger.error(f"Invalid price value for {data['symbol']}: {data['price']}")
            return False
        return True

    @retry_on_failure(max_retries=MAX_RETRIES, delay=RETRY_DELAY)
    def save_etf_data(self, data: Dict[str, Any]) -> None:
        """Save ETF data to Firebase."""
        try:
            if not self._validate_etf_data(data):
                logger.error(f"Invalid data for {data.get('symbol', 'unknown')}. Skipping save.")
                return

            collection_ref = self.db.collection(os.environ.get('FIREBASE_ETF_COLLECTION'))
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            doc_id = f"{data['symbol']}_{timestamp}"
            collection_ref.document(doc_id).set(data)
            logger.info(f"Successfully saved data for {data['symbol']} to Firebase")
        except Exception as e:
            logger.error(f"Error saving to Firebase: {str(e)}")
            raise

class ETFDataCollector:
    """Main class for collecting and processing ETF data."""
    def __init__(self, is_manual_trigger: bool = False):
        self.market_hours = MarketHours()
        self.firebase = FirebaseManager()
        self.is_manual_trigger = is_manual_trigger
        logger.info(f"Initialized ETFDataCollector with manual trigger: {is_manual_trigger}")

    @retry_on_failure(max_retries=MAX_RETRIES, delay=RETRY_DELAY)
    def get_etf_data(self, etf: ETF) -> Optional[Dict[str, Any]]:
        """Get current ETF data from Yahoo Finance."""
        try:
            ticker = yf.Ticker(etf.symbol)
            info = ticker.info
            
            data = {
                'symbol': etf.symbol,
                'category': etf.category.value,
                'description': etf.description,
                'leverage': etf.leverage,
                'inverse_symbol': etf.inverse,
                'price': info.get('regularMarketPrice'),
                'change': info.get('regularMarketChange'),
                'change_percent': info.get('regularMarketChangePercent'),
                'volume': info.get('regularMarketVolume'),
                'market_cap': info.get('marketCap'),
                'timestamp': firestore.SERVER_TIMESTAMP,
                'date': datetime.now().strftime('%Y-%m-%d'),
                'time': datetime.now().strftime('%H:%M:%S'),
                'beta': info.get('beta3Year'),
                'avg_volume': info.get('averageVolume'),
                'day_high': info.get('dayHigh'),
                'day_low': info.get('dayLow'),
                'open': info.get('regularMarketOpen'),
                'previous_close': info.get('regularMarketPreviousClose')
            }
            
            logger.info(f"Successfully fetched data for {etf.symbol}")
            return data
        except Exception as e:
            logger.error(f"Error getting data for {etf.symbol}: {str(e)}")
            raise

    def run(self) -> None:
        """Main execution method."""
        if not self.is_manual_trigger and not self.market_hours.is_market_open():
            logger.info("Outside market hours (4:00 AM - 4:00 PM EST). Exiting.")
            return

        logger.info("Starting ETF data collection...")
        try:
            db = self.firebase.initialize()
            success_count = 0
            failure_count = 0

            for etf in ETF_DEFINITIONS:
                logger.info(f"Fetching data for {etf.symbol}...")
                try:
                    data = self.get_etf_data(etf)
                    if data:
                        self.firebase.save_etf_data(data)
                        success_count += 1
                    else:
                        logger.warning(f"Failed to get data for {etf.symbol}")
                        failure_count += 1
                except Exception as e:
                    logger.error(f"Error processing {etf.symbol}: {str(e)}")
                    failure_count += 1

            logger.info(f"ETF data collection completed. Success: {success_count}, Failures: {failure_count}")
        except Exception as e:
            logger.error(f"Fatal error during ETF data collection: {str(e)}")
            sys.exit(1)

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='ETF Data Collector')
    parser.add_argument(
        '--manual',
        action='store_true',
        help='Run in manual mode (bypass market hours check)'
    )
    return parser.parse_args()

def get_trigger_type() -> TriggerType:
    """Determine the trigger type of the current run."""
    trigger_type = os.environ.get('TRIGGER_TYPE')
    if not trigger_type:
        return TriggerType.LOCAL
    
    try:
        return TriggerType(trigger_type)
    except ValueError:
        logger.warning(f"Unknown trigger type: {trigger_type}. Defaulting to local.")
        return TriggerType.LOCAL

def main() -> None:
    """Main entry point for the ETF data collector."""
    args = parse_args()
    
    # Determine trigger type and manual status
    trigger_type = get_trigger_type()
    is_manual = trigger_type == TriggerType.MANUAL or (trigger_type == TriggerType.LOCAL and args.manual)
    
    logger.info(f"Running with trigger type: {trigger_type.value}")
    logger.info(f"Manual mode: {is_manual}")
    
    collector = ETFDataCollector(is_manual_trigger=is_manual)
    collector.run()

if __name__ == "__main__":
    main() 