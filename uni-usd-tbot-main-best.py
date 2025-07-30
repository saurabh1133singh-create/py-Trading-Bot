import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from delta_rest_client import DeltaRestClient, OrderType
from enum import Enum
import logging
import signal
import sys
import threading
import os
from dotenv import load_dotenv # type: ignore

# Load environment variables
load_dotenv()

# Define enums locally since the API uses string values
class OrderSide(Enum):
    BUY = 'buy'
    SELL = 'sell'

class TimeInForce(Enum):
    GTC = 'gtc'  # Good Till Cancelled
    IOC = 'ioc'  # Immediate or Cancel
    FOK = 'fok'  # Fill or Kill

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# API and bot settings - Load from environment variables
API_KEY     = os.getenv('DELTA_API_KEY')
API_SECRET  = os.getenv('DELTA_API_SECRET')
BASE_URL    = os.getenv('DELTA_BASE_URL', 'https://api.india.delta.exchange')
SYMBOL      = os.getenv('TRADING_SYMBOL', 'UNIUSD')
ASSET_ID    = os.getenv('TRADING_ASSET_ID', 'UNI')         # Asset for balance queries
SIZE        = int(os.getenv('TRADING_SIZE', '1'))             # lots per trade
LEVERAGE    = int(os.getenv('TRADING_LEVERAGE', '25'))            # 25x leverage
TIMEFRAME   = os.getenv('TRADING_TIMEFRAME', '15m')           # candle interval
EMA_LENGTH  = 50
EMA_OFFSET  = 0.03          # Offset for entry trigger
REQUEST_TIMEOUT = 30         # seconds
MAX_RETRIES = 3
UPDATE_INTERVAL = 60         # seconds (1 minute) - for EMA calculation
TICKER_INTERVAL = 2          # seconds - for fast price monitoring

# Validate required environment variables
if not API_KEY or not API_SECRET:
    raise ValueError("DELTA_API_KEY and DELTA_API_SECRET must be set in environment variables or .env file")

# --- Supertrend calculation ---
def calculate_supertrend(df, period=10, multiplier=3):
    high = df['high']
    low = df['low']
    close = df['close']
    atr = pd.Series(np.nan, index=df.index)
    tr = pd.Series(np.nan, index=df.index)
    tr.iloc[0] = high.iloc[0] - low.iloc[0]
    for i in range(1, len(df)):
        tr.iloc[i] = max(
            high.iloc[i] - low.iloc[i],
            abs(high.iloc[i] - close.iloc[i-1]),
            abs(low.iloc[i] - close.iloc[i-1])
        )
    atr.iloc[period-1] = tr.iloc[:period].mean()
    for i in range(period, len(df)):
        atr.iloc[i] = (atr.iloc[i-1] * (period-1) + tr.iloc[i]) / period
    upperband = ((high + low) / 2) + (multiplier * atr)
    lowerband = ((high + low) / 2) - (multiplier * atr)
    supertrend = pd.Series(np.nan, index=df.index)
    direction = pd.Series(np.nan, index=df.index)
    for i in range(len(df)):
        if i < period:
            continue
        if i == period:
            if close.iloc[i] > upperband.iloc[i]:
                supertrend.iloc[i] = lowerband.iloc[i]
                direction.iloc[i] = 1
            else:
                supertrend.iloc[i] = upperband.iloc[i]
                direction.iloc[i] = -1
        else:
            prev_supertrend = supertrend.iloc[i-1]
            prev_direction = direction.iloc[i-1]
            curr_upper = upperband.iloc[i]
            curr_lower = lowerband.iloc[i]
            if prev_direction == 1:
                if close.iloc[i] < prev_supertrend:
                    direction.iloc[i] = -1
                    supertrend.iloc[i] = curr_upper
                else:
                    direction.iloc[i] = 1
                    supertrend.iloc[i] = max(curr_lower, prev_supertrend)
            elif prev_direction == -1:
                if close.iloc[i] > prev_supertrend:
                    direction.iloc[i] = 1
                    supertrend.iloc[i] = curr_lower
                else:
                    direction.iloc[i] = -1
                    supertrend.iloc[i] = min(curr_upper, prev_supertrend)
            else:
                direction.iloc[i] = 0
                supertrend.iloc[i] = np.nan
    df['supertrend'] = supertrend
    df['supertrend_direction'] = direction
    return df

class DeltaTradingBot:
    def __init__(self):
        self.delta_client = None
        self.pending_buy_price = None
        self.pending_sell_price = None
        self.last_signal = None
        self.position = None
        self.entry_price = None
        self.running = True
        self.cycle_count = 0
        self.failed_orders = []
        self.last_price = None
        self.current_ema = None
        self.prev_ema = None
        self.prev_close = None
        self.last_candle_update = 0
        self.last_signal_time = 0
        self.signal_cooldown = 5  # seconds between signals to prevent spam
        # Restore EMA cross-wait logic variables
        self.wait_for_cross = False  # Flag to indicate waiting for price to cross EMA after SL/TP
        self.last_close_side = None  # Track which side the last position was closed ('long' or 'short')
        self.cross_confirmed = False  # Flag to confirm EMA cross has occurred
        # --- Supertrend trailing logic ---
        # self.current_supertrend = None
        # self.current_supertrend_direction = None
        # self.st_trail_active = False
        # self.st_trailing_sl = None
        # self.st_entry_direction = None
        # self.st_entry_supertrend = None
        # self.last_position_open_time = None  # Track time of last position open
        # self.position_skip_cycles = 2        # Number of ticker cycles to skip SL/TP after entry
        # self.position_open_cycle = None      # Track ticker cycle when position was opened
        self.initialize_client()
        self.setup_signal_handlers()
        self.set_leverage()

    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}. Shutting down gracefully...")
        self.running = False

    def initialize_client(self):
        try:
            self.delta_client = DeltaRestClient(
                base_url=BASE_URL,
                api_key=API_KEY,
                api_secret=API_SECRET
            )
            logger.info("Delta client initialized successfully")
            try:
                balance = self.delta_client.get_balances(ASSET_ID)
                logger.info(f"✅ API connection verified for asset {ASSET_ID}: {balance}")
            except Exception as e:
                logger.warning(f"⚠️ API connection test failed: {e}")
                logger.info("Continuing anyway - will validate during trading")
        except Exception as e:
            logger.error(f"Failed to initialize Delta client: {e}")
            raise

    def set_leverage(self):
        """Set leverage for the trading pair"""
        try:
            if self.delta_client is None:
                logger.error("Delta client not initialized")
                return False
                
            product_id = self.get_fresh_product_id()
            if not product_id:
                logger.error("Failed to get product ID for leverage setting")
                return False
            
            # Set leverage using the Delta API
            response = self.delta_client.set_leverage(product_id=product_id, leverage=LEVERAGE)
            logger.info(f"✅ Leverage set to {LEVERAGE}x for {SYMBOL}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to set leverage: {e}")
            logger.info("Continuing without leverage setting - using default leverage")
            return False

    def get_fresh_product_id(self):
        try:
            if self.delta_client is None:
                logger.error("Delta client not initialized")
                return None
                
            resp = self.delta_client.get_ticker(SYMBOL)
            if 'result' in resp and isinstance(resp['result'], dict):
                product_id = resp['result'].get('product_id')
            else:
                product_id = resp.get('product_id')
            if not product_id:
                logger.error(f"No product_id found in ticker response: {resp}")
                return None
            return product_id
        except Exception as e:
            logger.error(f"Failed to get fresh product ID: {e}")
            return None

    def get_ticker_price(self):
        """Get current mark price from ticker - fast method"""
        try:
            if self.delta_client is None:
                logger.error("Delta client not initialized")
                return None
                
            resp = self.delta_client.get_ticker(SYMBOL)
            if 'result' in resp and isinstance(resp['result'], dict):
                price = resp['result'].get('mark_price')
            else:
                price = resp.get('mark_price')
            
            if price:
                return float(price)
            else:
                logger.warning(f"No mark price found in ticker response: {resp}")
                return None
        except Exception as e:
            logger.error(f"Failed to get ticker mark price: {e}")
            return None

    def fetch_candles(self, symbol, resolution='1m', limit=100):
        for attempt in range(MAX_RETRIES):
            try:
                now_utc = datetime.now(timezone.utc)
                sec = self._get_resolution_seconds(resolution)
                end_ts = int((now_utc - timedelta(seconds=sec)).timestamp())
                start_ts = end_ts - (limit - 1) * sec
                url = f"{BASE_URL}/v2/history/candles"
                params = {'symbol': symbol, 'resolution': resolution, 'start': start_ts, 'end': end_ts}
                headers = {'Accept': 'application/json', 'User-Agent': 'DeltaTradingBot/1.0'}
                resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
                if 'result' not in data:
                    raise ValueError(f"Invalid response: {data}")
                df = pd.DataFrame(data['result'])
                required_cols = ['time','open','high','low','close','volume']
                if not all(col in df.columns for col in required_cols):
                    logger.error(f"Missing columns in candle data: {df.columns}")
                    return pd.DataFrame()
                df['timestamp'] = pd.to_datetime(df['time'], unit='s', utc=True)
                df = df[['timestamp','open','high','low','close','volume']]
                for col in ['open','high','low','close','volume']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                df = df.sort_values(by='timestamp', ascending=True).reset_index(drop=True)  # type: ignore
                logger.info(f"Fetched candles DataFrame shape: {df.shape}, head: {df.head(3)}")
                return df
            except Exception as e:
                logger.error(f"Fetch candles error: {e}")
                if attempt < MAX_RETRIES-1:
                    time.sleep(2**attempt)
                else:
                    return pd.DataFrame()

    def _get_resolution_seconds(self, resolution):
        return {'1m':60,'5m':300,'15m':900,'30m':1800,'1h':3600,'4h':14400,'1d':86400}.get(resolution,60)

    def pine_ema(self, src, length):
        """Improved Pine Script EMA calculation that properly updates with new data"""
        try:
            src = np.array(src, dtype=float)
            if len(src) == 0:
                return np.full(0, np.nan)
            
            # Remove NaN values and get valid data
            valid_mask = ~np.isnan(src)
            if not valid_mask.any():
                return np.full(len(src), np.nan)
            
            # Calculate alpha (smoothing factor)
            alpha = 2.0 / (length + 1)
            
            # Initialize EMA array
            ema = np.full(len(src), np.nan)
            
            # Find first valid value and set it as initial EMA
            first_valid_idx = np.where(valid_mask)[0][0]
            ema[first_valid_idx] = src[first_valid_idx]
            
            # Calculate EMA for all subsequent values
            for i in range(first_valid_idx + 1, len(src)):
                if valid_mask[i]:
                    # Standard EMA formula: EMA = α × Price + (1 - α) × Previous EMA
                    ema[i] = alpha * src[i] + (1 - alpha) * ema[i-1]
                else:
                    # Carry forward previous EMA value for NaN inputs
                    ema[i] = ema[i-1]
            
            return ema
            
        except Exception as e:
            logger.error(f"Pine EMA calculation error: {e}")
            return np.full(len(src), np.nan)

    def backup_ema(self, src, length):
        """Backup EMA calculation using pandas ewm method for better reliability"""
        try:
            import pandas as pd
            src = np.array(src, dtype=float)
            if len(src) == 0:
                return np.full(0, np.nan)
            
            # Use pandas ewm for more reliable calculation
            df = pd.DataFrame({'close': src})
            ema_series = df['close'].ewm(span=length, adjust=False).mean()
            return ema_series.values
            
        except Exception as e:
            logger.error(f"Backup EMA calculation failed: {e}")
            return np.full(len(src), np.nan)

    def calculate_ema_with_backup(self, src, length):
        """Calculate EMA with primary method and backup fallback"""
        try:
            # Try primary method first
            primary_ema = self.pine_ema(src, length)
            
            # Check if primary method produced valid results
            valid_primary = primary_ema[~np.isnan(primary_ema)]
            if len(valid_primary) >= 2:
                self.current_ema_method = 'primary'
                self.ema_fail_count = 0
                logger.info(f"✅ Primary EMA calculation successful - Last value: {valid_primary[-1]:.6f}")
                return primary_ema
            
            # If primary failed, try backup method
            logger.warning("⚠️ Primary EMA failed, trying backup method...")
            backup_ema = self.backup_ema(src, length)
            
            valid_backup = backup_ema[~np.isnan(backup_ema)]
            if len(valid_backup) >= 2:
                self.current_ema_method = 'backup'
                self.ema_fail_count += 1
                logger.info(f"✅ Backup EMA calculation successful - Last value: {valid_backup[-1]:.6f}")
                return backup_ema
            
            # If both failed, increment failure count and return primary result anyway
            self.ema_fail_count += 1
            logger.error("❌ Both primary and backup EMA methods failed")
            return primary_ema
            
        except Exception as e:
            logger.error(f"EMA calculation error: {e}")
            return np.full(len(src), np.nan)

    def update_ema_data(self):
        """Update EMA data from candles - called periodically"""
        try:
            df = self.fetch_candles(SYMBOL, TIMEFRAME, 100)
            if df is None or df.empty: 
                logger.warning("No candle data available for EMA calculation")
                return False
            # Get the latest mark price for synthetic close
            latest_mark = self.get_ticker_price()
            if latest_mark is not None:
                df = df.copy()
                df.at[df.index[-1], 'close'] = latest_mark
                logger.info(f"Injected latest mark price {latest_mark} as synthetic close for EMA calculation.")
            # Log last 5 closes and timestamps for debug
            logger.info(f"Last 5 closes: {df['close'].tail().tolist()} at {df['timestamp'].tail().tolist()}")
            # Calculate EMA with backup
            df['ema'] = self.calculate_ema_with_backup(df['close'], EMA_LENGTH)
            # --- Supertrend calculation ---
            df = calculate_supertrend(df, period=10, multiplier=3)
            if 'supertrend' in df.columns:
                if bool(df['supertrend'].notna().any()):
                    last_valid_idx = df['supertrend'].last_valid_index()
                    if last_valid_idx is not None:
                        self.current_supertrend = df['supertrend'].loc[last_valid_idx]
                        self.current_supertrend_direction = df['supertrend_direction'].loc[last_valid_idx]
                        logger.info(f"Supertrend updated: Value={self.current_supertrend:.6f}, Direction={'UP' if self.current_supertrend_direction == 1 else 'DOWN'}")
                    else:
                        self.current_supertrend = None
                        self.current_supertrend_direction = None
                        logger.warning("Supertrend calculation returned all NaN.")
                else:
                    self.current_supertrend = None
                    self.current_supertrend_direction = None
                    logger.warning("Supertrend calculation failed or returned all NaN.")
            else:
                self.current_supertrend = None
                self.current_supertrend_direction = None
                logger.warning("Supertrend column missing from DataFrame.")
            # --- SL: Only EMA-based SL (no Supertrend) ---
            if len(df) >= 2:
                new_current_ema = df['ema'].iloc[-1]
                new_prev_ema = df['ema'].iloc[-2]
                new_prev_close = df['close'].iloc[-2]
                # Validate the new EMA values
                if (not np.isnan(new_current_ema) and not np.isnan(new_prev_ema) and 
                    new_current_ema > 0 and new_prev_ema > 0):
                    # Check if EMA actually changed (to detect if it's updating properly)
                    ema_changed = (self.current_ema is None or 
                                 abs(new_current_ema - self.current_ema) > 0.000001)
                    # Update the values
                    self.current_ema = new_current_ema
                    self.prev_ema = new_prev_ema
                    self.prev_close = new_prev_close
                    if ema_changed:
                        logger.info(f"🔄 EMA updated: Current={self.current_ema:.6f}, Previous={self.prev_ema:.6f} (Method: {self.current_ema_method})")
                    else:
                        logger.info(f"📊 EMA stable: Current={self.current_ema:.6f}, Previous={self.prev_ema:.6f} (Method: {self.current_ema_method})")
                    # Update the last update time
                    self.last_ema_update_time = time.time()
                    return True
                else:
                    logger.warning("Invalid EMA values calculated")
                    return False
            else:
                logger.warning("Insufficient data for EMA calculation")
                return False
        except Exception as e:
            logger.error(f"EMA update error: {e}")
            return False

    def check_ema_cross_wait(self, candle_close):
        """Wait for a new EMA cross (using candle close) after exit before allowing new pending orders."""
        if not self.wait_for_cross or self.current_ema is None:
            return
        # After closing long, wait for price to cross below EMA, then above again for new long
        if self.last_close_side == 'long':
            if candle_close < self.current_ema:
                self.cross_confirmed = True  # Price crossed below EMA
                self.cross_below = True
            elif hasattr(self, 'cross_below') and self.cross_below and candle_close > self.current_ema:
                # Now price crossed above EMA after being below
                self.wait_for_cross = False
                self.last_close_side = None
                self.cross_confirmed = False
                del self.cross_below
                logger.info("✅ EMA Cross Complete (LONG exit, candle close) - Ready for new signals")
        # After closing short, wait for price to cross above EMA, then below again for new short
        elif self.last_close_side == 'short':
            if candle_close > self.current_ema:
                self.cross_confirmed = True  # Price crossed above EMA
                self.cross_above = True
            elif hasattr(self, 'cross_above') and self.cross_above and candle_close < self.current_ema:
                # Now price crossed below EMA after being above
                self.wait_for_cross = False
                self.last_close_side = None
                self.cross_confirmed = False
                del self.cross_above
                logger.info("✅ EMA Cross Complete (SHORT exit, candle close) - Ready for new signals")
        # While waiting for cross, do not allow new pending orders
        return

    def check_trading_signals_with_candle_close(self, candle_close):
        """Check trading signals using candle close for cross and pending order reversal, ticker price for entry/exit"""
        current_time = time.time()

        # If waiting for EMA cross after exit, do nothing
        if self.wait_for_cross:
            return

        # If a pending order exists
        if self.pending_buy_price is not None or self.pending_sell_price is not None:
            # If price crosses EMA in the opposite direction before pending is filled, reverse pending order (use candle close)
            if (self.pending_buy_price is not None and self.current_ema is not None and candle_close < self.current_ema and self.prev_close is not None and self.prev_ema is not None and self.prev_close >= self.prev_ema):
                # Reverse to pending sell
                new_pending_sell = self.current_ema - EMA_OFFSET
                logger.info(f"🔄 Reversing pending BUY to SELL: {self.pending_buy_price:.6f} → {new_pending_sell:.6f} (Opposite EMA cross before fill, candle close)")
                self.pending_buy_price = None
                self.pending_sell_price = new_pending_sell
                self.pending_order_ema = self.current_ema
                self.last_signal_time = current_time
                return
            if (self.pending_sell_price is not None and self.current_ema is not None and candle_close > self.current_ema and self.prev_close is not None and self.prev_ema is not None and self.prev_close <= self.prev_ema):
                # Reverse to pending buy
                new_pending_buy = self.current_ema + EMA_OFFSET
                logger.info(f"🔄 Reversing pending SELL to BUY: {self.pending_sell_price:.6f} → {new_pending_buy:.6f} (Opposite EMA cross before fill, candle close)")
                self.pending_sell_price = None
                self.pending_buy_price = new_pending_buy
                self.pending_order_ema = self.current_ema
                self.last_signal_time = current_time
                return
            # If no reversal or fill, do nothing (entry is handled in fast_ticker_monitoring)
            return

        # If no position, no pending, and not waiting for cross, check for new EMA cross (use candle close)
        if (self.current_ema is None or self.prev_ema is None or self.prev_close is None or np.isnan(self.current_ema) or np.isnan(self.prev_ema)):
            return
        if current_time - self.last_signal_time < self.signal_cooldown:
            return
        # Buy cross (using candle close)
        if (candle_close > self.current_ema and self.prev_close <= self.prev_ema):
            self.pending_buy_price = self.current_ema + EMA_OFFSET
            self.pending_sell_price = None
            self.pending_order_ema = self.current_ema
            logger.info(f"🔵 BUY SIGNAL: Set pending buy at {self.pending_buy_price:.6f} (Candle Close {candle_close:.6f} > EMA {self.current_ema:.6f})")
            self.last_signal_time = current_time
        # Sell cross (using candle close)
        elif (candle_close < self.current_ema and self.prev_close >= self.prev_ema):
            self.pending_sell_price = self.current_ema - EMA_OFFSET
            self.pending_buy_price = None
            self.pending_order_ema = self.current_ema
            logger.info(f"🔴 SELL SIGNAL: Set pending sell at {self.pending_sell_price:.6f} (Candle Close {candle_close:.6f} < EMA {self.current_ema:.6f})")
            self.last_signal_time = current_time
        # Otherwise, do nothing
        return

    def execute_market_order(self, side, price):
        pid = self.get_fresh_product_id()
        if not pid: return False
        try:
            if self.delta_client is None:
                logger.error("Delta client not initialized")
                return False
            self.delta_client.place_order(product_id=pid, side=side, size=int(SIZE), order_type=OrderType.MARKET)
            self.position = 'long' if side=='buy' else 'short'
            self.entry_price = price
            logger.info(f"✅ Entered {self.position.upper()} position at {price:.6f}")
            self.last_position_open_time = time.time()  # Mark time of position open
            self.position_open_cycle = getattr(self, 'ticker_cycle', None)  # Mark ticker cycle of position open
            # --- Track entry candle timestamp for SL grace period ---
            candle_df = self.fetch_candles(SYMBOL, TIMEFRAME, 2)
            if candle_df is not None and not candle_df.empty:
                self.last_entry_candle_timestamp = candle_df['timestamp'].iloc[-1]
            else:
                self.last_entry_candle_timestamp = None
            self.trail_sl = None
            return True
        except Exception as e:
            logger.error(f"❌ Order failed: {e}")
            return False

    def close_position(self, side, reason=""):
        pid = self.get_fresh_product_id()
        if not pid: return False
        try:
            if self.delta_client is None:
                logger.error("Delta client not initialized")
                return False
            self.delta_client.place_order(product_id=pid, side=side, size=int(SIZE), order_type=OrderType.MARKET)
            logger.info(f"✅ Position closed with {side.upper()} order {reason}")
            # Set wait for cross flag if TP or Supertrend SL was hit (not for reversals)
            if "(TP)" in reason:
                self.last_close_side = self.position  # Remember which position was closed
                self.wait_for_cross = True
                self.cross_confirmed = False
                logger.info(f"⏳ {reason} hit - Now waiting for price to cross EMA (Last closed: {self.last_close_side.upper() if self.last_close_side else 'N/A'})")
            else:
                logger.info(f"🔄 Position closed for reversal - ready for immediate new position")
            self.position = None
            self.entry_price = None
            # Reset trailing variables (REMOVED)
            self.trail_sl = None
            return True
        except Exception as e:
            logger.error(f"❌ Failed to close position: {e}")
            return False

    def check_exit_conditions_with_ticker(self, current_price):
        """Check exit conditions using ticker price - called frequently (DEPRECATED: Logic moved to main loop)"""
        # This method is now deprecated - all exit logic is handled in fast_ticker_monitoring
        # Keeping for backward compatibility but not used
        pass

    def fast_ticker_monitoring(self):
        """Fast monitoring loop for strict cross→pending→entry→exit→wait cycle, candle close for cross/reversal, ticker price for entry/TP and trailing SL, candle close for simple SL, with SL grace period after entry"""
        ticker_cycle = 0
        FIXED_SL = 0.03
        self.trail_sl = None  # Reset on bot start
        self.last_entry_candle_timestamp = None  # Track entry candle for SL grace
        while self.running:
            ticker_cycle += 1
            self.ticker_cycle = ticker_cycle
            current_price = self.get_ticker_price()
            candle_close = self.get_candle_close_with_fallback()
            candle_df = None
            if current_price and candle_close is not None:
                self.last_price = current_price
                # Use candle close for simple SL, trailing SL
                if self.position:
                    if self.entry_price is not None:
                        # --- SL grace period: skip SL checks until a new candle close after entry ---
                        if self.last_entry_candle_timestamp is not None:
                            if candle_df is None:
                                candle_df = self.fetch_candles(SYMBOL, TIMEFRAME, 2)
                            if candle_df is not None and not candle_df.empty:
                                latest_candle_ts = candle_df['timestamp'].iloc[-1]
                                if latest_candle_ts == self.last_entry_candle_timestamp:
                                    # Still same candle as entry, skip SL checks
                                    pass  # Do not check SL/Trail SL
                                else:
                                    self.last_entry_candle_timestamp = None  # Reset grace period, allow SL checks
                            else:
                                # If can't fetch candles, be safe and skip SL checks
                                pass
                        if self.last_entry_candle_timestamp is None:
                            # --- Trailing SL logic ---
                            profit = candle_close - self.entry_price if self.position == 'long' else self.entry_price - candle_close
                            # Activate 0.47 trail SL if profit >= 0.5 (candle close only)
                            if self.position == 'long' and profit >= 0.5:
                                new_trail_047 = self.entry_price + 0.47
                                if not hasattr(self, 'trail_sl_047') or self.trail_sl_047 is None or new_trail_047 > self.trail_sl_047:
                                    self.trail_sl_047 = new_trail_047
                            elif self.position == 'short' and profit >= 0.5:
                                new_trail_047 = self.entry_price - 0.47
                                if not hasattr(self, 'trail_sl_047') or self.trail_sl_047 is None or new_trail_047 < self.trail_sl_047:
                                    self.trail_sl_047 = new_trail_047
                            # Other trailing SLs (use ticker price)
                            new_trail = None
                            if self.position == 'long':
                                if profit >= 0.4:
                                    new_trail = self.entry_price + 0.15
                                elif profit >= 0.3:
                                    new_trail = self.entry_price + 0.10
                                elif profit >= 0.2:
                                    new_trail = self.entry_price + 0.05
                            elif self.position == 'short':
                                if profit >= 0.4:
                                    new_trail = self.entry_price - 0.15
                                elif profit >= 0.3:
                                    new_trail = self.entry_price - 0.10
                                elif profit >= 0.2:
                                    new_trail = self.entry_price - 0.05
                            if new_trail is not None:
                                if self.trail_sl is None:
                                    self.trail_sl = new_trail
                                else:
                                    if self.position == 'long' and new_trail > self.trail_sl:
                                        self.trail_sl = new_trail
                                    elif self.position == 'short' and new_trail < self.trail_sl:
                                        self.trail_sl = new_trail
                            # Exit at fixed stop loss (use candle close)
                            if self.position == 'long' and candle_close <= self.entry_price - FIXED_SL:
                                logger.info(f"🛑 FIXED SL EXIT: Closing LONG at {candle_close:.6f} (<= Entry - SL {self.entry_price - FIXED_SL:.6f}) [Candle Close]")
                                self.close_position('sell', "(FIXED SL EXIT)")
                                self.wait_for_cross = True
                                self.trail_sl = None
                                self.trail_sl_047 = None
                                continue
                            elif self.position == 'short' and candle_close >= self.entry_price + FIXED_SL:
                                logger.info(f"🛑 FIXED SL EXIT: Closing SHORT at {candle_close:.6f} (>= Entry + SL {self.entry_price + FIXED_SL:.6f}) [Candle Close]")
                                self.close_position('buy', "(FIXED SL EXIT)")
                                self.wait_for_cross = True
                                self.trail_sl = None
                                self.trail_sl_047 = None
                                continue
                            # Exit at 0.47 trailing SL (candle close only)
                            if hasattr(self, 'trail_sl_047') and self.trail_sl_047 is not None:
                                if self.position == 'long' and candle_close <= self.trail_sl_047:
                                    logger.info(f"🟠 TRAIL SL 0.47 EXIT: Closing LONG at {candle_close:.6f} (<= Trail SL {self.trail_sl_047:.6f}) [Candle Close]")
                                    self.close_position('sell', "(TRAIL SL 0.47 EXIT)")
                                    self.wait_for_cross = True
                                    self.trail_sl = None
                                    self.trail_sl_047 = None
                                    continue
                                elif self.position == 'short' and candle_close >= self.trail_sl_047:
                                    logger.info(f"🟠 TRAIL SL 0.47 EXIT: Closing SHORT at {candle_close:.6f} (>= Trail SL {self.trail_sl_047:.6f}) [Candle Close]")
                                    self.close_position('buy', "(TRAIL SL 0.47 EXIT)")
                                    self.wait_for_cross = True
                                    self.trail_sl = None
                                    self.trail_sl_047 = None
                                    continue
                            # Exit at other trailing SLs (ticker price only)
                            if self.trail_sl is not None:
                                if self.position == 'long' and current_price <= self.trail_sl:
                                    logger.info(f"🟠 TRAIL SL EXIT: Closing LONG at {current_price:.6f} (<= Trail SL {self.trail_sl:.6f}) [Ticker Price]")
                                    self.close_position('sell', "(TRAIL SL EXIT)")
                                    self.wait_for_cross = True
                                    self.trail_sl = None
                                    self.trail_sl_047 = None
                                    continue
                                elif self.position == 'short' and current_price >= self.trail_sl:
                                    logger.info(f"🟠 TRAIL SL EXIT: Closing SHORT at {current_price:.6f} (>= Trail SL {self.trail_sl:.6f}) [Ticker Price]")
                                    self.close_position('buy', "(TRAIL SL EXIT)")
                                    self.wait_for_cross = True
                                    self.trail_sl = None
                                    self.trail_sl_047 = None
                                continue
                    else:
                        logger.warning(f"[Cycle {ticker_cycle}] Entry price is None - skipping exit check")
                else:
                    self.trail_sl = None
                # If not in position and not waiting for cross, handle pending order logic (cross/reversal with candle close)
                if not self.position and not self.wait_for_cross:
                    self.check_trading_signals_with_candle_close(candle_close)
                # If a pending order exists, check for fill using ticker price
                if not self.position and not self.wait_for_cross:
                    if self.pending_buy_price is not None and current_price >= self.pending_buy_price:
                        if self.execute_market_order('buy', current_price):
                            logger.info(f"🚀 BUY triggered at {current_price:.6f} (>= pending {self.pending_buy_price:.6f}) - Ticker Price")
                            self.pending_buy_price = None
                            self.pending_sell_price = None
                            self.pending_order_ema = None
                    elif self.pending_sell_price is not None and current_price <= self.pending_sell_price:
                        if self.execute_market_order('sell', current_price):
                            logger.info(f"🚀 SELL triggered at {current_price:.6f} (<= pending {self.pending_sell_price:.6f}) - Ticker Price")
                            self.pending_buy_price = None
                            self.pending_sell_price = None
                            self.pending_order_ema = None
                # If waiting for cross, check if cross has occurred (use candle close)
                if self.wait_for_cross:
                    self.check_ema_cross_wait(candle_close)
                # Display status every 30 ticker cycles (about 1 minute)
                if ticker_cycle % 30 == 0:
                    self.display_ticker_status(current_price, ticker_cycle, candle_close)
            time.sleep(TICKER_INTERVAL)

    def display_ticker_status(self, price, cycle, candle_close=None):
        IST = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(timezone.utc).astimezone(IST)
        
        status = f"Ticker#{cycle} @ {now.strftime('%H:%M:%S')} | Price: {price:.6f}"
        if candle_close is not None:
            status += f" | Candle Close: {candle_close:.6f}"
        
        if self.current_ema:
            status += f" | EMA: {self.current_ema:.6f} ({self.current_ema_method})"
            if self.ema_fail_count > 0:
                status += f" | Fails: {self.ema_fail_count}"
            
            # Show when EMA was last updated
            if self.last_ema_update_time:
                time_since_update = int(time.time() - self.last_ema_update_time)
                if time_since_update < 60:
                    status += f" | Updated: {time_since_update}s ago"
                else:
                    status += f" | Updated: {time_since_update//60}m ago"
        
        if self.position:
            pnl = price - self.entry_price if self.position == 'long' else self.entry_price - price
            status += f" | Position: {self.position.upper()} @ {self.entry_price:.6f} | PnL: {pnl:+.6f}"
            # Show SL status and which is active
            if self.entry_price is not None:
                fixed_sl = self.entry_price - 0.03 if self.position == 'long' else self.entry_price + 0.03
                trail_sl = self.trail_sl if hasattr(self, 'trail_sl') else None
                active_sl = None
                if trail_sl is not None:
                    # Determine which SL is closer to current candle_close in the direction of risk
                    if self.position == 'long':
                        if candle_close is not None and trail_sl > fixed_sl and candle_close > trail_sl:
                            active_sl = 'TRAIL'
                        elif candle_close is not None and fixed_sl >= trail_sl:
                            active_sl = 'FIXED'
                        else:
                            active_sl = 'TRAIL' if trail_sl > fixed_sl else 'FIXED'
                    elif self.position == 'short':
                        if candle_close is not None and trail_sl < fixed_sl and candle_close < trail_sl:
                            active_sl = 'TRAIL'
                        elif candle_close is not None and fixed_sl <= trail_sl:
                            active_sl = 'FIXED'
                        else:
                            active_sl = 'TRAIL' if trail_sl < fixed_sl else 'FIXED'
                else:
                    active_sl = 'FIXED'
                # Build SL status string
                if trail_sl is not None:
                    if active_sl == 'FIXED':
                        status += f" | SL: FIXED (0.03) (ACTIVE) | TRAIL SL: {trail_sl:.6f}"
                    else:
                        status += f" | SL: FIXED (0.03) | TRAIL SL: {trail_sl:.6f} (ACTIVE)"
                else:
                    status += f" | SL: FIXED (0.03) (ACTIVE)"
            else:
                status += f" | SL: N/A"
        elif self.wait_for_cross:
            status += f" | Position: WAITING FOR EMA CROSS (Last: {self.last_close_side.upper() if self.last_close_side else 'N/A'})"
        else:
            status += " | Position: NONE - Ready for REVERSAL and NEW signals"
        print(status)

    def run_ema_updates(self):
        """Separate thread for EMA updates - runs every 60 seconds"""
        logger.info("🔄 Starting EMA update thread...")
        while self.running:
            try:
                self.cycle_count += 1
                success = self.update_ema_data()
                
                if success:
                    logger.info(f"Cycle#{self.cycle_count} - EMA data updated successfully (Method: {self.current_ema_method})")
                else:
                    logger.warning(f"Cycle#{self.cycle_count} - EMA update failed (Fail count: {self.ema_fail_count})")
                
                time.sleep(UPDATE_INTERVAL)
            except Exception as e:
                logger.error(f"EMA update thread error: {e}")
                time.sleep(UPDATE_INTERVAL)

    def start_ema_update_thread(self):
        """Start the EMA update thread"""
        ema_thread = threading.Thread(target=self.run_ema_updates, daemon=True)
        ema_thread.start()
        logger.info("✅ EMA update thread started")
        return ema_thread

    def check_and_restore_open_position(self):
        """Check for any open position on startup and restore state if found."""
        try:
            if self.delta_client is None:
                logger.warning("Delta client not initialized - skipping position check.")
                return

            product_id = self.get_fresh_product_id()
            if not product_id:
                logger.warning("No product_id available for position check.")
                return

            pos = self.delta_client.get_position(product_id)
            logger.info(f"DeltaRestClient.get_position({product_id}) response: {pos}")

            if pos and 'size' in pos and pos['size'] and float(pos['size']) != 0 and 'entry_price' in pos and pos['entry_price']:
                side = 'long' if float(pos['size']) > 0 else 'short'
                self.position = side
                self.entry_price = float(pos['entry_price'])
                # Set entry candle timestamp to the most recent closed candle
                candle_df = self.fetch_candles(SYMBOL, TIMEFRAME, 2)
                if candle_df is not None and not candle_df.empty:
                    self.last_entry_candle_timestamp = candle_df['timestamp'].iloc[-1]
                else:
                    self.last_entry_candle_timestamp = None
                # Clear any pending orders
                self.pending_buy_price = None
                self.pending_sell_price = None
                self.pending_order_ema = None
                logger.info(f"✅ Restored open position: {side.upper()} @ {self.entry_price}")
            else:
                logger.info("✅ No open position found for product on startup.")
        except Exception as e:
            logger.error(f"Error in check_and_restore_open_position: {e}")

    def run_continuous(self):
        """Main execution method - focuses on fast ticker monitoring with CANDLE CLOSE for trading signals"""
        logger.info("Starting optimized Delta Trading Bot (Candle Close Trading Mode)...")
        logger.info(f"⚡ Ticker monitoring interval: {TICKER_INTERVAL}s")
        logger.info(f"📊 EMA update interval: {UPDATE_INTERVAL}s")
        logger.info(f"🔥 Leverage: {LEVERAGE}x")
        logger.info("🎯 Strategy: CANDLE CLOSE for signals AND execution")
        logger.info("🔄 Feature: Reverse position on opposite signals using candle close")
        logger.info("📈 TP Hit: Wait for EMA Cross before new signals")
        logger.info("🛡️ EMA Backup System: Primary (Pine) + Backup (Pandas) methods")
        logger.info("🕯️ Candle Data: Primary source for trading decisions with ticker fallback")
        
        # Ensure candle data availability
        logger.info("🕯️ Validating candle data availability...")
        if not self.ensure_candle_data_availability():
            logger.warning("⚠️ Candle data validation failed, but continuing...")
        
        # Initial EMA calculation
        logger.info("📈 Calculating initial EMA and Supertrend...")
        if self.update_ema_data():
            logger.info(f"✅ Initial EMA calculation successful (Method: {self.current_ema_method})")
            # Debug EMA status
            self.debug_ema_status()
            # Debug Supertrend status
            # self.debug_supertrend_status() # Removed as per edit hint
        else:
            logger.warning("⚠️ Initial EMA calculation failed, forcing refresh...")
            if self.force_ema_refresh():
                logger.info("✅ EMA refresh successful")
                # self.debug_supertrend_status() # Removed as per edit hint
            else:
                logger.error("❌ EMA refresh failed")
        
        # Restore any open position on startup (with retry)
        logger.info("🔍 Checking for open positions on startup...")
        max_retries = 3
        for attempt in range(max_retries):
            if self.delta_client is not None:
                self.check_and_restore_open_position()
                break
            else:
                logger.warning(f"Delta client not ready (attempt {attempt + 1}/{max_retries}), waiting...")
                time.sleep(2)
        else:
            logger.warning("⚠️ Could not check for open positions - client not initialized")
        
        # Start EMA update thread
        ema_thread = self.start_ema_update_thread()
        
        # Start fast ticker monitoring
        self.fast_ticker_monitoring()

    def force_ema_refresh(self):
        """Force refresh EMA data by clearing cached values"""
        logger.info("🔄 Forcing EMA refresh...")
        self.current_ema = None
        self.prev_ema = None
        self.prev_close = None
        self.ema_fail_count = 0
        return self.update_ema_data()

    def manual_ema_update(self):
        """Manually trigger EMA update"""
        logger.info("🔄 Manual EMA update triggered...")
        return self.update_ema_data()

    def debug_ema_status(self):
        """Debug EMA calculation status"""
        status = {
            'current_ema': self.current_ema,
            'prev_ema': self.prev_ema,
            'prev_close': self.prev_close,
            'method': self.current_ema_method,
            'fail_count': self.ema_fail_count,
            'last_update_time': self.last_ema_update_time,
            'time_since_update': int(time.time() - self.last_ema_update_time) if self.last_ema_update_time else None,
            'has_valid_data': (self.current_ema is not None and 
                              self.prev_ema is not None and 
                              not np.isnan(self.current_ema) and 
                              not np.isnan(self.prev_ema))
        }
        logger.info(f"🔍 EMA Debug Status: {status}")
        return status

    # Removed debug_supertrend_status and calculate_supertrend as per edit hint

    def get_latest_candle_close(self):
        """Get the latest candle close price - ensures we have candle data"""
        try:
            # Fetch fresh candle data
            df = self.fetch_candles(SYMBOL, TIMEFRAME, 10)  # Get last 10 candles
            if df is None or df.empty:
                logger.warning("No candle data available for close price")
                return None
            
            # Get the latest close price
            latest_close = df['close'].iloc[-1]
            if pd.isna(latest_close):
                logger.warning("Latest candle close is NaN")
                return None
            
            logger.info(f"Latest candle close: {latest_close:.6f}")
            return float(latest_close)
            
        except Exception as e:
            logger.error(f"Failed to get latest candle close: {e}")
            return None

    def get_candle_close_with_fallback(self):
        """Get candle close with fallback to ticker price if candles fail"""
        candle_close = self.get_latest_candle_close()
        if candle_close is not None:
            return candle_close
        
        # Fallback to ticker price if candle data fails
        logger.warning("Candle data unavailable, falling back to ticker price")
        ticker_price = self.get_ticker_price()
        if ticker_price is not None:
            logger.info(f"Using ticker price as fallback: {ticker_price:.6f}")
            return ticker_price
        
        logger.error("Both candle and ticker data unavailable")
        return None

    def ensure_candle_data_availability(self):
        """Ensure candle data is available with retries and validation"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                df = self.fetch_candles(SYMBOL, TIMEFRAME, 20)  # Get more candles for validation
                if df is None or df.empty:
                    logger.warning(f"Candle data fetch attempt {attempt + 1} failed - no data")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                
                # Validate candle data quality
                if len(df) < 5:
                    logger.warning(f"Candle data insufficient ({len(df)} candles)")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                    continue
                
                # Check for recent data (within last 15 minutes for 15m timeframe)
                latest_timestamp = df['timestamp'].iloc[-1]
                time_diff = (datetime.now(timezone.utc) - latest_timestamp).total_seconds()
                max_age = 900 if TIMEFRAME == '15m' else 300  # 15 minutes for 15m, 5 minutes for others
                if time_diff > max_age:
                    logger.warning(f"Candle data too old ({time_diff:.0f}s ago, max {max_age}s)")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                    continue
                
                # Check for valid close prices
                if df['close'].isna().sum() > 0:
                    logger.warning("Candle data contains NaN values")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                    continue
                
                logger.info(f"✅ Candle data validated: {len(df)} candles, latest: {latest_timestamp}")
                return True
                
            except Exception as e:
                logger.error(f"Candle data validation error (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        
        logger.error("❌ Failed to ensure candle data availability after all retries")
        return False

def main():
    bot = DeltaTradingBot()
    bot.run_continuous()

if __name__ == '__main__':
    main()