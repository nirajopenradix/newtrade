# --- START OF FILE trikal_helpers.py ---

import os
import pytz
import pandas as pd
from datetime import datetime, date, timedelta
import csv
import calendar
from dataclasses import dataclass, field
from typing import Optional
# --- ADDED IMPORTS ---
import numpy as np
from scipy import stats
# --- END ADDED IMPORTS ---


# ==============================================================================
# --- TRADE STATE CLASS ---
# ==============================================================================
@dataclass
class Trade:
    position_type: str
    contract: str
    opt_type: str
    strike: float
    qty: int
    entry_price: float
    entry_time: datetime
    entry_time_str: str
    entry_reason: str
    stoploss_price: float
    target_price: float
    future_price_at_entry: float
    strategy_config: dict = field(default_factory=dict, repr=False)
    capital_employed: float = 0.0
    highest_ltp: float = field(init=False)
    lowest_ltp: float = field(init=False)
    last_known_price: float = field(init=False)
    last_known_price_timestamp: datetime = field(init=False)
    gtt_order_id: Optional[str] = None
    reversal_signal_count: int = 0
    # --- MODIFICATION: Added fields to track instance and mode for logging ---
    instance_name: str = "default"
    mode: str = "live"
    
    is_winner_mode_active: bool = False

    def __post_init__(self):
        self.highest_ltp = self.entry_price
        self.lowest_ltp = self.entry_price
        self.last_known_price = self.entry_price
        self.last_known_price_timestamp = self.entry_time

# ==============================================================================
# --- CONFIGURATIONS & HELPERS ---
# ==============================================================================
LIVE_BOT_CONFIG = {"NO_ENTRY_AFTER": (15, 00), "SQUARE_OFF_TIME": (15, 20)}
MANUAL_TRIGGER_FILE = "MANUAL_SQUARE_OFF.trigger"
ist_timezone = pytz.timezone("Asia/Kolkata")

def is_sideways_market(df, lookback, ema_short_col, ema_long_col, ema_diff_threshold, range_ratio_threshold, crossover_threshold):
    """
    Detects sideways/choppy market zones based on EMA convergence,
    range compression, and frequent crossovers.
    """
    if len(df) < lookback:
        return False # Not enough data to make a decision

    recent = df.tail(lookback)
    closes = recent["close"].to_numpy()
    
    # Ensure EMA columns exist and have data
    if ema_short_col not in recent.columns or ema_long_col not in recent.columns or \
       recent[ema_short_col].isnull().any() or recent[ema_long_col].isnull().any():
        return False

    ema_short = recent[ema_short_col].to_numpy()
    ema_long = recent[ema_long_col].to_numpy()

    mean_price = np.mean(closes)
    if mean_price == 0: return True # Avoid division by zero

    # 1. EMA convergence
    ema_gap_ratio = np.mean(np.abs(ema_short - ema_long) / mean_price)
    ema_flat = ema_gap_ratio < ema_diff_threshold

    # 2. Range compression
    range_ratio = (np.max(closes) - np.min(closes)) / mean_price
    low_volatility = range_ratio < range_ratio_threshold

    # 3. Frequent EMA crossover
    crossover_signs = np.sign(ema_short - ema_long)
    crossover_flips = np.sum(np.diff(crossover_signs) != 0)
    choppy = crossover_flips > crossover_threshold

    if (ema_flat and low_volatility) or choppy:
        reasons = []
        if ema_flat and low_volatility:
            reasons.append(f"Flat+LowVol({ema_gap_ratio:.4f}, {range_ratio:.4f})")
        if choppy:
            reasons.append(f"Choppy({crossover_flips} flips)")
        print(f"   └── Sideways Market Detected. Reason(s): {', '.join(reasons)}. No entry.")
        return True

    return False


def handle_exit_logic(trade: Trade, exit_reason: str, exit_price: float, exit_time: datetime, exit_time_str: str):
    final_high = max(trade.highest_ltp, trade.last_known_price)
    final_low = min(trade.lowest_ltp, trade.last_known_price)
    log_exit_time = exit_time_str.split(' ')[-1] if ' ' in exit_time_str else exit_time_str
    print(f"💰 EXIT at {log_exit_time} | {trade.contract} at {exit_price:.2f} | Reason: {exit_reason} | HLtp: {final_high:.2f}, LLtp: {final_low:.2f}")

    if trade.position_type == 'SHORT':
        gross_pnl = (trade.entry_price - exit_price) * trade.qty
    else:
        gross_pnl = (exit_price - trade.entry_price) * trade.qty

    charges = calculate_detailed_charges(trade.entry_price, exit_price, trade.qty)
    net_pnl = gross_pnl - charges
    csv_entry_time = trade.entry_time_str.split(' ')[-1] if ' ' in trade.entry_time_str else trade.entry_time_str
    csv_exit_time = exit_time_str.split(' ')[-1] if ' ' in exit_time_str else exit_time_str
    trade_log_data = {"TradeDate": exit_time.strftime('%Y-%m-%d'), "Contract": trade.contract, "Qty": trade.qty,
                      "EntryTime": csv_entry_time, "EntryPrice": f"{trade.entry_price:.2f}",
                      "HLtp": f"{final_high:.2f}", "LLtp": f"{final_low:.2f}",
                      "EntryReason": trade.entry_reason, "ExitTime": csv_exit_time,
                      "ExitPrice": f"{exit_price:.2f}", "ExitReason": exit_reason,
                      "NetPnL": f"{net_pnl:.2f}"}
    # --- MODIFICATION: Pass the instance_name and mode from the trade to the logger ---
    log_trade_to_csv(trade_log_data, trade.instance_name, trade.mode)

def robust_datetime_parser(series):
    def convert_element(dt_obj):
        if not isinstance(dt_obj, (str, pd.Timestamp)): return pd.NaT
        try:
            parsed_dt = pd.to_datetime(dt_obj, format="%Y-%m-%dT%H:%M:%S.000Z", utc=True)
            return parsed_dt.tz_convert(ist_timezone)
        except (ValueError, TypeError):
            try:
                parsed_dt = pd.to_datetime(dt_obj, format='%Y-%m-%d %H:%M:%S')
                if parsed_dt.tzinfo is None: return ist_timezone.localize(parsed_dt)
                else: return parsed_dt.tz_convert(ist_timezone)
            except: return pd.NaT
    return series.apply(convert_element)

def parse_breeze_response(response, contract_info=""):
    if not response or not isinstance(response, dict):
        print(f"⚠️ Invalid or empty Breeze response for {contract_info}. Response: {response}")
        return []
    if response.get('Status') == 200:
        if 'Success' in response:
            return response.get('Success', [])
        else:
            return []
    else:
        return []

def load_and_prepare_data(provider, expiry_date, mode, target_date_obj=None):
    if mode == 'backtest':
        to_date = ist_timezone.localize(datetime.combine(target_date_obj, datetime.min.time())).replace(hour=9, minute=15)
    else:
        to_date = datetime.now(ist_timezone)
    from_date = to_date - timedelta(days=40)
    from_date_str = from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    to_date_str = to_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    print(f"   └── Fetching warm-up data from {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d %H:%M:%S')}...")
    response = provider.get_initial_historical_data(from_date_str, to_date_str, expiry_date)
    fut_data_list = parse_breeze_response(response, "Initial Futures")
    if not fut_data_list: return None
    df_fut = pd.DataFrame(fut_data_list)
    df_fut = df_fut[['datetime', 'open', 'high', 'low', 'close', 'volume']]
    df_fut["datetime"] = robust_datetime_parser(df_fut["datetime"])
    df_fut.dropna(subset=["datetime"], inplace=True)
    df_fut = df_fut.drop_duplicates(subset=["datetime"]).sort_values("datetime").set_index("datetime")
    for col in ["high", "low", "close", "open", "volume"]:
        df_fut[col] = pd.to_numeric(df_fut[col], errors='coerce')
    print(f"✅ Initial data preparation complete. Loaded {len(df_fut)} warm-up candles.")
    return df_fut

# --- ADDED THIS ENTIRE FUNCTION ---
def rolling_ols_slope(series, lookback):
    """
    Calculates a statistically robust slope using Ordinary Least Squares regression.
    Also returns the p-value to measure the slope's significance.
    """
    # --- CHANGE 1: DO NOT reset the index here. Keep the original DatetimeIndex. ---
    # s = pd.Series(series).reset_index(drop=True) # <-- OLD LINE
    s = pd.Series(series) # <-- NEW LINE

    n = len(s)
    # Initialize with the original index
    out = {'slope': pd.Series(index=s.index, dtype=float), 
           'p_value': pd.Series(index=s.index, dtype=float)}
    
    # We need to iterate using integer positions, but access data using the series `s`
    series_values = s.values

    for t in range(lookback - 1, n):
        y = series_values[t - lookback + 1: t + 1]
        if np.any(np.isnan(y)):
            continue
        
        x = np.arange(lookback).astype(float)
        x_mean = x.mean()
        y_mean = y.mean()
        denom = np.sum((x - x_mean)**2)
        if denom == 0:
            continue
            
        beta = np.sum((x - x_mean) * (y - y_mean)) / denom
        alpha = y_mean - beta * x_mean
        residuals = y - (alpha + beta * x)
        dof = lookback - 2
        if dof <= 0:
            continue
            
        sigma2 = np.sum(residuals**2) / dof
        se_beta = np.sqrt(sigma2 / denom)
        t_stat = beta / (se_beta + 1e-12)
        pval = 2 * (1 - stats.t.cdf(abs(t_stat), df=dof))

        # --- CHANGE 2: Use .iloc to assign to the correct integer position in our output Series ---
        out['slope'].iloc[t] = beta
        out['p_value'].iloc[t] = pval

    # The returned DataFrame now has the correct DatetimeIndex
    return pd.DataFrame(out)# --- END OF ADDITION ---

# --- THIS FUNCTION HAS BEEN MODIFIED ---
def apply_indicators_and_bias(df, strategy):
    df = strategy.add_indicators(df.copy())
    
    # Calculate the statistically significant slope of the 20-EMA
    ema_short_period = strategy.config.get("SHORT_EMA_PERIOD", 20)
    ema_short_slope_lookback = strategy.config.get("SLOPE_LOOKBACK_PERIOD", 20)
    ema_short_col = f"EMA_{ema_short_period}"
    
    if ema_short_col in df.columns and not df[ema_short_col].isnull().all():
        ols_results_ema_short = rolling_ols_slope(df[ema_short_col], ema_short_slope_lookback)
        df['ema_slope'] = ols_results_ema_short['slope']
        df['ema_slope_p_value'] = ols_results_ema_short['p_value']

    # --- ADD THIS ENTIRE BLOCK ---
    # Calculate the statistically significant slope of the 50-EMA
    ema_long_period = strategy.config.get("LONG_EMA_PERIOD", 50)
    ema_long_slope_lookback = strategy.config.get("SLOPE_LOOKBACK_PERIOD", 20) # Can use the same lookback
    ema_long_col = f"EMA_{ema_long_period}"

    if ema_long_col in df.columns and not df[ema_long_col].isnull().all():
        ols_results_ema_long = rolling_ols_slope(df[ema_long_col], ema_long_slope_lookback)
        df['long_ema_slope'] = ols_results_ema_long['slope']
    # --- END OF ADDITION ---
            
    return df
# --- END OF MODIFICATION ---

def get_monthly_expiry_for_date(target_date: date, roll_on_expiry_day: bool = False) -> str:
    """
    Finds the monthly expiry for a given date.
    If roll_on_expiry_day is True, it returns the *next* month's expiry if target_date is an expiry day.
    """
    cutoff_date = date(2025, 9, 1)
    expiry_weekday = 1 if target_date >= cutoff_date else 3
    year, month = target_date.year, target_date.month
    last_day_of_month = calendar.monthrange(year, month)[1]

    # Find the expiry date for the target_date's month
    for day in range(last_day_of_month, 0, -1):
        d = date(year, month, day)
        if d.weekday() == expiry_weekday:
            # --- THIS IS THE NEW LOGIC ---
            # If today IS the expiry day and we want to roll, find the next month's expiry
            if target_date == d and roll_on_expiry_day:
                next_month_target = date(year, month + 1, 1) if month < 12 else date(year + 1, 1, 1)
                return get_monthly_expiry_for_date(next_month_target, roll_on_expiry_day=False) # Important: Don't roll recursively
            # --- END OF NEW LOGIC ---
            
            # If the expiry has already passed this month, find next month's
            if target_date > d:
                next_month_target = date(year, month + 1, 1) if month < 12 else date(year + 1, 1, 1)
                return get_monthly_expiry_for_date(next_month_target, roll_on_expiry_day=False)
            else:
                return d.strftime('%Y-%m-%d')
    return None


def is_trade_restricted(run_date: date, expiry_date_str: str, days_to_restrict: int = 5) -> bool:
    try:
        expiry_date = datetime.strptime(expiry_date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return True
    if run_date > expiry_date: return False
    restricted_dates = set()
    current_date_check = expiry_date
    days_counted = 0
    while days_counted < days_to_restrict:
        if current_date_check.weekday() < 5:
            restricted_dates.add(current_date_check)
            days_counted += 1
        current_date_check -= timedelta(days=1)
    return run_date in restricted_dates

def calculate_detailed_charges(buy_price, sell_price, qty):
    FLAT_BROKERAGE_PER_ORDER = 20.0; EXCHANGE_TXN_CHARGE_PERC = 0.00053; GST_PERC = 0.18
    SEBI_CHARGE_PERC = 0.000001; STT_SELL_PERC = 0.000625; STAMP_DUTY_PERC = 0.000003
    buy_value = buy_price * qty; sell_value = sell_price * qty; turnover = buy_value + sell_value
    brokerage = FLAT_BROKERAGE_PER_ORDER * 2; stt = sell_value * STT_SELL_PERC
    exchange_txn_charge = turnover * EXCHANGE_TXN_CHARGE_PERC; sebi_charge = turnover * SEBI_CHARGE_PERC
    stamp_duty = buy_value * STAMP_DUTY_PERC; gst = (brokerage + exchange_txn_charge + sebi_charge) * GST_PERC
    return brokerage + stt + exchange_txn_charge + sebi_charge + stamp_duty + gst

# --- MODIFICATION: Logging functions now use the 'mode' to determine the filename ---
def log_trade_to_csv(trade_log_data, instance_name="default", mode="live"):
    if mode == 'live':
        file_path = f'trades_summary_{instance_name}.csv'
    else:  # backtest mode
        file_path = 'trades_summary.csv'
    
    headers = ['TradeDate', 'Contract', 'Qty', 'EntryTime', 'EntryPrice', 'HLtp', 'LLtp', 'EntryReason', 'ExitTime', 'ExitPrice', 'ExitReason', 'NetPnL']
    file_exists = os.path.isfile(file_path)
    try:
        with open(file_path, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            if not file_exists: writer.writeheader()
            writer.writerow(trade_log_data)
    except IOError as e: print(f"❌ Error writing to trade summary CSV: {e}")

# In trikal_helpers.py

# --- THIS ENTIRE BLOCK IS REPLACED ---

# We are removing log_chart_data_to_csv and append_chart_data_to_csv
# and replacing them with a single, smarter function.

def write_chart_data(df_to_write: pd.DataFrame, instance_name="default", mode="live"):
    """
    Intelligently writes or appends chart data to the correct CSV file.
    - Creates the file with a header if it doesn't exist.
    - Appends data without the header if the file already exists.
    """
    if df_to_write.empty:
        return

    if mode == 'live':
        file_path = f'live_chart_data_{instance_name}.csv'
    else:  # backtest mode
        # In backtest mode, we will now intelligently append to a single, persistent file.
        file_path = 'live_chart_data.csv'

    try:
        # Check if the file exists to decide whether to write the header
        file_exists = os.path.isfile(file_path)
        
        # 'a' mode is for appending. `header=not file_exists` is the key.
        df_to_write.to_csv(file_path, mode='a', header=not file_exists, index=True, index_label='datetime')
        
        if not file_exists:
            print(f"📈 Created and wrote initial data to {file_path}")
        
    except IOError as e:
        print(f"❌ Error writing chart data to CSV: {e}")

# --- END OF REPLACEMENT ---
