# --- START OF FILE yuktidhar.py ---

from abc import ABC, abstractmethod
import pandas as pd
import pandas_ta as ta
from datetime import timedelta, time
from trikal_helpers import Trade, is_sideways_market # Removed STRATEGY_CONFIGS as it's unused
import numpy as np
from datetime import datetime

class BaseStrategy(ABC):
    def __init__(self, name, config):
        self.name = name
        self.config = config
        print(f"✅ Initialized Strategy: {self.name}")

    def add_indicators(self, df):
        raise NotImplementedError("Each strategy must implement its own add_indicators method.")

    @abstractmethod
    def check_entry(self, provider_obj, df, expiry_date, capital_config, current_timestamp): pass
    @abstractmethod
    def get_analysis_string(self, df, strategy_config, timezone): pass

class TrendPullbackStrategy(BaseStrategy):

    def add_indicators(self, df):
        df.ta.ema(length=self.config.get("SHORT_EMA_PERIOD", 20), append=True, talib=False)
        df.ta.ema(length=self.config.get("LONG_EMA_PERIOD", 50), append=True, talib=False)
        return df

    def get_analysis_string(self, df, strategy_config, timezone):
        long_ema_period = strategy_config.get("LONG_EMA_PERIOD", 50)
        short_ema_period = strategy_config.get("SHORT_EMA_PERIOD", 20)
        long_ema_col = f"EMA_{long_ema_period}"
        short_ema_col = f"EMA_{short_ema_period}"
        required_cols = [long_ema_col, short_ema_col]
        if any(col not in df.columns for col in required_cols) or df[required_cols].iloc[-1].isnull().any():
            return "Waiting for trend indicators to populate..."
        last_row = df.iloc[-1]
        is_bullish_regime = last_row['close'] > last_row[long_ema_col]
        regime_str = "Trend: Bullish 🟢" if is_bullish_regime else "Trend: Bearish 🔴"
        ema_50_val, ema_20_val = last_row[long_ema_col], last_row[short_ema_col]
        return f"{regime_str} | 50EMA: {ema_50_val:.2f} | 20EMA: {ema_20_val:.2f}"

    def check_entry(self, provider_obj, df, expiry_date, capital_config, current_timestamp):
        # --- FILTER 1: SIDEWAYS MARKET CHECK (FOR FLAT/CHOPPY CONDITIONS) ---
        if self.config.get("USE_SIDEWAYS_MARKET_FILTER", False):
            short_ema_period = self.config.get("SHORT_EMA_PERIOD", 20)
            long_ema_period = self.config.get("LONG_EMA_PERIOD", 50)
            
            if is_sideways_market(
                df=df,
                lookback=self.config.get("SIDEWAYS_LOOKBACK", 30),
                ema_short_col=f"EMA_{short_ema_period}",
                ema_long_col=f"EMA_{long_ema_period}",
                ema_diff_threshold=self.config.get("SIDEWAYS_EMA_DIFF_THRESHOLD", 0.0008),
                range_ratio_threshold=self.config.get("SIDEWAYS_RANGE_RATIO_THRESHOLD", 0.0015),
                crossover_threshold=self.config.get("SIDEWAYS_CROSSOVER_THRESHOLD", 4)
            ):
                return None, None

        # --- Data validation and window checks ---
        active_windows = self.config.get("ACTIVE_WINDOWS", [])
        current_time = current_timestamp.time()
        is_active_window = any(time.fromisoformat(start) <= current_time <= time.fromisoformat(end) for start, end in active_windows) if active_windows else True
        if not is_active_window: return None, None

        short_ema_period = self.config.get("SHORT_EMA_PERIOD", 20)
        long_ema_period = self.config.get("LONG_EMA_PERIOD", 50)
        short_ema_col, long_ema_col = f"EMA_{short_ema_period}", f"EMA_{long_ema_period}"
        required_cols = [short_ema_col, long_ema_col, 'ema_slope']
        if len(df) < long_ema_period + 3 or any(col not in df.columns for col in required_cols) or df[required_cols].iloc[-2:].isnull().values.any():
            return None, None
            
        reclaim_candle, undercut_candle = df.iloc[-1], df.iloc[-2]
        bias = None
        long_term_trend_slope = undercut_candle['long_ema_slope']
        is_bullish_regime = long_term_trend_slope > 0
        is_bearish_regime = long_term_trend_slope < 0
        
        short_term_momentum_slope = undercut_candle['ema_slope']

        if is_bullish_regime:
            # --- FILTER 2: MOMENTUM ALIGNMENT ---
            if short_term_momentum_slope < 0:
                print(f"   └── Reversal Detected. Trend is Bullish, but 20-EMA slope ({short_term_momentum_slope:.2f}) is negative. No entry.")
                return None, None

            # Original pullback logic
            undercut_cond = undercut_candle['close'] < undercut_candle[short_ema_col]
            reclaim_cond = reclaim_candle['close'] > reclaim_candle[short_ema_col]
            if undercut_cond and reclaim_cond:
                bias = "bullish"
                print(f"   └── Bullish Signal Confirmed:")
                print(f"       ├── Trend Check:   Prev_Close ({undercut_candle['close']:.2f}) > Prev_50_EMA ({undercut_candle[long_ema_col]:.2f}) -> {is_bullish_regime}")
                print(f"       ├── Undercut Check: Prev_Close ({undercut_candle['close']:.2f}) < Prev_20_EMA ({undercut_candle[short_ema_col]:.2f}) -> {undercut_cond}")
                print(f"       └── Reclaim Check:  Close ({reclaim_candle['close']:.2f}) > 20_EMA ({reclaim_candle[short_ema_col]:.2f}) -> {reclaim_cond}")

        elif is_bearish_regime:
            # --- FILTER 2: MOMENTUM ALIGNMENT ---
            if short_term_momentum_slope > 0:
                print(f"   └── Reversal Detected. Trend is Bearish, but 20-EMA slope ({short_term_momentum_slope:.2f}) is positive. No entry.")
                return None, None

            # Original pullback logic
            overshoot_cond = undercut_candle['close'] > undercut_candle[short_ema_col]
            reject_cond = reclaim_candle['close'] < reclaim_candle[short_ema_col]
            if overshoot_cond and reject_cond:
                bias = "bearish"
                print(f"   └── Bearish Signal Confirmed:")
                print(f"       ├── Trend Check:   Prev_Close ({undercut_candle['close']:.2f}) < Prev_50_EMA ({undercut_candle[long_ema_col]:.2f}) -> {is_bearish_regime}")
                print(f"       ├── Overshoot Check: Prev_Close ({undercut_candle['close']:.2f}) > Prev_20_EMA ({undercut_candle[short_ema_col]:.2f}) -> {overshoot_cond}")
                print(f"       └── Reject Check:  Close ({reclaim_candle['close']:.2f}) < 20_EMA ({reclaim_candle[short_ema_col]:.2f}) -> {reject_cond}")
        
        if not bias: return None, None
        
        opt_type, right_str = ("C", "call") if bias == "bullish" else ("P", "put")
        future_close, traded_strike = reclaim_candle["close"], round(reclaim_candle["close"] / 50) * 50
        
        price_data, entry_time_str, options_block_df = None, None, pd.DataFrame()
        if provider_obj.mode == 'backtest':
            options_block_df = provider_obj.fetch_1s_options_data(expiry_date, right_str, traded_strike, current_timestamp, current_timestamp + timedelta(minutes=1))
            if options_block_df.empty: return None, None
            first_candle = options_block_df.iloc[0]
            price_data, entry_time_str = {'close': first_candle['close']}, first_candle['datetime_str']
        elif provider_obj.mode == 'live':
            price_data, entry_time_str = provider_obj.get_live_ltp(expiry_date, right_str, traded_strike)
        
        if not price_data: return None, None
        entry_price = price_data.get('close')
        if not entry_price or entry_price <= 0: return None, None
        
        # --- THIS IS THE NEW DYNAMIC LOT SIZE LOGIC ---
        lot_size = 0
        try:
            cutoff_date_str = capital_config.get("LOT_SIZE_CHANGE_DATE", "2025-10-28")
            cutoff_date = datetime.strptime(cutoff_date_str, "%Y-%m-%d").date()
            
            # expiry_date is a string 'YYYY-MM-DD' passed from the engine
            trade_expiry_date = datetime.strptime(expiry_date, "%Y-%m-%d").date()

            if trade_expiry_date > cutoff_date:
                lot_size = capital_config.get("NIFTY_LOT_SIZE_NEW", 65)
                print(f"   └── Using NEW lot size: {lot_size} for expiry {expiry_date}")
            else:
                lot_size = capital_config.get("NIFTY_LOT_SIZE", 75)
        except (ValueError, KeyError) as e:
            print(f"   └── ⚠️  Could not determine dynamic lot size, falling back to default. Error: {e}")
            lot_size = capital_config.get("NIFTY_LOT_SIZE", 75)
        # --- END OF NEW LOGIC ---

        if lot_size == 0:
            print("   └── ❌ Lot size is zero. Aborting entry.")
            return None, None
        
        cost_of_one_lot = entry_price * lot_size
        if cost_of_one_lot == 0 or cost_of_one_lot > capital_config["TOTAL_CAPITAL"]: return None, None
        num_lots = int(capital_config["TOTAL_CAPITAL"] // cost_of_one_lot)
        if num_lots == 0: return None, None
        active_trade_qty = num_lots * lot_size
        capital_employed = active_trade_qty * entry_price

        # --- CORRECTED TIME-BASED LOGIC APPLIED TO THIS STRATEGY ---
        afternoon_start_str = self.config.get("AFTERNOON_SESSION_START_TIME", "14:30")
        afternoon_start_time = time.fromisoformat(afternoon_start_str)
        
        is_afternoon_session = current_timestamp.time() >= afternoon_start_time
        entry_reason_suffix = ""

        if is_afternoon_session:
            print("   └── 🕑 Afternoon session rules applied.")
            stop_loss_pct = self.config.get("AFTERNOON_FIXED_STOP_LOSS_PCT")
            target_pct = self.config.get("AFTERNOON_FIXED_TARGET_PCT")
            entry_reason_suffix = " (Afternoon)"
        else:
            stop_loss_pct = self.config.get("FIXED_STOP_LOSS_PCT")
            target_pct = self.config.get("FIXED_TARGET_PCT")

        # Safety check to ensure parameters were loaded correctly
        if stop_loss_pct is None or target_pct is None:
            print("   └── ❌ ERROR: SL/TP parameters not found in config. Aborting entry.")
            return None, None
        
        # This part now uses the dynamically selected SL/TP percentages
        stoploss_price, target_price = entry_price * (1 - stop_loss_pct), entry_price * (1 + target_pct)
        
        contract_name = f"NIFTY{expiry_date.replace('-', '')[-6:]}{opt_type}{int(traded_strike)}"
        # Append the suffix to the entry reason for clear logging
        entry_reason = f"{bias.capitalize()} Trend Pullback{entry_reason_suffix}"
        
        new_trade = Trade(position_type='LONG',contract=contract_name, opt_type=opt_type, strike=traded_strike, qty=active_trade_qty,
                          entry_price=entry_price, entry_time=current_timestamp, entry_time_str=entry_time_str,
                          entry_reason=entry_reason, stoploss_price=stoploss_price, target_price=target_price,
                          future_price_at_entry=future_close, capital_employed=capital_employed,
                          strategy_config=self.config)
        return new_trade, options_block_df

