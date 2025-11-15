# --- START OF FILE trikal_engine.py (Your Version + Final Dual-Loop Implementation) ---

import pandas as pd
from datetime import datetime, date, timedelta
from typing import Optional, Iterator, Tuple
import threading
import time
import os

from trikal_provider import TrikalProvider
from yuktidhar import BaseStrategy
from trikal_helpers import (
    Trade, LIVE_BOT_CONFIG, ist_timezone, apply_indicators_and_bias,
    handle_exit_logic,  write_chart_data, is_trade_restricted,
    MANUAL_TRIGGER_FILE
)

class TrikalEngine:
    def __init__(self, strategy: BaseStrategy, provider: TrikalProvider, capital_config: dict, expiry_date: str, interval_minutes: int = 1, instance_name: str = "default"):
        print(f"⚙️  Initializing TrikalEngine for instance: '{instance_name}'...")
        self.strategy = strategy
        self.provider = provider
        self.capital_config = capital_config
        self.expiry_date = expiry_date
        self.interval_minutes = interval_minutes
        self.instance_name = instance_name
        self.active_trade: Optional[Trade] = None
        self.df_fut_history: Optional[pd.DataFrame] = None
        
        self.run_date = provider.backtest_date_obj if provider.mode == 'backtest' else date.today()
        run_date = self.run_date
        self.no_entry_time = ist_timezone.localize(datetime.combine(run_date, datetime.min.time())).replace(hour=LIVE_BOT_CONFIG["NO_ENTRY_AFTER"][0], minute=LIVE_BOT_CONFIG["NO_ENTRY_AFTER"][1])
        self.square_off_time = ist_timezone.localize(datetime.combine(run_date, datetime.min.time())).replace(hour=LIVE_BOT_CONFIG["SQUARE_OFF_TIME"][0], minute=LIVE_BOT_CONFIG["SQUARE_OFF_TIME"][1])

    def _prepare_warmup_data(self):
        if self.provider.mode == 'backtest':
            self.df_fut_history = self.provider.get_initial_warmup_data()
        else:
            from trikal_helpers import load_and_prepare_data
            self.df_fut_history = load_and_prepare_data(self.provider, self.expiry_date, 'live')
        
        if self.df_fut_history is None or self.df_fut_history.empty:
            raise RuntimeError("❌ Could not load warm-up data.")
        
        if self.interval_minutes > 1:
            resample_rules = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
            self.df_fut_history = self.df_fut_history.resample(f'{self.interval_minutes}min').apply(resample_rules).dropna()
            
        self.df_fut_history['volume'] = self.df_fut_history['volume'].where(self.df_fut_history['volume'] >= 0).ffill().fillna(0)
        self.df_fut_history = apply_indicators_and_bias(self.df_fut_history, self.strategy)
        print(f"✅ Warm-up complete. Initialized with {len(self.df_fut_history)} historical candles.")

    def run(self, data_iterator: Iterator[Tuple[datetime, pd.DataFrame]]):
        print(f"🚀 TrikalEngine starting in [{self.provider.mode.upper()}] mode on a {self.interval_minutes}-minute timeframe.")
        self._prepare_warmup_data()

        if self.provider.mode == 'live':
            print("🚀 Starting 30-second live trade manager loop...")
            self._stop_requested = False
            self.trade_manager_thread = threading.Thread(target=self._live_trade_manager_loop, daemon=True)
            self.trade_manager_thread.start()

        one_min_buffer = []
        try:
            for candle_start_time, fut_candle_row in data_iterator:
                if self.interval_minutes == 1:
                    self._process_candle(candle_start_time, fut_candle_row)
                else:
                    one_min_buffer.append(fut_candle_row)
                    if (candle_start_time.minute + 1) % self.interval_minutes == 0:
                        if not one_min_buffer: continue
                        df_1min = pd.concat(one_min_buffer)
                        one_min_buffer.clear()
                        resample_rules = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
                        df_resampled = df_1min.resample(f'{self.interval_minutes}min', label='right').apply(resample_rules).dropna()
                        if not df_resampled.empty:
                            new_candle = df_resampled.iloc[[0]]
                            new_timestamp = new_candle.index[0] - timedelta(minutes=self.interval_minutes)
                            self._process_candle(new_timestamp, new_candle)
        except KeyboardInterrupt:
            print(f"\nGracefully shutting down {self.provider.mode} engine...")
        finally:
            self._cleanup()

    # --- In trikal_engine.py, replace the entire function ---

    def _live_trade_manager_loop(self):
        while not getattr(self, '_stop_requested', True):
            try:
                # Priority 1: Check for manual override file
                if self.active_trade and os.path.exists(MANUAL_TRIGGER_FILE):
                    print(f"   └── [LIVE FAST LOOP] Manual trigger file detected! Executing square-off...")
                    self._execute_manual_exit("Manual Override")
                    continue 
                
                if self.active_trade:
                    trade = self.active_trade

                    # --- PRIORITY 2: RESTORED ROBUST STATE SYNCHRONIZATION ---
                    # This uses get_order_list, which is the ultimate source of truth for executions.
                    try:
                        now_time = datetime.now(ist_timezone)
                        from_date_str = (now_time - timedelta(days=1)).strftime("%Y-%m-%dT06:00:00.000Z")
                        to_date_str = now_time.strftime("%Y-%m-%dT18:00:00.000Z")
                        order_list = self.provider.breeze.get_order_list(
                            exchange_code="NFO", from_date=from_date_str, to_date=to_date_str
                        )
                        if order_list and order_list.get('Success'):
                            for order in reversed(order_list['Success']):
                                # This complex check correctly identifies our specific exit order.
                                is_our_trade = (
                                    order.get('status', '').lower() == 'executed' and
                                    order.get('action', '').lower() == 'sell' and
                                    str(int(order.get('strike_price', 0))) == str(int(trade.strike)) and
                                    order.get('right', '').lower() == ('call' if trade.opt_type == 'C' else 'put') and
                                    str(order.get('quantity')) == str(trade.qty) and
                                    pd.to_datetime(order['order_datetime']).tz_localize(ist_timezone) > trade.entry_time
                                )
                                if is_our_trade:
                                    exit_price = float(order['average_price'])
                                    exit_time = pd.to_datetime(order['order_datetime']).tz_localize(ist_timezone)
                                    # Determine reason based on price
                                    exit_reason = "GTT Target Hit" if exit_price >= trade.target_price else "GTT SL Hit"
                                    if trade.is_winner_mode_active and exit_price <= trade.stoploss_price:
                                        exit_reason = "Winner Mode SL"
                                    
                                    print(f"   └── ✅ [LIVE FAST LOOP] Exit Confirmed via Order List! Reason: {exit_reason}")
                                    handle_exit_logic(trade, exit_reason, exit_price, exit_time, exit_time.strftime('%H:%M:%S'))
                                    self._cleanup_after_exit()
                                    break # Exit the for loop
                    except Exception as e:
                        print(f"   └── ⚠️ [LIVE FAST LOOP] Could not get order list for sync: {e}")
                    
                    # --- START: NEW 30-SECOND ASYMMETRIC MOMENTUM EXIT LOGIC ---
                    
                    # If the trade was closed by the GTT check above, self.active_trade will be None.
                    # This check ensures we don't proceed if the trade is already closed.
                    if not self.active_trade:
                        continue

                    # A. Get the REAL-TIME Option Price (LTP)
                    right_str = 'call' if trade.opt_type == 'C' else 'put'
                    ltp_data, _ = self.provider.get_live_ltp(self.expiry_date, right_str, trade.strike)
                    
                    if not ltp_data:
                        # If we can't get the price, skip this check for this loop iteration
                        time.sleep(30)
                        continue
                    
                    current_ltp = ltp_data['close']
                    
                    # B. Determine REAL Profit/Loss State
                    is_in_profit = current_ltp > trade.entry_price
                    
                    # C. Get the latest 5-minute candle with all indicators
                    latest_candle = self.df_fut_history.iloc[-1]
                    exit_reason = None

                    if is_in_profit:
                        # PATIENT CHECK (for winning trades)
                        if 'EMA_50' in latest_candle and pd.notna(latest_candle['EMA_50']):
                            if (trade.opt_type == 'C' and latest_candle['close'] < latest_candle['EMA_50']) or \
                            (trade.opt_type == 'P' and latest_candle['close'] > latest_candle['EMA_50']):
                                exit_reason = "Momentum Exit (Profit): Breached 50EMA"
                    else: # If trade is in a loss
                        # IMPATIENT CHECKS (for losing trades)
                        if 'ema_slope' in latest_candle and pd.notna(latest_candle['ema_slope']):
                            if (trade.opt_type == 'C' and latest_candle['ema_slope'] < 0 and latest_candle['ema_slope_p_value'] < 0.15) or \
                            (trade.opt_type == 'P' and latest_candle['ema_slope'] > 0 and latest_candle['ema_slope_p_value'] < 0.15):
                                exit_reason = "Momentum Exit (Loss): Slope Reversal"
                        
                        if not exit_reason and 'EMA_20' in latest_candle and pd.notna(latest_candle['EMA_20']):
                            if (trade.opt_type == 'C' and latest_candle['close'] < latest_candle['EMA_20']) or \
                            (trade.opt_type == 'P' and latest_candle['close'] > latest_candle['EMA_20']):
                                exit_reason = "Momentum Exit (Loss): Breached 20EMA"
                    
                    if exit_reason:
                        print(f"   └── ⚠️ [LIVE FAST LOOP] ASYMMETRIC MOMENTUM EXIT! Reason: {exit_reason}")
                        self._execute_manual_exit(exit_reason)
                        continue # Stop processing this loop after exit

                    # --- END: NEW 30-SECOND MOMENTUM LOGIC ---

                    # Priority 4: Manage "Ride the Winner" GTT modification
                    use_winner_mode = self.strategy.config.get("USE_RIDE_WINNER_MODE", False)
                    if use_winner_mode and not trade.is_winner_mode_active:
                        trigger_pct = self.strategy.config.get("RIDE_WINNER_TRIGGER_PCT", 0.90)
                        trigger_price = trade.entry_price * (1 + trigger_pct)
                        if current_ltp >= trigger_price:
                            print(f"   └── [LIVE FAST LOOP] RIDE WINNER TRIGGERED! Modifying GTT...")
                            self._modify_gtt_for_winner_mode(trade)

            except Exception as e:
                print(f"   └── ❌ Error in live trade manager loop: {e}")
            time.sleep(30)

    def _modify_gtt_for_winner_mode(self, trade: Trade):
        try:
            new_target_pct = self.strategy.config.get("RIDE_WINNER_NEW_TARGET_PCT", 0.30)
            profit_lock_pct = self.strategy.config.get("RIDE_WINNER_PROFIT_LOCK_PCT", 0.10)
            new_target_price = trade.entry_price * (1 + new_target_pct)
            new_stop_price = trade.entry_price * (1 + profit_lock_pct)
            
            res = self.provider.breeze.gtt_three_leg_modify_order(
                exchange_code="NFO", gtt_order_id=trade.gtt_order_id, gtt_type="oco",
                order_details=[
                    {"gtt_leg_type": "target", "action": "sell", "limit_price": str(round(new_target_price*0.99, 1)), "trigger_price": str(round(new_target_price, 1))},
                    {"gtt_leg_type": "stoploss", "action": "sell", "limit_price": str(round(new_stop_price*0.99, 1)), "trigger_price": str(round(new_stop_price, 1))},
                ]
            )
            print(f"      └── Modify GTT Response: {res}")
            if res and res.get('Success'):
                trade.target_price = new_target_price
                trade.stoploss_price = new_stop_price
                trade.is_winner_mode_active = True
                print("      └── ✅ GTT Modified Successfully for Ride Winner Mode!")
        except Exception as e:
            print(f"      └── ❌ Failed to modify GTT for winner mode: {e}")

    def _process_candle(self, candle_start_time: datetime, fut_candle_row: pd.DataFrame):
        self.df_fut_history = pd.concat([self.df_fut_history, fut_candle_row])
        self.df_fut_history = self.df_fut_history[~self.df_fut_history.index.duplicated(keep='last')]
        self.df_fut_history = apply_indicators_and_bias(self.df_fut_history, self.strategy)
        latest_candle_with_indicators = self.df_fut_history.iloc[[-1]]
        write_chart_data(latest_candle_with_indicators, self.instance_name, self.provider.mode)
        
        close_price = self.df_fut_history.iloc[-1]['close']
        time_range_str = f"{candle_start_time.strftime('%H:%M')}-{(candle_start_time + timedelta(minutes=self.interval_minutes)).strftime('%H:%M')}"
        analysis_string = self.strategy.get_analysis_string(self.df_fut_history, self.strategy.config, ist_timezone)
        print(f"[{time_range_str}] Close Price: {close_price:<8.2f} Analysis: {analysis_string}")

        if self.active_trade:
            self._handle_price_based_exits(candle_start_time)
        if not self.active_trade:
             self._check_for_entry(candle_start_time + timedelta(minutes=self.interval_minutes, seconds=3))
    
    def _check_for_entry(self, action_timestamp: datetime):
        new_trade, options_data_block = self.strategy.check_entry(
            self.provider, self.df_fut_history, self.expiry_date, self.capital_config, action_timestamp
        )
        if new_trade:
            self._handle_entry_signal(new_trade, options_data_block)

    def _handle_entry_signal(self, new_trade: Trade, options_data_block: Optional[pd.DataFrame]):
        if os.path.exists(MANUAL_TRIGGER_FILE):
            try:
                os.remove(MANUAL_TRIGGER_FILE)
            except (FileNotFoundError, OSError) as e:
                print(f"   └── ⚠️  Error removing trigger file: {e}")

        self.active_trade = new_trade
        self.active_trade.instance_name = self.instance_name
        self.active_trade.mode = self.provider.mode

        log_entry_time = self.active_trade.entry_time_str.split(' ')[-1] if ' ' in self.active_trade.entry_time_str else self.active_trade.entry_time_str
        print(f"[{log_entry_time}] 🔔 SIGNAL | {self.active_trade.contract} | Qty: {self.active_trade.qty} at ~{self.active_trade.entry_price:.2f} | SL: {self.active_trade.stoploss_price:.2f} | TP: {self.active_trade.target_price:.2f}")

        if self.provider.mode == 'live':
            print("   └── [LIVE] Placing 3-leg GTT order...")
            try:
                sl_trigger = round(self.active_trade.stoploss_price, 1)
                sl_limit = round(sl_trigger * 0.99, 1)
                target_trigger = round(self.active_trade.target_price, 1)
                target_limit = round(target_trigger * 0.99, 1)
                entry_limit_price = round(self.active_trade.entry_price * 1.01, 1)
                expiry_date_api_format = f"{self.expiry_date}T06:00:00.000Z"
                
                res = self.provider.breeze.gtt_three_leg_place_order(
                    exchange_code="NFO", stock_code="NIFTY", product="options",
                    quantity=str(self.active_trade.qty), expiry_date=expiry_date_api_format,
                    right="call" if self.active_trade.opt_type == 'C' else "put",
                    strike_price=str(int(self.active_trade.strike)), gtt_type="cover_oco",
                    fresh_order_action="buy", fresh_order_price=str(entry_limit_price),
                    fresh_order_type="limit", index_or_stock="index",
                    trade_date=datetime.now(ist_timezone).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    order_details=[
                        {"gtt_leg_type": "target", "action": "sell", "limit_price": str(target_limit), "trigger_price": str(target_trigger)},
                        {"gtt_leg_type": "stoploss", "action": "sell", "limit_price": str(sl_limit), "trigger_price": str(sl_trigger)},
                    ]
                )
                if res and res.get('Success') and res.get('Success').get('gtt_order_id'):
                    self.active_trade.gtt_order_id = res['Success']['gtt_order_id']
                    print(f"   └── ✅ [LIVE] GTT Order Placed Successfully! Main ID: {self.active_trade.gtt_order_id}")
                else:
                    print(f"   └── ❌ [LIVE] GTT Order Placement FAILED. Response: {res}")
                    self.active_trade = None
            except Exception as e:
                print(f"   └── ❌ [LIVE] An exception occurred during GTT order placement: {e}")
                self.active_trade = None

        elif self.provider.mode == 'backtest':
            self._iterate_and_check_exits(options_data_block)
    
    def _handle_price_based_exits(self, candle_start_time: datetime):
        if not self.active_trade: return

        # In live mode, this function handles EOD and momentum exits. GTT is in the fast loop.
        if self.provider.mode == 'live':
            now_time = datetime.now(ist_timezone)
            if now_time >= self.square_off_time:
                print(f"   └── [LIVE EOD] Squaring off position...")
                self._execute_manual_exit("Square-Off")
                return
        
        # --- START: Backtest Logic Restructuring ---
        if self.provider.mode == 'backtest':
            action_timestamp = candle_start_time + timedelta(minutes=self.interval_minutes)

            # Priority 1: Check for End-of-Day square-off first.
            if action_timestamp >= self.square_off_time:
                current_ltp = self.active_trade.last_known_price
                exit_time_str = action_timestamp.strftime('%H:%M:%S')
                handle_exit_logic(self.active_trade, "Square-Off", current_ltp, action_timestamp, exit_time_str)
                self._cleanup_after_exit()
                return

             # Priority 2: Run the high-frequency 1-second simulation which now contains ALL exit logic (SL, TP, and Momentum).
            start_of_period = max(candle_start_time, self.active_trade.entry_time)
            end_of_period = candle_start_time + timedelta(minutes=self.interval_minutes)
            right_str = 'call' if self.active_trade.opt_type == 'C' else 'put'
            options_block_df = self.provider.fetch_1s_options_data(
                self.expiry_date, right_str, self.active_trade.strike, start_of_period, end_of_period
            )
            self._iterate_and_check_exits(options_block_df)
            # If the 1s simulation resulted in an exit, the trade is now inactive. Stop processing.
            if not self.active_trade:
                return
        # --- END: Backtest Logic Restructuring ---
        
    def _execute_manual_exit(self, reason: str):
        if not self.active_trade or self.provider.mode != 'live':
            return
        
        # Store the trade object locally before we potentially clear it
        trade_to_exit = self.active_trade
        
        try:
            print(f"      └── Placing market square-off order for reason: {reason}...")
            # --- (API calls remain the same) ---
            res = self.provider.breeze.square_off(
                exchange_code="NFO", product="options", stock_code="NIFTY",
                expiry_date=f"{self.expiry_date}T06:00:00.000Z",
                right="call" if trade_to_exit.opt_type == 'C' else "put",
                strike_price=str(int(trade_to_exit.strike)),
                action="sell", order_type="market", validity="day",
                quantity=str(trade_to_exit.qty)
            )
            print(f"      └── Square-off response: {res}")
            
            if trade_to_exit.gtt_order_id:
                try:
                    print(f"      └── Cleaning up by cancelling GTT order ID: {trade_to_exit.gtt_order_id}...")
                    cancel_res = self.provider.breeze.gtt_three_leg_cancel_order(
                        exchange_code="NFO",
                        gtt_order_id=trade_to_exit.gtt_order_id
                    )
                    print(f"      └── GTT Cancel response: {cancel_res}")
                except Exception as e:
                    print(f"      └── ⚠️ [LIVE] Could not cancel GTT order. Please check manually. Error: {e}")

            # Log the successful exit
            now_time = datetime.now(ist_timezone)
            ltp_data, _ = self.provider.get_live_ltp(self.expiry_date, 'call' if trade_to_exit.opt_type == 'C' else 'put', trade_to_exit.strike)
            exit_price = ltp_data['close'] if ltp_data else trade_to_exit.entry_price
            
            handle_exit_logic(trade_to_exit, reason, exit_price, now_time, now_time.strftime('%H:%M:%S'))

        except Exception as e:
            print(f"      └── ❌ [LIVE] An exception occurred during forced exit: {e}")
            # Even if the exit fails, we should still log an attempt and clean up
            # so the bot doesn't get stuck. We can use last known price for logging.
            now_time = datetime.now(ist_timezone)
            handle_exit_logic(trade_to_exit, f"Manual Exit FAILED: {e}", trade_to_exit.last_known_price, now_time, now_time.strftime('%H:%M:%S'))

        finally:
            # --- THIS BLOCK IS THE CRITICAL FIX ---
            # It will run ALWAYS, whether the try block succeeded or failed.
            print(f"      └── Finalizing manual exit: Cleaning up state and trigger file.")
            
            # 1. Clean up the bot's internal state
            self._cleanup_after_exit() 
            
            # 2. Remove the trigger file so it doesn't run again
            try:
                if os.path.exists(MANUAL_TRIGGER_FILE):
                    os.remove(MANUAL_TRIGGER_FILE)
                    print(f"      └── Trigger file '{MANUAL_TRIGGER_FILE}' removed.")
            except (FileNotFoundError, OSError) as e:
                print(f"      └── ⚠️  Could not remove trigger file: {e}")
            # --- END OF FIX ---


    def _iterate_and_check_exits(self, options_df: Optional[pd.DataFrame]):
        if not self.active_trade or options_df is None or options_df.empty:
            return

        latest_5m_candle = self.df_fut_history.iloc[-1]
        
        # We only iterate over ticks where a trade actually occurred.
        valid_ticks_df = options_df[options_df['volume'] > 0].copy()
        if valid_ticks_df.empty:
            if self.active_trade:
                self.active_trade.last_known_price = options_df.iloc[-1]['close']
            return

        # Counter to trigger bot-side checks every 30 valid ticks.
        bot_check_counter = 0

        # The main loop iterates through EVERY valid tick.
        for tick in valid_ticks_df.itertuples(index=True, name='Tick'):
            opt_timestamp = tick.Index
            trade = self.active_trade
            
            if opt_timestamp <= trade.entry_time:
                continue

            # --- PRIORITY 1: BOT-SIDE DECISIONS (CHECKED EVERY 30 TICKS) ---
            bot_check_counter += 1
            if bot_check_counter >= 30:
                bot_check_counter = 0 # Reset the counter
                current_ltp = tick.close

                # A. Check for "Ride the Winner" activation
                use_winner_mode = self.strategy.config.get("USE_RIDE_WINNER_MODE", False)
                if use_winner_mode and not trade.is_winner_mode_active:
                    trigger_pct = self.strategy.config.get("RIDE_WINNER_TRIGGER_PCT")
                    if trigger_pct is not None:
                        trigger_price = trade.entry_price * (1 + trigger_pct)
                        if current_ltp >= trigger_price:
                            new_target_pct = self.strategy.config.get("RIDE_WINNER_NEW_TARGET_PCT")
                            profit_lock_pct = self.strategy.config.get("RIDE_WINNER_PROFIT_LOCK_PCT")
                            if new_target_pct is not None and profit_lock_pct is not None:
                                trade.target_price = trade.entry_price * (1 + new_target_pct)
                                trade.stoploss_price = trade.entry_price * (1 + profit_lock_pct)
                                trade.is_winner_mode_active = True
                                print(f"   └── [{opt_timestamp.strftime('%H:%M:%S')}] RIDE WINNER MODE ARMED! New TP: {trade.target_price:.2f}, New SL: {trade.stoploss_price:.2f}")

                # B. Check for Asymmetric Momentum Exits (Original "Impatient" Logic)
                is_in_profit = current_ltp > trade.entry_price
                exit_reason = None

                if is_in_profit:
                    if not trade.is_winner_mode_active:
                        if 'EMA_50' in latest_5m_candle and pd.notna(latest_5m_candle['EMA_50']):
                            if (trade.opt_type == 'C' and latest_5m_candle['close'] < latest_5m_candle['EMA_50']) or \
                            (trade.opt_type == 'P' and latest_5m_candle['close'] > latest_5m_candle['EMA_50']):
                                exit_reason = "Momentum Exit (Profit): Breached 50EMA"
                else: # If trade is in a loss
                    if 'ema_slope' in latest_5m_candle and pd.notna(latest_5m_candle['ema_slope']):
                        if (trade.opt_type == 'C' and latest_5m_candle['ema_slope'] < 0 and latest_5m_candle['ema_slope_p_value'] < 0.15) or \
                        (trade.opt_type == 'P' and latest_5m_candle['ema_slope'] > 0 and latest_5m_candle['ema_slope_p_value'] < 0.15):
                            exit_reason = "Momentum Exit (Loss): Slope Reversal"
                    
                    if not exit_reason and 'EMA_20' in latest_5m_candle and pd.notna(latest_5m_candle['EMA_20']):
                        if (trade.opt_type == 'C' and latest_5m_candle['close'] < latest_5m_candle['EMA_20']) or \
                        (trade.opt_type == 'P' and latest_5m_candle['close'] > latest_5m_candle['EMA_20']):
                            exit_reason = "Momentum Exit (Loss): Breached 20EMA"
                
                if exit_reason:
                    print(f"   └── ⚠️ [{opt_timestamp.strftime('%H:%M:%S')}] ASYMMETRIC MOMENTUM EXIT! Reason: {exit_reason}")
                    handle_exit_logic(trade, exit_reason, current_ltp, opt_timestamp, opt_timestamp.strftime('%H:%M:%S'))
                    self.active_trade = None
                    return

            # --- PRIORITY 2: BROKER-SIDE GTT SIMULATION (CHECKED ON EVERY TICK) ---
            if tick.low <= trade.stoploss_price:
                exit_reason = "SL Hit"
                if trade.is_winner_mode_active: exit_reason = "Winner Mode SL"
                handle_exit_logic(trade, exit_reason, trade.stoploss_price, opt_timestamp, opt_timestamp.strftime('%H:%M:%S'))
                self.active_trade = None
                return

            if tick.high >= trade.target_price:
                handle_exit_logic(trade, "Target Hit", trade.target_price, opt_timestamp, opt_timestamp.strftime('%H:%M:%S'))
                self.active_trade = None
                return

            # Update High/Low tracking
            trade.highest_ltp = max(trade.highest_ltp, tick.high)
            trade.lowest_ltp = min(trade.lowest_ltp, tick.low)
        
        if self.active_trade:
            self.active_trade.last_known_price = valid_ticks_df.iloc[-1].close        
            
    def _cleanup_after_exit(self):
        self.active_trade = None

    def _cleanup(self):
        if hasattr(self, 'trade_manager_thread') and self.trade_manager_thread.is_alive():
            print("🛑 Stopping live trade manager loop...")
            self._stop_requested = True
            self.trade_manager_thread.join(timeout=5)
        
        if self.active_trade:
            print("Engine shutting down with an active trade. This should not happen in a clean exit.")
        if self.provider:
            self.provider.shutdown()
        print("✅ Engine has stopped.")