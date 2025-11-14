# --- FILE: trikal_provider.py ---

import pandas as pd
from datetime import datetime, time, timedelta, date
import pytz
import threading
import time as time_sleep
import os
import traceback
from trikal_helpers import parse_breeze_response, robust_datetime_parser, get_monthly_expiry_for_date

class TrikalProvider:
    def __init__(self, mode, date_str=None, breeze_api=None, interval="1minute"):
        self.breeze = breeze_api
        self.mode = mode
        self.interval = interval
        self.ist_timezone = pytz.timezone("Asia/Kolkata")
        
        if not self.breeze: raise ValueError("Breeze API object must be provided.")
        
        self.live_data_cache = {}
        self.data_lock = threading.Lock()
        self.background_threads = []

        self.backtest_date_obj = None
        self.warmup_df = None
        self.day_feed_df = None
        self.options_1s_cache = {}
        
        if self.mode == 'backtest':
            if not date_str: raise ValueError("Date string needed for backtest mode.")
            self.backtest_date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            print(f"🗂️  TrikalProvider initialized in BACKTEST mode for date: {date_str}")
            self._fetch_backtest_warmup_data()
            self._load_backtest_day_feed(date_str)
        elif self.mode == 'live':
            print("📡 TrikalProvider initialized in LIVE mode.")
        else:
            raise ValueError(f"Invalid mode: {self.mode}")

    def _load_backtest_day_feed(self, date_str):
        futures_file = f"data/futures/FUT_{date_str}.csv"
        try:
            print(f"   └── [Provider] Loading day-feed data from: {futures_file}")
            df_day = pd.read_csv(futures_file)
            df_day["datetime"] = robust_datetime_parser(df_day["datetime"])
            df_day.set_index('datetime', inplace=True)
            
            start_of_day = self.ist_timezone.localize(datetime.combine(self.backtest_date_obj, time(9, 15)))
            end_of_day = self.ist_timezone.localize(datetime.combine(self.backtest_date_obj, time(15, 30)))
            
            self.day_feed_df = df_day[(df_day.index >= start_of_day) & (df_day.index <= end_of_day)]
                            
            print(f"   └── [Provider] ✅ Day-feed loaded and filtered to {len(self.day_feed_df)} candles for today.")
        except FileNotFoundError as e:
            print(f"❌ FATAL ERROR: Futures data file not found: {e.filename}.")
            raise e
            
    def _fetch_backtest_warmup_data(self):
        print("   └── [Provider] Fetching historical warm-up data from API...")
        
        to_date_obj = self.ist_timezone.localize(datetime.combine(self.backtest_date_obj, time(9, 15))) if self.mode == 'backtest' else datetime.now(self.ist_timezone)
        from_date_obj = to_date_obj - timedelta(days=40)
        from_date_str = from_date_obj.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        to_date_str = to_date_obj.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        
        expiry_date = get_monthly_expiry_for_date(to_date_obj.date())

        response = self.get_initial_historical_data(from_date_str, to_date_str, expiry_date)
            
        warmup_data = parse_breeze_response(response, "Warm-up Futures")
        if warmup_data:
            df_warmup = pd.DataFrame(warmup_data)
            df_warmup = df_warmup[['datetime', 'open', 'high', 'low', 'close', 'volume']]
            df_warmup["datetime"] = robust_datetime_parser(df_warmup["datetime"])
            df_warmup = df_warmup.drop_duplicates(subset=["datetime"]).sort_values("datetime").set_index("datetime")
            for col in ["high", "low", "close", "open", "volume"]: 
                df_warmup[col] = pd.to_numeric(df_warmup[col], errors='coerce')
            
            limit_date = self.backtest_date_obj if self.mode == 'backtest' else date.today()
            self.warmup_df = df_warmup[df_warmup.index.date < limit_date]
            
            print(f"   └── [Provider] ✅ Warm-up data fetched with {len(self.warmup_df)} candles.")
        else:
            print("   └── [Provider] ⚠️ No warm-up data received.")
            self.warmup_df = pd.DataFrame()

    def get_initial_warmup_data(self):
        return self.warmup_df.copy() if self.warmup_df is not None else pd.DataFrame()

    def get_day_data_feed(self):
        self.options_1s_cache.clear()
        return self.day_feed_df

    def get_live_ltp(self, expiry_date, right, strike_price):
        try:
            response = self.breeze.get_quotes(stock_code="NIFTY",
                                     exchange_code="NFO",
                                     product_type="options",
                                     expiry_date=f"{expiry_date}T06:00:00.000Z",
                                     right=right.lower(),
                                     strike_price=str(strike_price))
            
            if response and response.get('Status') == 200 and response.get('Success'):
                quote = response['Success'][0]
                ltp = float(quote.get('ltp'))
                ltt_str = quote.get('ltt', datetime.now(self.ist_timezone).strftime('%d-%b-%Y %H:%M:%S'))
                price_dict = {'close': ltp, 'high': ltp, 'low': ltp}
                return price_dict, ltt_str
        except Exception as e:
            print(f"❌ Error getting live LTP: {e}")
        return None, None
    
    def fetch_1s_options_data(self, expiry_date, right, strike, from_dt, to_dt):
        if self.mode != 'backtest':
            print("⚠️ fetch_1s_options_data is only configured for backtest mode.")
            return pd.DataFrame()
        try:
            date_str = from_dt.strftime('%Y-%m-%d')
            contract_name = f"{right.upper()}_{int(strike)}"
            
            if contract_name in self.options_1s_cache:
                df_contract = self.options_1s_cache[contract_name]
            else:
                file_path = os.path.join('data', 'options_1s', date_str, f'{contract_name}.csv')
                df_contract = pd.read_csv(file_path)
                
                df_contract['datetime'] = pd.to_datetime(df_contract['datetime'])
                df_contract['datetime'] = df_contract['datetime'].apply(
                    lambda dt: self.ist_timezone.localize(dt) if dt.tzinfo is None else dt.tz_convert(self.ist_timezone)
                )
                df_contract.set_index('datetime', inplace=True)
                
                self.options_1s_cache[contract_name] = df_contract

            df_slice = df_contract[(df_contract.index >= from_dt) & (df_contract.index < to_dt)].copy()
            
            if df_slice.empty:
                return pd.DataFrame()

            df_slice['datetime_str'] = df_slice.index.strftime('%H:%M:%S')
            for col in ['open', 'high', 'low', 'close']:
                df_slice[col] = pd.to_numeric(df_slice[col], errors='coerce')

            return df_slice

        except FileNotFoundError:
            print(f"   └── [Provider] ⚠️ WARNING: Data file not found for {contract_name} on {date_str}.")
            return pd.DataFrame()
        except Exception as e:
            print(f"   └── [Provider] ❌ An unexpected error occurred: {e}")
            return pd.DataFrame()

    def get_initial_historical_data(self, from_date, to_date, expiry_date):
        try:
            # Using the format that was proven to work for warm-up and historical calls.
            expiry_date_for_api = f"{expiry_date}T15:30:00.000Z"
            
            response = self.breeze.get_historical_data_v2(
                interval=self.interval, 
                from_date=from_date, 
                to_date=to_date, 
                stock_code="NIFTY",
                exchange_code="NFO", 
                product_type="futures",
                expiry_date=expiry_date_for_api,
                right="others",
                strike_price="0"
            )
            
            return response
        except Exception as e:
            # --- MODIFICATION START: Print a clean, user-friendly message ---
            print("   └── ❌ NETWORK ERROR: Could not connect to Breeze API to fetch historical data.")
            # We can optionally log the full error to a file for debugging if needed,
            # but we will not show it to the user.
            # For example:
            # with open("error_log.txt", "a") as f:
            #     f.write(f"{datetime.now()}: {traceback.format_exc()}\n")
            return None
            # --- MODIFICATION END ---
    
    def shutdown(self):
        if self.mode == 'live':
            print("\nShutting down background data fetchers...")
            for thread in self.background_threads:
                if thread.is_alive():
                    thread.join(timeout=3)
            print("✅ All threads shut down.")