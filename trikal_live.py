# --- START OF FILE trikal_live.py ---

import pandas as pd
from datetime import datetime, timedelta
import threading
import time as time_sleep
from typing import Iterator, Tuple, TYPE_CHECKING
import queue

from trikal_provider import TrikalProvider
from trikal_helpers import (
    robust_datetime_parser, ist_timezone, parse_breeze_response
)

if TYPE_CHECKING:
    from trikal_helpers import Trade

stop_event = threading.Event()

def get_next_run_time(interval_minutes=1):
    now = datetime.now(ist_timezone)
    next_run = now.replace(second=0, microsecond=0)
    minute_floor = (now.minute // interval_minutes) * interval_minutes
    next_run = next_run.replace(minute=minute_floor)
    while next_run <= now:
        next_run += timedelta(minutes=interval_minutes)
    return next_run

def live_data_generator(provider: TrikalProvider, expiry_date_str: str, interval_minutes: int = 1) -> Iterator[Tuple[datetime, pd.DataFrame]]:
    print(f"\n🚀 Live data source starting for {interval_minutes}-minute candles...")
    now = datetime.now(ist_timezone)
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

    if now < market_open:
        wait_seconds = (market_open - now).total_seconds()
        m, s = divmod(wait_seconds, 60)
        h, m = divmod(m, 60)
        print(f"Market opens at {market_open.strftime('%H:%M:%S')}. Waiting for {int(h)}h {int(m)}m {int(s)}s...")
        time_sleep.sleep(wait_seconds)
    elif now >= market_close:
        print("Market is already closed. Live data source will not produce any data."); return

    next_candle_time = get_next_run_time(interval_minutes)
    seen_candle_times = set()

    while not stop_event.is_set():
        now = datetime.now(ist_timezone)
        
        # --- MODIFICATION START: Graceful shutdown after market close ---
        if now >= market_close:
            print(f"[{now.strftime('%H:%M:%S')}] Market is now closed. Shutting down live data feed.")
            break
        # --- MODIFICATION END ---
        
        if now >= next_candle_time:
            candle_start_time = next_candle_time - timedelta(minutes=interval_minutes)

            if candle_start_time in seen_candle_times:
                next_candle_time += timedelta(minutes=interval_minutes)
                continue
            
            time_sleep.sleep(5)

            from_date_str = candle_start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            to_date_str = candle_start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            
            got_new_candle = False
            for poll_attempt in range(5):
                raw_res = provider.get_initial_historical_data(from_date_str, to_date_str, expiry_date_str)
                candle_list = parse_breeze_response(raw_res, f"{interval_minutes}-min Futures Poll")

                if candle_list:
                    matched_candle = None
                    for candle_data in reversed(candle_list):
                        candle_time = robust_datetime_parser(pd.Series([candle_data['datetime']]))[0]
                        if candle_time == candle_start_time:
                            matched_candle = candle_data
                            break
                    
                    if matched_candle:
                        new_candle_data = {
                            "open": float(matched_candle['open']), "high": float(matched_candle['high']),
                            "low": float(matched_candle['low']), "close": float(matched_candle['close']),
                            "volume": int(matched_candle['volume'])
                        }
                        new_row = pd.DataFrame(new_candle_data, index=[candle_start_time])
                        
                        yield candle_start_time, new_row
                        
                        seen_candle_times.add(candle_start_time)
                        got_new_candle = True
                        break
                
                time_sleep.sleep(5)

            if not got_new_candle:
                print(f"⚠️ No candle found for {candle_start_time.strftime('%H:%M')} after 5 attempts.")

            next_candle_time += timedelta(minutes=interval_minutes)
            
        time_sleep.sleep(3)