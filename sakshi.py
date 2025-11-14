# --- START OF FILE sakshi.py (Updated for Multi-Instrument) ---

import os
import argparse
import pandas as pd
from datetime import datetime, time
import time as time_sleep
from breeze_connect import BreezeConnect
import traceback
from tqdm import tqdm
from trikal_helpers import get_monthly_expiry_for_date

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================
API_KEY = "6EdY48855hZq0m484243(2181jl06F38"
API_SECRET = "41`x8(9894&87s60CN2Y@4469B616K02"
STRIKES_ON_EITHER_SIDE = 5
# --- MODIFIED: Strike difference is now instrument-dependent ---
STRIKE_DIFFERENCE_MAP = {
    "NIFTY": 50,
    "CNXBAN": 100
}
API_DELAY_SECONDS = 0.5

# ==============================================================================
# --- HELPER FUNCTIONS ---
# ==============================================================================
def parse_breeze_response(response, contract_info=""):
    """Parses the response from the Breeze API, with added context for errors."""
    if response and response.get('Status') == 200 and 'Success' in response:
        success_data = response.get('Success')
        if isinstance(success_data, list):
            return success_data
        else:
            print(f"⚠️ 'Success' is not a list in response for {contract_info}: {response}")
            return []
    elif response and response.get('Status') != 200:
        print(f"❌ Breeze call failed for {contract_info}: Status {response.get('Status')} — {response.get('Error')}")
        return []
    else:
        print(f"⚠️ Invalid Breeze response for {contract_info}: {response}")
        return []

# ==============================================================================
# --- MAIN SAKSHI LOGIC ---
# ==============================================================================
def gather_data_for_date(breeze_api, date_str, instrument="NIFTY", force_overwrite=False):
    """Main function to fetch and save futures and options data."""
    
    print(f"--- GATHERING DATA FOR INSTRUMENT: {instrument.upper()} ---")
    
    if not force_overwrite:
        print("💡 Overwrite flag is OFF. Existing data files will be skipped.")

    # --- Create output directories based on the instrument name ---
    instrument_dir = instrument.lower()
    futures_dir = os.path.join(instrument_dir, "futures")
    options_dir = os.path.join(instrument_dir, "options_1s")
    os.makedirs(futures_dir, exist_ok=True)
    os.makedirs(options_dir, exist_ok=True)

    target_date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    
    print("⏳ Determining relevant monthly expiry...")
    expiry_date = get_monthly_expiry_for_date(target_date_obj, roll_on_expiry_day=True)
    if not expiry_date:
        print(f"❌ Could not determine a valid expiry date for {date_str}. Aborting.")
        return
    print(f"✅ Found Expiry: {expiry_date}")

    # --- Fetch Futures Data ---
    print(f"\n--- Fetching 1-min Futures Data for {date_str} ---")
    futures_filepath = os.path.join(futures_dir, f"FUT_{date_str}.csv")
    df_futures = None

    if not force_overwrite and os.path.exists(futures_filepath):
        print(f"✅ Futures file already exists. Skipping download. Loading from {futures_filepath}")
        df_futures = pd.read_csv(futures_filepath)
    else:
        try:
            from_date_str_fmt = f"{date_str}T09:00:00.000Z"
            to_date_str_fmt = f"{date_str}T16:00:00.000Z"
            
            response = breeze_api.get_historical_data_v2(
                interval="1minute", from_date=from_date_str_fmt, to_date=to_date_str_fmt,
                stock_code=instrument, # Use the instrument parameter
                exchange_code="NFO", product_type="futures", expiry_date=f"{expiry_date}T15:30:00.000Z",
                right="others", strike_price="0")
                
            futures_data = parse_breeze_response(response, f"{instrument} Futures")
            if futures_data:
                df_futures = pd.DataFrame(futures_data)
                df_futures = df_futures[['datetime', 'open', 'high', 'low', 'close', 'volume']]
                df_futures.to_csv(futures_filepath, index=False)
                print(f"✅ Futures data saved to {futures_filepath}")
        except Exception as e:
            print(f"❌ An error occurred during futures data fetch: {e}")
            traceback.print_exc()

    if df_futures is None or df_futures.empty:
        print("❌ Could not load or fetch futures data. Aborting options download.")
        return

    # --- Determine Strike Range ---
    strike_diff = STRIKE_DIFFERENCE_MAP.get(instrument, 50) # Get correct strike difference
    df_futures['datetime_obj'] = pd.to_datetime(df_futures['datetime'])
    first_candle = df_futures[df_futures['datetime_obj'].dt.time >= time(9, 15)].iloc[0]
    anchor_price = float(first_candle['open'])
    atm_strike = round(anchor_price / strike_diff) * strike_diff
    
    strikes_to_fetch = [atm_strike + (i * strike_diff) for i in range(-STRIKES_ON_EITHER_SIDE, STRIKES_ON_EITHER_SIDE + 1)]
    print(f"\nAnchor Price: {anchor_price:.2f} -> ATM Strike: {atm_strike}")
    print(f"Strikes to fetch: {strikes_to_fetch}")

    # --- Fetch 1-Second Options Data ---
    print(f"\n--- Fetching 1-sec Options Data for {len(strikes_to_fetch)*2} contracts ---")
    day_options_dir = os.path.join(options_dir, date_str)
    os.makedirs(day_options_dir, exist_ok=True)
    
    market_start_time = datetime.strptime(f"{date_str} 09:15:00", "%Y-%m-%d %H:%M:%S")
    market_end_time = datetime.strptime(f"{date_str} 15:30:00", "%Y-%m-%d %H:%M:%S")

    contracts_to_fetch = [(strike, right) for strike in strikes_to_fetch for right in ["call", "put"]]
    
    for strike, right in tqdm(contracts_to_fetch, desc="Fetching Contracts"):
        contract_filename = f"{right.upper()}_{strike}.csv"
        contract_filepath = os.path.join(day_options_dir, contract_filename)

        if not force_overwrite and os.path.exists(contract_filepath):
            continue

        all_chunks_for_contract = []
        current_time = market_start_time
        
        while current_time < market_end_time:
            chunk_end_time = current_time + pd.Timedelta(minutes=15)
            from_chunk_str = current_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            to_chunk_str = chunk_end_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

            try:
                response = breeze_api.get_historical_data_v2(
                    interval="1second", from_date=from_chunk_str, to_date=to_chunk_str,
                    stock_code=instrument, # Use the instrument parameter
                    exchange_code="NFO", product_type="options",
                    expiry_date=f"{expiry_date}T15:30:00.000Z",
                    right=right, strike_price=str(strike))
                    
                options_chunk = parse_breeze_response(response, f"{instrument} {strike} {right.upper()} {current_time.strftime('%H:%M')}")
                if options_chunk:
                    all_chunks_for_contract.extend(options_chunk)
                
                time_sleep.sleep(API_DELAY_SECONDS)
            except Exception as e:
                print(f"❌ Error fetching chunk for {strike} {right.upper()}: {e}")
            
            current_time = chunk_end_time

        if not all_chunks_for_contract:
            print(f"⚠️ No 1s data fetched for {strike} {right.upper()}. Skipping file creation.")
            continue
            
        df_contract = pd.DataFrame(all_chunks_for_contract)
        df_contract = df_contract[['datetime', 'open', 'high', 'low', 'close', 'volume']]
        df_contract.to_csv(contract_filepath, index=False)
        
    print(f"\n✅ All options data downloaded and saved into individual files in {day_options_dir}")
    print("\n--- Sakshi has borne witness. Data gathering complete. ---")

# ==============================================================================
# --- SCRIPT ENTRY POINT ---
# ==============================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sakshi: The Data Witness for Trikal's Backtesting.")
    parser.add_argument("--date", required=True, help="The date to fetch data for, in YYYY-MM-DD format.")
    parser.add_argument("--token", required=True, help="Your valid Breeze API session token.")
    # --- ADDED THIS ARGUMENT ---
    parser.add_argument("--instrument", default="NIFTY", help="The stock code to fetch (e.g., NIFTY, CNXBAN). Default: NIFTY.")
    parser.add_argument("--overwrite", action="store_true", help="Force overwrite of existing data files.")
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print("❌ Error: Date format is incorrect. Please use YYYY-MM-DD.")
        exit()

    try:
        print("Connecting to Breeze API...")
        breeze = BreezeConnect(api_key=API_KEY)
        breeze.generate_session(api_secret=API_SECRET, session_token=args.token)
        print("Breeze API session generated successfully.")

        gather_data_for_date(breeze, args.date, args.instrument, args.overwrite)

    except Exception as e:
        print(f"\n❌ A critical error occurred: {e}")
        traceback.print_exc()