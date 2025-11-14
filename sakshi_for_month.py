# --- START OF FILE sakshi_for_month.py (Updated for Multi-Instrument) ---

import os
import argparse
import subprocess
import calendar
from datetime import date

# ==============================================================================
# --- SAKSHI HELPER LOGIC ---
# ==============================================================================

def get_trading_days_for_month(year, month):
    """Generates a list of all weekdays (Mon-Fri) for a given year and month."""
    cal = calendar.Calendar()
    month_days = cal.monthdatescalendar(year, month)
    trading_days = []
    for week in month_days:
        for day in week:
            if day.weekday() < 5 and day.month == month:
                trading_days.append(day)
    return trading_days

def run_batch_gather(year, month, session_token, instrument="NIFTY"):
    """Orchestrates the process of gathering data for an entire month."""
    trading_days = get_trading_days_for_month(year, month)
    
    if not trading_days:
        print(f"No trading days found for {year}-{month:02d}. Exiting.")
        return

    print(f"Found {len(trading_days)} potential trading days for {instrument} in {year}-{month:02d}.")
    print("Will now attempt to gather data for each day.")
    print("Existing files will be skipped.\n")
    
    instrument_dir = instrument.lower()

    for day in trading_days:
        date_str = day.strftime("%Y-%m-%d")
        
        # --- MODIFIED: Check in the correct instrument-specific directory ---
        futures_file = os.path.join(instrument_dir, "futures", f"FUT_{date_str}.csv")
        options_dir = os.path.join(instrument_dir, "options_1s", date_str)
        
        print(f"--- Processing Date: {date_str} for {instrument} ---")

        # A simplified check: if the futures file exists, assume the day is done.
        # This is faster than checking all individual option files.
        if os.path.exists(futures_file):
            print(f"✅ Futures data for {date_str} already exists. Skipping.\n")
            continue
            
        print(f"⏳ Data not found for {date_str}. Calling sakshi.py...")
        
        command = [
            "python3",
            "sakshi.py",
            "--date", date_str,
            "--token", session_token,
            "--instrument", instrument # Pass the instrument to the subprocess
        ]
        
        try:
            result = subprocess.run(command, check=True)
            print(f"✅ Successfully finished processing for {date_str}.\n")
        except FileNotFoundError:
            print("❌ Error: 'sakshi.py' not found in the current directory.")
            break
        except subprocess.CalledProcessError as e:
            print(f"\n❌ An error occurred while running sakshi.py for {date_str}.")
            print(f"   Exit code: {e.returncode}")
            print("Stopping the batch process. Please check the error above.\n")
            break

    print("--- Batch data gathering process finished. ---")

# ==============================================================================
# --- SCRIPT ENTRY POINT ---
# ==============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sakshi Helper: A batch data gatherer for Trikal's backtesting.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--year", required=True, type=int, help="The year to fetch data for (e.g., 2023).")
    parser.add_argument("--month", required=True, type=int, help="The month to fetch data for (e.g., 11 for November).")
    # --- ADDED THIS ARGUMENT ---
    parser.add_argument("--instrument", default="NIFTY", help="The stock code to fetch (e.g., NIFTY, CNXBAN). Default: NIFTY.")
    parser.add_argument("session_token", help="Your valid, single Breeze API session token.")

    args = parser.parse_args()
    
    if not (1 <= args.month <= 12):
        print("❌ Error: Month must be between 1 and 12.")
        exit()

    run_batch_gather(args.year, args.month, args.session_token, args.instrument)