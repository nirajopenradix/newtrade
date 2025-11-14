# --- START OF FILE trikal.py ---

import argparse
from datetime import datetime, date
from breeze_connect import BreezeConnect
import traceback
import warnings

warnings.filterwarnings(
    'ignore',
    message='Converting to PeriodArray/Index representation will drop timezone information.',
    category=UserWarning
)

from trikal_engine import TrikalEngine
from trikal_backtest import backtest_data_generator
from trikal_live import live_data_generator
from trikal_provider import TrikalProvider
from yuktidhar import TrendPullbackStrategy
from trikal_helpers import get_monthly_expiry_for_date

# --- MODIFICATION: Centralized configuration for multiple bot instances ---
INSTANCE_CONFIG = {
    "niraj": {
        "API_KEY": "6EdY48855hZq0m484243(2181jl06F38",
        "API_SECRET": "41`x8(9894&87s60CN2Y@4469B616K02",
        "CAPITAL_CONFIG": {
            "TOTAL_CAPITAL": 60000.0,
            "NIFTY_LOT_SIZE": 75,
            "NIFTY_LOT_SIZE_NEW": 65,
            "LOT_SIZE_CHANGE_DATE": "2025-12-30",
            "FIXED_LOTS_TO_TRADE": 100 # This is currently not used but kept for future use
        }
    },
    "yash": {
        "API_KEY": "16nM2=7231E9*006247G30Q30$h%418!",
        "API_SECRET": "7651373b40_+6YU!0063LAh3254N0mYq",
        "CAPITAL_CONFIG": {
            "TOTAL_CAPITAL": 45000.0,
            "NIFTY_LOT_SIZE": 75,
            "NIFTY_LOT_SIZE_NEW": 65,
            "LOT_SIZE_CHANGE_DATE": "2025-12-30",
            "FIXED_LOTS_TO_TRADE": 100
        }
    },
    "akshay": {
        "API_KEY": "4X0095!%f83`D63L2V2144wS4907g6L1",
        "API_SECRET": "I41b85Nc16_4F989737s3e93(94i%tM7",
        "CAPITAL_CONFIG": {
            "TOTAL_CAPITAL": 45000.0,
            "NIFTY_LOT_SIZE": 75,
            "NIFTY_LOT_SIZE_NEW": 65,
            "LOT_SIZE_CHANGE_DATE": "2025-12-30",
            "FIXED_LOTS_TO_TRADE": 100
        }
    },
    "rajeev": {
        "API_KEY": "537A+59E6Ck02O0847991900Z898r7l1",
        "API_SECRET": "v6F21l5E652b652~87373740`92oF7TX",
        "CAPITAL_CONFIG": {
            "TOTAL_CAPITAL": 45000.0,
            "NIFTY_LOT_SIZE": 75,
            "NIFTY_LOT_SIZE_NEW": 65,
            "LOT_SIZE_CHANGE_DATE": "2025-12-30",
            "FIXED_LOTS_TO_TRADE": 100
        }
    }
}
# --- END MODIFICATION ---

STRATEGY_PARAMS = {
    "TrendPullback": {
        "LONG_EMA_PERIOD": 50,
        "SHORT_EMA_PERIOD": 20,
        "SLOPE_LOOKBACK_PERIOD": 20, 
        "FIXED_STOP_LOSS_PCT": 0.08,
        "FIXED_TARGET_PCT": 0.15,
        "BREAKEVEN_TRIGGER_PCT": 0.90,
        "BREAKEVEN_STOP_PCT": 0.01,
        "ACTIVE_WINDOWS": [("09:30", "15:00")],
        # -- Afternoon Session Parameters --
        "AFTERNOON_SESSION_START_TIME": "14:20", # The time when new rules apply
        "AFTERNOON_FIXED_STOP_LOSS_PCT": 0.05,   # Wider stop for volatility (e.g., 10%)
        "AFTERNOON_FIXED_TARGET_PCT": 0.05,
        
        # --- ADD THIS ENTIRE BLOCK ---
        # -- "Ride the Winner" Mode Configuration --
        "USE_RIDE_WINNER_MODE": True,    # Set to False to disable this feature
        "RIDE_WINNER_TRIGGER_PCT": 0.135, # Activate when 13.5% profit is reached
        "RIDE_WINNER_NEW_TARGET_PCT": 0.20, # New, higher target
        "RIDE_WINNER_PROFIT_LOCK_PCT": 0.10, # New stop-loss at +10% profit
        # --- END OF ADDITION ---
        
        # -- Sideways Market Filter (Still useful for flat/choppy markets) --
        "USE_SIDEWAYS_MARKET_FILTER": True,
        "SIDEWAYS_LOOKBACK": 30,
        "SIDEWAYS_EMA_DIFF_THRESHOLD": 0.0008,
        "SIDEWAYS_RANGE_RATIO_THRESHOLD": 0.0015,
        "SIDEWAYS_CROSSOVER_THRESHOLD": 4,
    }
}

STRATEGY_MAP = {
    "TrendPullback": TrendPullbackStrategy
}

def main():
    parser = argparse.ArgumentParser(description="Trikal: Unified Trading Engine")
    parser.add_argument("--date", help="Run in BACKTEST mode for a specific date (YYYY-MM-DD).")
    parser.add_argument("--token", required=True, help="Breeze API session token.")
    parser.add_argument(
        "--strategy",
        required=True,
        choices=STRATEGY_MAP.keys(),
        help="The name of the strategy to run."
    )
    # --- MODIFICATION: Added --instance argument to select the bot configuration ---
    parser.add_argument(
        "--instance",
        required=True,
        choices=INSTANCE_CONFIG.keys(),
        help="The bot instance configuration to run."
    )
    # --- END MODIFICATION ---

    args = parser.parse_args()

    # --- MODIFICATION: Select the entire config block based on the instance argument ---
    config = INSTANCE_CONFIG[args.instance]
    try:
        print(f"Connecting to Breeze API for instance: '{args.instance}'...")
        breeze = BreezeConnect(api_key=config["API_KEY"])
        breeze.generate_session(api_secret=config["API_SECRET"], session_token=args.token)
        print("Breeze API session generated successfully.")
    except Exception as e:
        print(f"❌ Failed to connect to Breeze API: {e}"); traceback.print_exc(); return
    # --- END MODIFICATION ---

    strategy_name = args.strategy
    strategy_class = STRATEGY_MAP[strategy_name]
    strategy_config = STRATEGY_PARAMS[strategy_name]
    strategy_to_run = strategy_class(name=strategy_name, config=strategy_config)
    print(f"✅ Strategy selected: {strategy_name}")

    if args.date:
        run_date_obj = datetime.strptime(args.date, "%Y-%m-%d").date()
        expiry_date = get_monthly_expiry_for_date(run_date_obj, roll_on_expiry_day=True)
        provider = TrikalProvider(mode='backtest', date_str=args.date, breeze_api=breeze, interval="1minute")
        # Pass the specific capital config and instance name to the engine
        engine = TrikalEngine(strategy_to_run, provider, config["CAPITAL_CONFIG"], expiry_date, interval_minutes=5, instance_name=args.instance)
        data_gen = backtest_data_generator(provider)
        engine.run(data_iterator=data_gen)
    else:
        today = date.today()
        expiry_date = get_monthly_expiry_for_date(today, roll_on_expiry_day=True)
        provider = TrikalProvider(mode='live', breeze_api=breeze, interval="1minute")
        # Pass the specific capital config and instance name to the engine
        engine = TrikalEngine(strategy_to_run, provider, config["CAPITAL_CONFIG"], expiry_date, interval_minutes=5, instance_name=args.instance)
        data_gen = live_data_generator(provider, expiry_date)
        engine.run(data_iterator=data_gen)

if __name__ == "__main__":
    main()