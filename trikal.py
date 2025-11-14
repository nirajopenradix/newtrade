# --- START OF FILE trikal.py (Unified with Mantri Remote Control) ---

import argparse
import threading
import asyncio
import logging
from datetime import datetime, date
from breeze_connect import BreezeConnect
import traceback
import warnings

# --- NEW IMPORTS FOR TELEGRAM INTEGRATION ---
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

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

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================

# --- NEW: TELEGRAM MANTRI CONFIGURATION ---
# IMPORTANT: Fill in your Bot Token and Chat ID here.
TELEGRAM_CONFIG = {
    "BOT_TOKEN": "8371304783:AAEzsfjwYOtwmS33wOXNzk6wwH6uNtbDmVw",
    "CHAT_ID": "-1003220703549"
}
# --- END NEW CONFIGURATION ---

INSTANCE_CONFIG = {
    "niraj": {
        "API_KEY": "6EdY48855hZq0m484243(2181jl06F38",
        "API_SECRET": "41`x8(9894&87s60CN2Y@4469B616K02",
        "CAPITAL_CONFIG": {
            "TOTAL_CAPITAL": 60000.0,
            "NIFTY_LOT_SIZE": 75,
            "NIFTY_LOT_SIZE_NEW": 65,
            "LOT_SIZE_CHANGE_DATE": "2025-12-30",
            "FIXED_LOTS_TO_TRADE": 100
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
        "AFTERNOON_SESSION_START_TIME": "14:20",
        "AFTERNOON_FIXED_STOP_LOSS_PCT": 0.05,
        "AFTERNOON_FIXED_TARGET_PCT": 0.05,
        "USE_RIDE_WINNER_MODE": True,
        "RIDE_WINNER_TRIGGER_PCT": 0.135,
        "RIDE_WINNER_NEW_TARGET_PCT": 0.20,
        "RIDE_WINNER_PROFIT_LOCK_PCT": 0.10,
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

# --- NEW: GLOBAL VARIABLE TO SHARE THE ENGINE INSTANCE ---
trikal_engine_instance: TrikalEngine = None

# ==============================================================================
# --- NEW: TELEGRAM HANDLER LOGIC (FOR DIRECT CALLS) ---
# ==============================================================================

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

async def exit_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /exit command by directly calling the engine's exit function."""
    global trikal_engine_instance
    
    if not update.message or not update.message.chat:
        return

    chat_id = str(update.message.chat_id)
    if chat_id != TELEGRAM_CONFIG["CHAT_ID"]:
        logger.warning(f"Ignoring /exit command from unauthorized chat ID: {chat_id}")
        return

    logger.info("Received '/exit' command. Attempting direct manual exit...")

    if trikal_engine_instance and trikal_engine_instance.active_trade:
        try:
            # Run the synchronous exit function in a thread to avoid blocking the async loop
            await asyncio.to_thread(trikal_engine_instance._execute_manual_exit, "Manual Override (Telegram)")
            
            await update.message.reply_text("✅ Instantaneous exit signal sent. Trikal is squaring off the position now.")
            logger.info("Successfully called _execute_manual_exit.")

        except Exception as e:
            logger.error(f"Failed to call _execute_manual_exit: {e}")
            await update.message.reply_text(f"❌ ERROR: Could not execute manual exit: {e}")
    else:
        await update.message.reply_text("ℹ️ No active trade found in Trikal to exit.")
        logger.warning("Received /exit command, but no active trade was found.")

# ==============================================================================
# --- MAIN APPLICATION ENTRY POINT (MODIFIED) ---
# ==============================================================================

def main():
    global trikal_engine_instance

    # --- This entire block of argument parsing and config loading is UNCHANGED ---
    parser = argparse.ArgumentParser(description="Trikal: Unified Trading Engine with Mantri Remote")
    parser.add_argument("--date", help="Run in BACKTEST mode for a specific date (YYYY-MM-DD).")
    parser.add_argument("--token", required=True, help="Breeze API session token.")
    parser.add_argument("--strategy", required=True, choices=STRATEGY_MAP.keys(), help="The name of the strategy to run.")
    parser.add_argument("--instance", required=True, choices=INSTANCE_CONFIG.keys(), help="The bot instance configuration to run.")
    args = parser.parse_args()

    config = INSTANCE_CONFIG[args.instance]
    try:
        print(f"Connecting to Breeze API for instance: '{args.instance}'...")
        breeze = BreezeConnect(api_key=config["API_KEY"])
        breeze.generate_session(api_secret=config["API_SECRET"], session_token=args.token)
        print("Breeze API session generated successfully.")
    except Exception as e:
        print(f"❌ Failed to connect to Breeze API: {e}"); traceback.print_exc(); return

    strategy_name = args.strategy
    strategy_class = STRATEGY_MAP[strategy_name]
    strategy_config = STRATEGY_PARAMS[strategy_name]
    strategy_to_run = strategy_class(name=strategy_name, config=strategy_config)
    print(f"✅ Strategy selected: {strategy_name}")

    # --- THE LOGIC NOW SPLITS FOR BACKTEST vs LIVE ---

    if args.date:
        # --- BACKTEST MODE: RUNS NORMALLY, NO TELEGRAM ---
        print("\n--- Running in BACKTEST mode. Mantri remote is disabled. ---")
        run_date_obj = datetime.strptime(args.date, "%Y-%m-%d").date()
        expiry_date = get_monthly_expiry_for_date(run_date_obj, roll_on_expiry_day=True)
        provider = TrikalProvider(mode='backtest', date_str=args.date, breeze_api=breeze, interval="1minute")
        engine = TrikalEngine(strategy_to_run, provider, config["CAPITAL_CONFIG"], expiry_date, interval_minutes=5, instance_name=args.instance)
        data_gen = backtest_data_generator(provider)
        
        # This is a blocking call, as it should be for backtesting.
        engine.run(data_iterator=data_gen)

    else:
        # --- LIVE MODE: RUNS ENGINE IN BACKGROUND AND TELEGRAM IN FOREGROUND ---
        print("\n--- Running in LIVE mode. Initializing Mantri remote... ---")
        today = date.today()
        expiry_date = get_monthly_expiry_for_date(today, roll_on_expiry_day=True)
        provider = TrikalProvider(mode='live', breeze_api=breeze, interval="1minute")
        
        # Create the engine instance and assign it to the global variable
        trikal_engine_instance = TrikalEngine(strategy_to_run, provider, config["CAPITAL_CONFIG"], expiry_date, interval_minutes=5, instance_name=args.instance)
        
        data_gen = live_data_generator(provider, expiry_date)

        # Start the Trikal Engine in a separate, background thread
        print("--- Starting Trikal Engine in background... ---")
        engine_thread = threading.Thread(
            target=trikal_engine_instance.run,
            args=(data_gen,),
            daemon=True
        )
        engine_thread.start()
        print("--- Trikal Engine is running. ---")

        # Start the Telegram bot on the main thread to listen for commands
        print("\n--- Starting Mantri remote control listener... ---")
        if "YOUR_" in TELEGRAM_CONFIG["BOT_TOKEN"] or "YOUR_" in TELEGRAM_CONFIG["CHAT_ID"]:
            logger.error("Telegram Bot Token or Chat ID is not configured. Remote control is disabled.")
            # The engine thread will continue to run, but without remote control.
            engine_thread.join() # Wait for the engine to finish (runs forever)
            return

        application = Application.builder().token(TELEGRAM_CONFIG["BOT_TOKEN"]).build()
        application.add_handler(CommandHandler("exit", exit_command_handler))
        
        print("--- Mantri is now listening for the /exit command. ---")
        # This is a blocking call that runs forever, listening for Telegram updates.
        application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()