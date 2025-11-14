#!/usr/bin/env python3
"""
Mantri - The Remote Control for the Trikal Trading Bot.

This script runs continuously, listening for commands from a specific
Telegram group. Based on the commands, it interacts with the Trikal bot
by creating trigger files.

(Version 2.3 - Ignores old messages on startup)
"""

import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# --- USER CONFIGURATION ---
CONFIG = {
    "TELEGRAM_BOT_TOKEN": "8371304783:AAEzsfjwYOtwmS33wOXNzk6wwH6uNtbDmVw",
    "TELEGRAM_CHAT_ID": "-1003220703549"  # <-- PASTE YOUR CHAT ID HERE
}

MANUAL_TRIGGER_FILE = "MANUAL_SQUARE_OFF.trigger"
# --- END CONFIGURATION ---

# Set up logging to be less noisy
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


async def exit_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """This function is called ONLY when the '/exit' command is received."""
    if not update.message or not update.message.chat:
        return

    chat_id = str(update.message.chat_id)
    
    if chat_id != CONFIG["TELEGRAM_CHAT_ID"]:
        logger.warning(f"Ignoring /exit command from unauthorized chat ID: {chat_id}")
        return

    logger.info("Received '/exit' command. Creating trigger file...")
    try:
        with open(MANUAL_TRIGGER_FILE, 'w') as f:
            pass
        
        await update.message.reply_text(
            "✅ Manual exit signal sent. Trikal will square off the position within 30 seconds."
        )
        logger.info(f"Successfully created trigger file: {MANUAL_TRIGGER_FILE}")

    except Exception as e:
        logger.error(f"Failed to create trigger file: {e}")
        await update.message.reply_text(f"❌ ERROR: Could not create trigger file: {e}")


def main() -> None:
    """Starts the Mantri remote control listener."""
    logger.info("Starting Mantri remote control...")
    
    if "YOUR_" in CONFIG["TELEGRAM_BOT_TOKEN"] or "YOUR_" in CONFIG["TELEGRAM_CHAT_ID"]:
        logger.error("Telegram Bot Token or Chat ID is not configured. Please edit mantri.py. Exiting.")
        return

    application = Application.builder().token(CONFIG["TELEGRAM_BOT_TOKEN"]).build()
    application.add_handler(CommandHandler("exit", exit_command_handler))

    logger.info("Mantri is now listening for the /exit command (ignoring any old messages)...")
    
    # --- THIS IS THE FIX ---
    # The `drop_pending_updates=True` tells the bot to clear the message queue before it starts.
    application.run_polling(drop_pending_updates=True)
    # --- END OF FIX ---


if __name__ == "__main__":
    main()