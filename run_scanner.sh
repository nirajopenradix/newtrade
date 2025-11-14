#!/bin/bash

# --- UPDATE THESE PATHS ---
PROJECT_DIR="/Users/a9768030444/orapps/prj/trade"
VENV_PYTHON_PATH="/Users/a9768030444/orapps/prj/newtrade/venv/bin/python"
# --- END OF PATHS ---

# --- EXECUTION LOGIC ---

# Navigate to the project directory
cd "$PROJECT_DIR" || exit

echo "Starting NSE Scanner with a 2-hour timeout... ($(date))"

# The Python script will now fix its own path internally.
/opt/homebrew/bin/gtimeout 2h "$VENV_PYTHON_PATH" nse_announcements_analyzer.py

# --- CORRECTED FINAL LINE ---
# Using 'printf' is safer than 'echo' for printing strings with special characters
# or command substitutions. This will not produce a syntax error.
printf "NSE Scanner finished or was timed out. (%s)\n" "$(date)"