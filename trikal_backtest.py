# --- START OF FILE trikal_backtest.py ---

import pandas as pd
from typing import Iterator, Tuple
from datetime import datetime

from trikal_provider import TrikalProvider

def backtest_data_generator(provider: TrikalProvider) -> Iterator[Tuple[datetime, pd.DataFrame]]:
    """
    A generator that yields historical 1-minute futures candles for backtesting.
    """
    day_futures_df = provider.get_day_data_feed()
    if day_futures_df is None or day_futures_df.empty:
        print("❌ No futures data found for the specified backtest date. Backtest cannot run.")
        return

    # MODIFIED: Corrected the log message to accurately reflect the 1-minute interval.
    print(f"✅ Backtest data source initialized. Yielding {len(day_futures_df)} historical 1-min candles...")
    for fut_timestamp, fut_row in day_futures_df.iterrows():
        yield fut_timestamp, fut_row.to_frame().T