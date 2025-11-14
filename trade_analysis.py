# --- START OF FILE trade_analysis.py ---

import argparse
import traceback
from breeze_connect import BreezeConnect
from collections import OrderedDict
from datetime import date, datetime, timedelta

# --- START: Date Configuration ---
start_date_str = "2025-09-01T06:00:00.000Z"
today_date_str = date.today().strftime("%Y-%m-%dT06:00:00.000Z")
# --- END: Date Configuration ---


# --- START: Centralized Instance Configuration ---
INSTANCE_CONFIG = {
    "niraj": {
        "API_KEY": "6EdY48855hZq0m484243(2181jl06F38",
        "API_SECRET": "41`x8(9894&87s60CN2Y@4469B616K02",
    },
    "yash": {
        "API_KEY": "16nM2=7231E9*006247G30Q30$h%418!",
        "API_SECRET": "7651373b40_+6YU!0063LAh3254N0mYq",
    },
    "akshay": {
        "API_KEY": "4X0095!%f83`D63L2V2144wS4907g6L1",
        "API_SECRET": "I41b85Nc16_4F989737s3e93(94i%tM7",
    },
    "rajeev": {
        "API_KEY": "537A+59E6Ck02O0847991900Z898r7l1",
        "API_SECRET": "v6F21l5E652b652~87373740`92oF7TX",
    }
}
# --- END: Centralized Instance Configuration ---

def pair_trades(trades):
    open_positions = {}
    completed_trades = []
    def get_contract_id(trade):
        return f"{trade['expiry_date']}-{trade['strike_price']}-{trade['right']}"
    for trade in trades:
        contract_id = get_contract_id(trade)
        if contract_id in open_positions:
            entry_trade = open_positions.pop(contract_id)
            exit_trade = trade
            completed_trades.append((entry_trade, exit_trade))
        else:
            open_positions[contract_id] = trade
    return completed_trades, open_positions

def analyze_and_print_summary(api_response):
    if "Success" not in api_response or not api_response["Success"]:
        if "Error" in api_response and api_response["Error"]:
             print(f"API Error: {api_response['Error']}")
        else:
             print("Could not retrieve trade data. The 'Success' key is missing or empty in the response.")
        return

    completed_trades, open_positions = pair_trades(api_response["Success"])
    daily_summary = {}

    for entry_trade, exit_trade in completed_trades:
        if entry_trade['action'].lower() == 'buy' and exit_trade['action'].lower() == 'sell':
            buy_trade, sell_trade = entry_trade, exit_trade
        elif exit_trade['action'].lower() == 'buy' and entry_trade['action'].lower() == 'sell':
            buy_trade, sell_trade = exit_trade, entry_trade
        else:
            continue

        try:
            sell_value = float(sell_trade['average_cost']) * int(sell_trade['quantity'])
            buy_value = float(buy_trade['average_cost']) * int(buy_trade['quantity'])
            
            sell_charges = float(sell_trade['brokerage_amount']) + float(sell_trade['total_taxes'])
            buy_charges = float(buy_trade['brokerage_amount']) + float(buy_trade['total_taxes'])
            total_pair_charges = sell_charges + buy_charges
            net_pl = (sell_value - buy_value) - total_pair_charges

            trade_date = sell_trade['trade_date']
            if trade_date not in daily_summary:
                daily_summary[trade_date] = {'profitable': 0, 'loss_making': 0, 'net_pl': 0.0, 'total_charges': 0.0}
            
            daily_summary[trade_date]['profitable'] += 1 if net_pl > 0 else 0
            daily_summary[trade_date]['loss_making'] += 1 if net_pl <= 0 else 0
            daily_summary[trade_date]['net_pl'] += net_pl
            daily_summary[trade_date]['total_charges'] += total_pair_charges

        except (ValueError, KeyError) as e:
            print(f"Skipping a trade pair due to data error: {e}")
            continue

    sorted_summary = OrderedDict(sorted(daily_summary.items(), key=lambda item: datetime.strptime(item[0], '%d-%b-%Y')))

    print("\nDaily Transactions Summary")
    print("--------------------------")
    header = f"{'Trade Date':<16} | {'Profit':>6} | {'Loss':>5} | {'Charges':>10} | {'Net Pnl':>12}"
    print(header)
    print("-" * len(header))

    total_profitable, total_loss_making, total_charges, total_net_pl = 0, 0, 0.0, 0.0
    for date_str, data in sorted_summary.items():
        date_obj = datetime.strptime(date_str, '%d-%b-%Y')
        formatted_date = date_obj.strftime('%a, %d-%b-%Y')
        total_profitable += data['profitable']
        total_loss_making += data['loss_making']
        total_charges += data['total_charges']
        total_net_pl += data['net_pl']
        print(f"{formatted_date:<16} | {data['profitable']:>6} | {data['loss_making']:>5} | {data['total_charges']:>10,.2f} | {data['net_pl']:>12,.2f}")

    print("-" * len(header))
    print(f"{'Total':<16} | {total_profitable:>6} | {total_loss_making:>5} | {total_charges:>10,.2f} | {total_net_pl:>12,.2f}")
    print("-" * len(header))

    if open_positions:
        print("\nActive Open Positions")
        print("---------------------")
        header = f"{'Contract':<25} | {'Action':<6} | {'Qty':>5} | {'Price':>10} | {'Date':<12}"
        print(header)
        print("-" * (len(header) + 2))
        for _, trade in open_positions.items():
            contract = f"{trade['stock_code']} {trade['expiry_date']} {trade['strike_price']} {trade['right']}"
            print(f"{contract:<25} | {trade['action']:<6} | {trade['quantity']:>5} | {float(trade['average_cost']):>10.2f} | {trade['trade_date']:<12}")
        print("-" * (len(header) + 2))

def analyze_and_print_monthly_summary(api_response):
    if "Success" not in api_response or not api_response["Success"]:
        return

    completed_trades, _ = pair_trades(api_response["Success"])
    monthly_summary = {}

    for entry_trade, exit_trade in completed_trades:
        if entry_trade['action'].lower() == 'buy' and exit_trade['action'].lower() == 'sell':
            buy_trade, sell_trade = entry_trade, exit_trade
        elif exit_trade['action'].lower() == 'buy' and entry_trade['action'].lower() == 'sell':
            buy_trade, sell_trade = exit_trade, entry_trade
        else:
            continue

        try:
            sell_value = float(sell_trade['average_cost']) * int(sell_trade['quantity'])
            buy_value = float(buy_trade['average_cost']) * int(buy_trade['quantity'])
            
            sell_charges = float(sell_trade['brokerage_amount']) + float(sell_trade['total_taxes'])
            buy_charges = float(buy_trade['brokerage_amount']) + float(buy_trade['total_taxes'])
            total_pair_charges = sell_charges + buy_charges
            net_pl = (sell_value - buy_value) - total_pair_charges

            date_obj = datetime.strptime(sell_trade['trade_date'], '%d-%b-%Y')
            month_key = date_obj.strftime('%b-%Y') 

            if month_key not in monthly_summary:
                monthly_summary[month_key] = {'profitable': 0, 'loss_making': 0, 'net_pl': 0.0, 'total_charges': 0.0}
            
            monthly_summary[month_key]['profitable'] += 1 if net_pl > 0 else 0
            monthly_summary[month_key]['loss_making'] += 1 if net_pl <= 0 else 0
            monthly_summary[month_key]['net_pl'] += net_pl
            monthly_summary[month_key]['total_charges'] += total_pair_charges

        except (ValueError, KeyError):
            continue

    sorted_summary = OrderedDict(sorted(monthly_summary.items(), key=lambda item: datetime.strptime(item[0], '%b-%Y')))
    
    print("\n\nMonthly Transactions Summary")
    print("----------------------------")
    header = f"{'Month':<16} | {'Profit':>6} | {'Loss':>5} | {'Charges':>10} | {'Net Pnl':>12}"
    print(header)
    print("-" * len(header))

    total_profitable, total_loss_making, total_charges, total_net_pl = 0, 0, 0.0, 0.0
    for month_str, data in sorted_summary.items():
        total_profitable += data['profitable']
        total_loss_making += data['loss_making']
        total_charges += data['total_charges']
        total_net_pl += data['net_pl']
        print(f"{month_str:<16} | {data['profitable']:>6} | {data['loss_making']:>5} | {data['total_charges']:>10,.2f} | {data['net_pl']:>12,.2f}")

    print("-" * len(header))
    print(f"{'Total':<16} | {total_profitable:>6} | {total_loss_making:>5} | {total_charges:>10,.2f} | {total_net_pl:>12,.2f}")
    print("-" * len(header))

# --- REWRITTEN: Simplified function with order_id sorting ---
def print_individual_trades(api_response):
    if "Success" not in api_response or not api_response["Success"]:
        return
        
    completed_trades, _ = pair_trades(api_response["Success"])
    if not completed_trades:
        print("\nNo completed trades found to display in the individual log.")
        return

    trade_details_list = []

    for entry_trade, exit_trade in completed_trades:
        if entry_trade['action'].lower() == 'buy' and exit_trade['action'].lower() == 'sell':
            buy_trade, sell_trade = entry_trade, exit_trade
        elif exit_trade['action'].lower() == 'buy' and entry_trade['action'].lower() == 'sell':
            buy_trade, sell_trade = exit_trade, entry_trade
        else:
            continue
            
        try:
            sell_value = float(sell_trade['average_cost']) * int(sell_trade['quantity'])
            buy_value = float(buy_trade['average_cost']) * int(buy_trade['quantity'])
            
            sell_charges = float(sell_trade['brokerage_amount']) + float(sell_trade['total_taxes'])
            buy_charges = float(buy_trade['brokerage_amount']) + float(buy_trade['total_taxes'])
            total_pair_charges = sell_charges + buy_charges
            net_pl = (sell_value - buy_value) - total_pair_charges

            trade_details = {
                # This key is now used for sorting but not displayed
                "sort_key_order_id": buy_trade['order_id'], 
                "Date": sell_trade['trade_date'],
                "Contract": f"{sell_trade['stock_code']} {sell_trade['expiry_date']} {sell_trade['strike_price']} {sell_trade['right']}",
                "Qty": int(sell_trade['quantity']),
                "Buy Price": float(buy_trade['average_cost']),
                "Sell Price": float(sell_trade['average_cost']),
                "Charges": total_pair_charges,
                "Net PnL": net_pl,
            }
            trade_details_list.append(trade_details)

        except (ValueError, KeyError) as e:
            print(f"Skipping a trade for the detailed log due to data error: {e}")
            continue

    # Sort trades by the entry order_id
    sorted_trades = sorted(trade_details_list, key=lambda t: t['sort_key_order_id'])
    
    print("\n\nIndividual Trade Log")
    print("--------------------")
    # Updated header to show only "Date"
    header = f"{'Date':<12} | {'Contract':<30} | {'Qty':>5} | {'Buy Price':>10} | {'Sell Price':>11} | {'Charges':>10} | {'Net Pnl':>12}"
    print(header)
    print("-" * len(header))

    # Updated print statement to match the new header
    for trade in sorted_trades:
        print(f"{trade['Date']:<12} | {trade['Contract']:<30} | {trade['Qty']:>5} | {trade['Buy Price']:>10.2f} | {trade['Sell Price']:>11.2f} | {trade['Charges']:>10.2f} | {trade['Net PnL']:>12.2f}")

    print("-" * len(header))


def main():
    parser = argparse.ArgumentParser(description="Trikal: Trade Analysis Tool")
    parser.add_argument("--instance", required=True, choices=INSTANCE_CONFIG.keys(), help="The bot instance configuration to use for API connection.")
    parser.add_argument("--token", required=True, help="Breeze API session token.")
    parser.add_argument("--trades", action="store_true", help="Display a detailed list of all individual completed trades.")
    args = parser.parse_args()

    config = INSTANCE_CONFIG[args.instance]

    try:
        print(f"Connecting to Breeze API for instance: '{args.instance}'...")
        breeze = BreezeConnect(api_key=config["API_KEY"])
        breeze.generate_session(api_secret=config["API_SECRET"], session_token=args.token)
        print("Breeze API session generated successfully.")
    except Exception as e:
        print(f"❌ Failed to connect to Breeze API: {e}"); traceback.print_exc(); return

    # --- SIMPLIFIED: Only fetching the trade list ---
    print(f"Fetching trades from {start_date_str.split('T')[0]} to {today_date_str.split('T')[0]}...")
    trade_response = breeze.get_trade_list(from_date=start_date_str, to_date=today_date_str, exchange_code="NFO", product_type="", action="", stock_code="")
    
    analyze_and_print_summary(trade_response)
    analyze_and_print_monthly_summary(trade_response)

    if args.trades:
        print_individual_trades(trade_response)

if __name__ == "__main__":
    main()