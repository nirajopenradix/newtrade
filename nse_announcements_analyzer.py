#!/usr/bin/env python3
"""
NSE Corporate Announcements Daily Scanner
Scans a list of stocks for new order announcements and updates a persistent
JavaScript database file (nse_orders_database.js) for the HTML report.
"""

import requests
import json
from datetime import datetime, timedelta
import time
import os
import PyPDF2
import re
import io
import smtplib
from email.mime.text import MIMEText
from typing import List, Dict, Optional
import random
import subprocess
import pid  # <--- THIS IS THE PID LOCK LIBRARY

# --- USER CONFIGURATION FOR EMAIL ALERTS ---
ENABLE_EMAIL_ALERTS = True
SMTP_SERVER = 'smtp.office365.com'
SMTP_PORT = 587
SENDER_EMAIL = 'niraj@openradix.in'
SENDER_PASSWORD = 'Wework@130'
RECIPIENT_EMAIL = [
    'niraj@openradix.in',
    'akshay@openradix.in',
    'kaustubh@openradix.in',
    'yashnirajwagle@gmail.com',
    'rajeev.patil10@gmail.com'
]

# --- USER CONFIGURATION FOR TELEGRAM ALERTS ---
ENABLE_TELEGRAM_ALERTS = True
TELEGRAM_BOT_TOKEN = '8371304783:AAEzsfjwYOtwmS33wOXNzk6wwH6uNtbDmVw'
TELEGRAM_CHAT_ID = '-4700008458'

# --- RSYNC CONFIGURATION ---
ENABLE_RSYNC_UPLOAD = True
RSYNC_PEM_KEY = '/Users/a9768030444/Dropbox/usefuldocs/pemfiles/zimbraonaws.pem'
RSYNC_SOURCE_FILE = '/Users/a9768030444/orapps/prj/newtrade/nse_orders_database.js'
RSYNC_DESTINATION = 'bitnami@3.109.75.2:/usr/src/prj/ozone/app/website/nse_orders_database.js'

# --- PROCESS LOCKING CONFIGURATION ---
PID_FILE = 'nse_scanner.pid'


class NSEAnnouncementAnalyzer:
    # ... (No changes inside this class) ...
    def __init__(self):
        self.base_url = "https://www.nseindia.com/api/corporate-announcements"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01', 'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br', 'Connection': 'keep-alive',
            'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-announcements',
            'Origin': 'https://www.nseindia.com', 'DNT': '1', 'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors', 'Sec-Fetch-Site': 'same-origin',
        })
        self.cookies_initialized = False

    def initialize_session(self) -> bool:
        if self.cookies_initialized: return True
        try:
            home_response = self.session.get('https://www.nseindia.com/', timeout=20)
            home_response.raise_for_status()
            announcements_page = self.session.get('https://www.nseindia.com/companies-listing/corporate-filings-announcements', timeout=20)
            announcements_page.raise_for_status()
            time.sleep(random.uniform(1, 2))
            self.cookies_initialized = True
            return True
        except Exception as e:
            print(f"Error: Session initialization failed: {e}")
            return False

    def get_date_range(self) -> tuple:
        to_date = datetime.now()
        from_date = to_date - timedelta(days=1)
        return from_date.strftime("%d-%m-%Y"), to_date.strftime("%d-%m-%Y")

    def fetch_announcements(self, symbol: str) -> Optional[List[Dict]]:
        try:
            if not self.cookies_initialized:
                if not self.initialize_session(): return None
            from_date, to_date = self.get_date_range()
            params = {'index': 'equities', 'from_date': from_date, 'to_date': to_date, 'symbol': symbol.upper()}
            response = self.session.get(self.base_url, params=params, timeout=20)
            if response.status_code == 200:
                return response.json() if response.text.strip() else []
            elif response.status_code == 401:
                self.cookies_initialized = False
                if self.initialize_session():
                    response = self.session.get(self.base_url, params=params, timeout=20)
                    return response.json() if response.status_code == 200 and response.text.strip() else None
            return None
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(f"  -> Error fetching data for {symbol}: {e}")
            return None

    def analyze_announcements(self, announcements: List[Dict]) -> List[Dict]:
        found_orders = []
        for ann in announcements:
            pdf_url = ann.get('attchmntFile')
            order_snippet = None
            if pdf_url and pdf_url.lower().endswith('.pdf'):
                order_snippet = self._extract_order_snippet_from_pdf(pdf_url)
            if not order_snippet:
                order_snippet = "Order value not automatically found in PDF."
            found_orders.append({'announcement': ann, 'order_snippet': order_snippet})
        return found_orders

    def _extract_order_snippet_from_pdf(self, pdf_url: str) -> Optional[str]:
        try:
            pdf_response = self.session.get(pdf_url, timeout=30)
            pdf_response.raise_for_status()
            full_text = ""
            with io.BytesIO(pdf_response.content) as pdf_file:
                pdf_reader = PyPDF2.PdfReader(pdf_file)
                for page in pdf_reader.pages:
                    page_text = page.extract_text()
                    if page_text: full_text += page_text.replace('\n', ' ') + ' '
            if not full_text.strip(): return None
            cleaned_text = re.sub(r'[^\x00-\x7F]+', ' ', full_text)
            cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
            keywords = ['crore', 'crores', 'cr', 'lakh', 'lakhs', 'lac']
            pattern_with_keyword = r'([0-9,.]+\s*\b(?:' + '|'.join(keywords) + r')\b)'
            match = re.search(pattern_with_keyword, cleaned_text, re.IGNORECASE)
            if match:
                full_value_expression = match.group(1)
                preceding_text = cleaned_text[:match.start()]
                words_before = preceding_text.strip().split()
                snippet = " ".join(words_before[-15:]) + " " + full_value_expression
                return snippet.strip()
            pattern_inr_value = r'((?:Rs\.?|INR)\s*[0-9,]+(?:,\d+)*\/?-?)'
            match = re.search(pattern_inr_value, cleaned_text, re.IGNORECASE)
            if match:
                full_value_expression = match.group(1)
                preceding_text = cleaned_text[:match.start()]
                words_before = preceding_text.strip().split()
                snippet = " ".join(words_before[-15:]) + " " + full_value_expression
                return snippet.strip()
        except Exception as e:
            print(f"  -> Warning: Could not process PDF {pdf_url}. Reason: {e}")
            return None
        return None

def send_telegram_alert(message: str):
    if not ENABLE_TELEGRAM_ALERTS or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = { 'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown', 'disable_web_page_preview': True }
    try:
        print("  -> Sending Telegram alert...")
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200 and response.json().get('ok'):
            print("  -> Telegram alert sent successfully.")
        else:
            print(f"  -> ERROR: Failed to send Telegram alert. Status: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"  -> CRITICAL ERROR: An exception occurred while sending Telegram alert: {e}")

def send_email_alert(order_data: Dict):
    telegram_message = (
        f"*New Order Announcement*\n\n"
        f"Symbol: *{order_data['symbol']}*\n"
        f"Description: {order_data['description']}\n"
        f"Order Value: `{order_data['order_value']}`\n\n"
        f"[View PDF]({order_data['link']})\n"
        f"[View All Announcements](https://openradix.com/home/announcements.html)"
    )
    send_telegram_alert(telegram_message)
    if not ENABLE_EMAIL_ALERTS: return
    subject = f"New Order Announcement: {order_data['symbol']} ({order_data['order_value']})"
    body = f"A new order announcement was detected.\n\nDate: {order_data['date']}\nSymbol: {order_data['symbol']}\nDescription: {order_data['description']}\nOrder Value: {order_data['order_value']}\n\nPDF Link: {order_data['link']}\n\nView All Announcements: https://openradix.com/home/announcements.html"
    msg = MIMEText(body)
    msg['Subject'], msg['From'], msg['To'] = subject, SENDER_EMAIL, ', '.join(RECIPIENT_EMAIL)
    try:
        print(f"  -> Sending email alert for {order_data['symbol']}...")
        # --- ADDED A 60-SECOND TIMEOUT ---
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=60) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        print("  -> Email alert sent successfully.")
    except Exception as e:
        print(f"  -> ERROR: Failed to send email alert: {e}")

def sync_database_file():
    if not ENABLE_RSYNC_UPLOAD: return
    print("  -> [DEBUG] Attempting to sync database to remote server...")
    try:
        command = ['rsync', '-avz', '-e', f'ssh -i {RSYNC_PEM_KEY}', RSYNC_SOURCE_FILE, RSYNC_DESTINATION]
        print(f"  -> [DEBUG] Executing command: {' '.join(command)}")
        # --- ADDED A 120-SECOND TIMEOUT ---
        result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=120)
        if result.stdout: print(f"  -> [DEBUG] rsync stdout:\n{result.stdout.strip()}")
        if result.stderr: print(f"  -> [DEBUG] rsync stderr:\n{result.stderr.strip()}")
        if result.returncode == 0: print("  -> Sync successful.")
        else: print(f"  -> ERROR: rsync command failed with exit code {result.returncode}.")
    except subprocess.TimeoutExpired:
        print(f"  -> ERROR: rsync command timed out after 120 seconds.")
    except FileNotFoundError:
        print(f"  -> ERROR: 'rsync' command not found.")
    except Exception as e:
        print(f"  -> An unexpected error occurred during rsync: {e}")

def main():
    print("NSE Corporate Announcements Daily Scanner")
    print("="*55)
    SYMBOLS_FILE = 'symbols.txt'
    DB_FILE = RSYNC_SOURCE_FILE
    all_orders = []
    if os.path.exists(DB_FILE):
        with open(DB_FILE, 'r', encoding='utf-8') as f:
            content = f.read()
            json_str = content.strip().replace('const ordersData = ', '').rstrip(';')
            if json_str:
                try: all_orders = json.loads(json_str)
                except json.JSONDecodeError: print(f"Warning: Could not parse '{DB_FILE}'.")
    seen_order_ids = {f"{order['symbol']}_{order['link']}" for order in all_orders}
    with open(SYMBOLS_FILE, 'r', encoding='utf-8') as f:
        symbols = [line.strip().upper() for line in f if line.strip()]
    analyzer = NSEAnnouncementAnalyzer()
    print(f"Scanning {len(symbols)} symbols...")
    print(f"Database will be updated in '{DB_FILE}'\n")
    new_orders_found_this_run = 0
    if not analyzer.initialize_session():
        print("Exiting due to session initialization failure.")
        return
    for i, symbol in enumerate(symbols):
        try:
            print(f"[{i+1}/{len(symbols)}] Processing {symbol}...")
            announcements = analyzer.fetch_announcements(symbol)
            if announcements:
                for all_ann in announcements: print(f"  -> Announcement: {all_ann}")
                order_keywords = [ "award of order", "awarding of order", "awarding or bagging", "bagging of order", "bagging/receiving of order", "receives order", "receipt of order", "new order", "bags order", "bagging" ]
                order_related = [ann for ann in announcements if 'desc' in ann and any(phrase in ann['desc'].lower() for phrase in order_keywords)]
                if order_related:
                    found_orders = analyzer.analyze_announcements(order_related)
                    is_db_updated = False
                    for order in found_orders:
                        ann = order['announcement']
                        pdf_link = ann.get('attchmntFile', '')
                        unique_id = f"{ann.get('symbol', '')}_{pdf_link}"
                        if unique_id not in seen_order_ids:
                            print(f"  -> NEW ORDER FOUND: {ann.get('symbol')}")
                            new_orders_found_this_run += 1
                            is_db_updated = True
                            try:
                                date_str = datetime.strptime(ann.get('an_dt', ''), '%d-%b-%Y %H:%M:%S').strftime('%Y-%m-%d')
                            except (ValueError, TypeError):
                                date_str = datetime.now().strftime('%Y-%m-%d')
                            order_data = {'date': date_str, 'symbol': ann.get('symbol', 'N/A'), 'description': ann.get('desc', 'N/A'), 'order_value': order['order_snippet'], 'link': pdf_link}
                            all_orders.append(order_data)
                            seen_order_ids.add(unique_id)
                            send_email_alert(order_data)
                    if is_db_updated:
                        all_orders.sort(key=lambda x: x['date'], reverse=True)
                        with open(DB_FILE, 'w', encoding='utf-8') as f:
                            f.write("const ordersData = ")
                            json.dump(all_orders, f, indent=2)
                            f.write(";")
                        print(f"  -> Database '{DB_FILE}' updated locally.")
                        sync_database_file()
        except Exception as e:
            print(f"  -> CRITICAL ERROR for {symbol}: {e}.")
        finally:
            time.sleep(random.uniform(2.0, 4.0))
    print("\n" + "="*55)
    print("Scan complete.")
    if new_orders_found_this_run > 0:
        print(f"Found and processed {new_orders_found_this_run} new order announcements.")
    else:
        print("No new order announcements were found in this run.")

if __name__ == "__main__":
    # --- THIS WRAPPER HANDLES THE SELF-HEALING LOCK ---
    try:
        with pid.PidFile(pidname="nse_scanner", piddir="."):
            main()
    except pid.PidFileAlreadyLockedError:
        print(f"Scanner is already running. Exiting this instance. ({datetime.now()})")