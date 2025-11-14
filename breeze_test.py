from breeze_connect import BreezeConnect
import json

# Initialize SDK
breeze = BreezeConnect(api_key="6EdY48855hZq0m484243(2181jl06F38")

# Obtain your session key from https://api.icicidirect.com/apiuser/login?api_key=YOUR_API_KEY
# Incase your api-key has special characters(like +,=,!) then encode the api key before using in the url as shown below.
import urllib
print("https://api.icicidirect.com/apiuser/login?api_key="+urllib.parse.quote_plus("6EdY48855hZq0m484243(2181jl06F38"))

# Generate Session
breeze.generate_session(api_secret="41`x8(9894&87s60CN2Y@4469B616K02",
                        session_token="53509939")

# Generate ISO8601 Date/DateTime String
import datetime
iso_date_string = datetime.datetime.strptime("28/02/2021","%d/%m/%Y").isoformat()[:10] + 'T05:30:00.000Z'
iso_date_time_string = datetime.datetime.strptime("28/02/2021 23:59:59","%d/%m/%Y %H:%M:%S").isoformat()[:19] + '.000Z'

response = breeze.get_trade_list(from_date="2025-09-01T06:00:00.000Z",
                        to_date="2025-10-24T06:00:00.000Z",
                        exchange_code="NFO",
                        product_type="",
                        action="",
                        stock_code="")


print(json.dumps(response, indent=4))