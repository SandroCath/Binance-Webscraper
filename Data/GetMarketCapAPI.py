import requests
from binance.client import Client
import pandas as pd
from datetime import datetime
import time

def fetch_coins_list():
    url = 'https://api.coingecko.com/api/v3/coins/list'
    response = requests.get(url)
    return response.json()

# Function to check if modified name matches any key or value in the dictionary
def matches_key_or_value(modified_name, dictionary):
    if modified_name in dictionary.keys() or modified_name in dictionary.values():
        return True
    return False

def fetch_historical_market_data(coin_id, vs_currency='usd', from_timestamp=1262304000,
                                 to_timestamp=int(datetime.now().timestamp())):
    url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range'
    params = {
        'vs_currency': vs_currency,
        'from': from_timestamp,
        'to': to_timestamp
    }
    response = requests.get(url, params=params)
    return response.json()

def find_in_symbollist(binance_symbols,coingecko_coin):
    filename=None
    for symbol in binance_symbols:
        clean_symbol = symbol[:-4]  # Remove the last 4 characters assuming they are 'USDT'
        matches = coingecko_coin['symbol'].upper() == clean_symbol or coingecko_coin['symbol'].upper()==symbol
        if matches:
            filename =r'C:\Users\sandr\PycharmProjects\Webscraper\Data\Crypto Market Caps' + "\\" + f"{symbol}.csv"
            binance_symbols.remove(symbol)
    return filename


# Replace 'your_api_key' and 'your_api_secret' with your actual Binance API key and secret
api_key = 'Y6ipHw0QMDSZLGrguqriY4oCRHABbDBjucc4ZJdOLE7z0NolaRunt5PChGy7V3js'
api_secret = 'xJHUNTPGWo7mSH3Uc9RJy4jJwFsLTfuyIZx8iGdPBy8AmZnwEFZkQqffhSVivNnY'

# Initialize the client
client = Client(api_key, api_secret)
info = client.get_exchange_info()
binance_status = info.get('symbols')
binance_symbols=[]
for key in binance_status:
    binance_symbols.append(key['symbol'])

print(binance_symbols)

coingecko_coins = fetch_coins_list()  # Fetch the list of coins from CoinGecko

# Simple mapping based on symbol matching (you might need a more sophisticated approach)

catches=0
misses=0

with open(r'C:\Users\sandr\PycharmProjects\Webscraper\Data\Crypto Market Caps\unmatched.txt','a') as errorlog:
    # Limit to first 10 coins for example; remove or adjust this limit as needed
    for coin in coingecko_coins:
        try:
            print(f"Fetching data for {coin['id']}...")
            historical_data = fetch_historical_market_data(coin['id'], 'usd', 1262304000, int(datetime.now().timestamp()))
            # Process and save the data
            market_caps = historical_data.get('market_caps', [])
            data = {'Timestamp': [mc[0] for mc in market_caps], 'MarketCap': [mc[1] for mc in market_caps]}
            df = pd.DataFrame(data)
            df['Date'] = pd.to_datetime(df['Timestamp'], unit='ms')
            df.drop('Timestamp', axis=1, inplace=True)

            filename=find_in_symbollist(binance_symbols,coin)
            if filename==None:
                filename = r'C:\Users\sandr\PycharmProjects\Webscraper\Data\Crypto Market Caps' + "\\" + f"{coin['id']}.csv"
                errorlog.write(f"{coin['id']}.csv" + "\n")
                misses+=1
            else:catches+=1
            df.to_csv(filename, index=False)
            print(f"Data saved to {filename} with {misses} misses :"+str(catches/(catches+misses))+"%")
            time.sleep(0.4)  # Sleep to avoid hitting rate limits

        except Exception as e:
            print(f"Failed to fetch data for {coin['id']}: {e}")


print("Binance symbols that could be matched with coingecko: "+str(catches/(catches+misses))+"%")

