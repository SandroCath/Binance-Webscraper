from binance.client import Client
from datetime import datetime
import os
import time
import csv

# Replace 'your_api_key' and 'your_api_secret' with your actual Binance API key and secret
api_key = 'Y6ipHw0QMDSZLGrguqriY4oCRHABbDBjucc4ZJdOLE7z0NolaRunt5PChGy7V3js'
api_secret = 'xJHUNTPGWo7mSH3Uc9RJy4jJwFsLTfuyIZx8iGdPBy8AmZnwEFZkQqffhSVivNnY'

# Initialize the client
client = Client(api_key, api_secret)
info = client.get_exchange_info()
symbols_status = {symbol['symbol']: symbol['status'] for symbol in info.get('symbols', [])}

def fetch_symbols():
    """
    Fetch all symbols for trading pairs available on Binance.

    :return: List of symbols.
    """
    info = client.get_exchange_info()
    symbols = [symbol['symbol'] for symbol in info['symbols']]
    return symbols


def fetch_historical_prices(symbol, interval):
    """
    Fetch historical price data for a given symbol since its inception.

    :param symbol: Symbol for the cryptocurrency (e.g., 'BTCUSDT')
    :param interval: Data interval (e.g., '1d' for daily)
    :return: Historical klines data
    """
    try:
        klines = client.get_historical_klines(symbol, interval, "1 Jan, 2010")
        processed_data = []
        for kline in klines:
            open_time = datetime.fromtimestamp(kline[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')
            open_price, high_price, low_price, close_price = kline[1:5]
            processed_data.append([open_time, open_price, high_price, low_price, close_price])
        return processed_data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None


def save_to_csv(symbol, data):
    """
    Save the price history data to a CSV file.

    :param symbol: Symbol for the cryptocurrency
    :param data: Price history data
    """
    path =r"C:\Users\sandr\PycharmProjects\Webscraper\Data\Price History"
    filename = path + "\\" + f"{symbol}.csv"
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Open Time', 'Open', 'High', 'Low', 'Close'])
        writer.writerows(data)
    print(f"Data for {symbol} saved to {filename}")


symbols = fetch_symbols()
interval = '1d'  # Daily interval
print(str(len(symbols))+" coins to be loaded")
for symbol in symbols:
    if symbols_status.get(symbol) == 'BREAK':
        pass
    historical_prices = fetch_historical_prices(symbol, interval)
    if historical_prices:
        save_to_csv(symbol, historical_prices)
    time.sleep(0.5)  # Sleep to respect API rate limits, adjust as necessary

print("Finished extracting data")