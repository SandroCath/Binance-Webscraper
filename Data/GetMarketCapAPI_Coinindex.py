import requests
import pandas as pd
from datetime import datetime, timedelta

# Calculate the number of days from 01.01.2012 to today
start_date = datetime(2012, 1, 1)
end_date = datetime.now()
days = (end_date - start_date).days

# API parameters
api_url = f"https://coincodex.com/api/coincodex/get_firstpage_history/{days}/1/10"

# Send request to the API
response = requests.get(api_url)
data = response.json()

# Prepare DataFrame to store cryptocurrency data
columns = [coin for coin in data.keys()]
date_range = pd.date_range(end=datetime.today(), periods=days)
df = pd.DataFrame(index=date_range, columns=columns)

# Populate the DataFrame with price data
for coin, values in data.items():
    for entry in values:
        timestamp = datetime.fromtimestamp(entry[0])
        price = entry[1]
        df.at[timestamp, coin] = price

# Fill missing values (if any) and convert prices to float
df.fillna(method='ffill', inplace=True)
df = df.astype(float)

# Calculate equally-weighted day-over-day returns
df['EWD_Returns'] = df.pct_change().mean(axis=1)

# Save the DataFrame to CSV
df.to_csv(r'C:\Users\sandr\PycharmProjects\Webscraper\Data\Benchmark Returns\cmc10_prices_and_returns.csv')

print("Data successfully saved to 'crypto_prices_and_returns.csv'")
