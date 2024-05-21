import pandas as pd
import os
from pandas.tseries.offsets import MonthEnd

# Path to the folder containing the price history CSV files
folder_path = r'C:\Users\sandr\PycharmProjects\Webscraper\Data\Price History'
# Output Excel file name
output_file = r'C:\Users\sandr\PycharmProjects\Webscraper\crypto_returns_volatility.xlsx'

# Initialize an empty DataFrame to store aggregated data

aggregated_data = []  # List to hold aggregated data for each cryptocurrency

for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        filepath = os.path.join(folder_path, filename)

        # Read the CSV into a DataFrame
        df = pd.read_csv(filepath)

        # Adjust the column names and parse dates
        df.rename(columns={'Open Time': 'Date'}, inplace=True)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)

        # Assuming 'Close' column exists; adjust if your data uses a different name
        df['Close'].ffill()  # Forward fill the 'Close' column

        # Resample to monthly, using the last value of each month
        monthly_prices = df['Close'].resample('ME').last()

        # Calculate monthly returns
        monthly_returns = monthly_prices.pct_change().ffill()

        # Calculate annual returns from monthly returns
        annual_returns = (1 + monthly_returns).groupby(monthly_returns.index.year).prod() - 1

        # Calculate monthly and annual volatility
        monthly_volatility = monthly_returns.std() * (12 ** 0.5)
        annual_volatility = annual_returns.std()

        # Append calculated metrics to the aggregated_data list
        aggregated_data.append({
            'Crypto': filename.replace('.csv', ''),
            'Monthly Return': monthly_returns.mean(),
            'Annual Return': annual_returns.mean(),
            'Monthly Volatility': monthly_volatility,
            'Annual Volatility': annual_volatility
        })

# Convert the aggregated data to a DataFrame
aggregated_df = pd.DataFrame(aggregated_data)

# Save the DataFrame to an Excel file
aggregated_df.to_excel(output_file, index=False)

print(f'Aggregated data saved to {output_file}.')