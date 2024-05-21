import pandas as pd
import os
import numpy as np

# Path to the directory containing the files
directory_path = r'C:\Users\sandr\PycharmProjects\Webscraper\Data\Crypto Market Caps'


# List to hold each cryptocurrency DataFrame
crypto_data_list = []
all_dates = set()  # To track all unique dates across files

# Iterate over each file in the directory
for filename in os.listdir(directory_path):
    if filename.endswith(".csv") and not filename.startswith("0000"):  # Ensure the file is a CSV
        file_path = os.path.join(directory_path, filename)
        # Read the CSV file into a DataFrame
        try:
            temp_df = pd.read_csv(file_path)
            temp_df['Date'] = pd.to_datetime(temp_df['Date'])
            # Keep track of all unique dates
            all_dates.update(temp_df['Date'])
            # Set the Date column as the index
            temp_df.set_index('Date', inplace=True)
            # The filename without extension as the cryptocurrency name
            crypto_name = os.path.splitext(filename)[0]
            # Store DataFrame in list
            crypto_data_list.append(temp_df.rename(columns={'MarketCap': crypto_name}))
        except pd.errors.EmptyDataError:
            print(f"No data in file {filename}")

# Create a single DataFrame containing all cryptocurrencies
combined_df = pd.concat(crypto_data_list, axis=1)

# Standardize the DataFrame index to include all possible dates found
all_dates = sorted(list(all_dates))
combined_df = combined_df.reindex(all_dates)

# Fill in missing values (NaNs)
combined_df.fillna(method='ffill', inplace=True)  # Forward fill
combined_df.fillna(method='bfill', inplace=True)  # Backward fill if needed

# Prepare the final DataFrame to store top 10 cryptos by market cap per date
top_10_per_date = pd.DataFrame()

combined_df.to_csv(directory_path+r'\0000_debug.csv')
# Iterate over each date
for date in combined_df.index:
    # Sort the cryptocurrencies by market cap in descending order and select the top 10
    sorted_cryptos = combined_df.loc[date].sort_values(ascending=False)[:10]
    top_10_per_date[date] = sorted_cryptos.index

# Transpose the DataFrame for easier reading (dates as columns)
top_10_per_date = top_10_per_date.T

# Save the top 10 cryptos per date to a new CSV file
top_10_per_date.to_csv(directory_path+'/0000_top_10_cryptos.csv', index_label='Date')

print("Top 10 cryptocurrencies by market cap per date have been saved.")