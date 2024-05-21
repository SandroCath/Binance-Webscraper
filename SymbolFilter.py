from binance.client import Client
import os

api_key = 'Y6ipHw0QMDSZLGrguqriY4oCRHABbDBjucc4ZJdOLE7z0NolaRunt5PChGy7V3js'
api_secret = 'xJHUNTPGWo7mSH3Uc9RJy4jJwFsLTfuyIZx8iGdPBy8AmZnwEFZkQqffhSVivNnY'

client = Client(api_key, api_secret)

# Fetch exchange information
info = client.get_exchange_info()

# Extract symbol information into a dictionary
symbols_status = {symbol['symbol']: symbol['status'] for symbol in info.get('symbols', [])}

# Specify the folder to check for CSV files
folder_path = r"C:\Users\sandr\PycharmProjects\Webscraper\Data\Price History"

# Iterate through files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        # Extract symbol name from the filename
        symbol_name = filename.replace('.csv', '')

        # Check if the symbol is on 'BREAK' status
        if symbols_status.get(symbol_name) == 'BREAK':
            # Construct full file path
            file_path = os.path.join(folder_path, filename)
            # Delete the file
            os.remove(file_path)
            print(f"Deleted {file_path} because its status is 'BREAK'.")