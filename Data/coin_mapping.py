import os
import pandas as pd
from datetime import datetime


folder1 = r'C:\Users\sandr\PycharmProjects\Webscraper\Data\Price History'  # Folder with original .csv files
folder2 = r'C:\Users\sandr\PycharmProjects\Webscraper\Data\Crypto Market Caps' # Folder with .csv files to join
destination=r'C:\Users\sandr\PycharmProjects\Webscraper\Data\Merged'

unmatched_files = []  # To record files in folder1 without a match in folder2

for filename in os.listdir(folder1):
    print(filename)
    if filename.endswith(".csv"):
        filepath1 = os.path.join(folder1, filename)
        filepath2 = os.path.join(folder2, filename)

        # Check for the corresponding file in folder2
        if os.path.exists(filepath2):
            # Read the CSV file from folder1
            df1 = pd.read_csv(filepath1, index_col='Open Time', parse_dates=True)
            # Ensure datetime index for df1 has no time component for proper join
            df1.index = df1.index.date

            # Read the CSV file from folder2
            df2 = pd.read_csv(filepath2)
            # Convert 'Date' column to datetime format and set as index
            df2['Date'] = pd.to_datetime(df2['Date'])
            df2.set_index('Date', inplace=True)

            # Perform a left-outer join, joining on date
            combined_df = df1.join(df2, how='left', rsuffix='_r')

            # Fill missing values by carrying the last known value forward
            combined_df.fillna(method='ffill', inplace=True)

            # Save the combined DataFrame back to a file in the first folder
            combined_df.to_csv(destination)
        else:
            # If there's no matching file, add to the list of unmatched files
            unmatched_files.append(filename)
    '''
    except Exception as e:
        print(filename, e)
        unmatched_files.append(destination)
    '''

counter=0
# Save the list of unmatched files to a text file
with open(r'C:\Users\sandr\PycharmProjects\Webscraper\Data\unmatched_files.txt', 'w') as f:
    for file in unmatched_files:
        counter+=1
        f.write(f"{file}\n")

print(counter)