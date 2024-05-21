import pandas as pd
import os
import numpy as np
from statsmodels.tsa.ar_model import AutoReg
from statsmodels.tsa.stattools import adfuller
folder_path = r'C:\Users\sandr\PycharmProjects\Webscraper\Data\Price History'
output_file = r'C:\Users\sandr\PycharmProjects\Webscraper\optimal_lags.csv'
results = []


def find_optimal_lag(file_path, min_sample_size=100):  # Set a minimum sample size
    df = pd.read_csv(file_path, parse_dates=['Open Time'], index_col='Open Time')
    df['Return'] = df['Close'].pct_change().ffill()

    # Check if the sample size is too short
    if len(df['Return']) < min_sample_size:
        return None  # Sample size too short, return None

    # Check for stationarity
    adf_test = adfuller(df['Return'][1:])
    if adf_test[1] > 0.05:  # Non-stationary series
        return None

    max_lag = 30  # Adjust based on your preference
    best_lag = None
    best_aic = np.inf
    for lag in range(1, max_lag + 1):
        try:
            model = AutoReg(df['Return'], lags=lag, old_names=False)
            model_fitted = model.fit()
            if model_fitted.aic < best_aic:
                best_aic = model_fitted.aic
                best_lag = lag
        except:
            continue

    return best_lag


for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        filepath = os.path.join(folder_path, filename)
        optimal_lag = find_optimal_lag(filepath)
        # Record the optimal lag or None if sample size is too short or series is non-stationary
        results.append({'Crypto': filename.replace('.csv', ''), 'Optimal Lag': optimal_lag})

pd.DataFrame(results).to_csv(output_file, index=False)
print(f'Optimal lags saved to {output_file}.')