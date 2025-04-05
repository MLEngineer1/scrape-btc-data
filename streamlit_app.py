import os
import time
import pandas as pd
import requests
import streamlit as st
from datetime import datetime, timedelta, timezone

# Function to fetch data from Bitstamp API
def fetch_bitstamp_data(currency_pair, start_timestamp, end_timestamp, step=900, limit=1000):
    """
    Fetch OHLC data from Bitstamp API.
    
    Args:
        currency_pair: Trading pair to fetch (e.g. 'btcusd')
        start_timestamp: Start time as Unix timestamp
        end_timestamp: End time as Unix timestamp
        step: Time interval in seconds (900 = 15-minute interval)
        limit: Maximum number of data points per request
        
    Returns:
        List of OHLC data points
    """
    url = f"https://www.bitstamp.net/api/v2/ohlc/{currency_pair}/"
    params = {
        "step": step,  # 15 minutes (900 seconds)
        "start": start_timestamp,
        "end": end_timestamp,
        "limit": limit,  # Fetch 1000 data points max per request
    }
    try:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        return response.json().get("data", {}).get("ohlc", [])
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data: {e}")
        return []

# Fetch the data and return as a pandas dataframe
def get_bitcoin_data(start_date, end_date):
    # Convert start and end dates to Unix timestamps
    start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
    end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())

    currency_pair = "btcusd"
    data = []

    # Fetch data in chunks (Bitstamp API limits)
    while start_timestamp < end_timestamp:
        chunk_data = fetch_bitstamp_data(currency_pair, start_timestamp, end_timestamp)
        if chunk_data:
            chunk_df = pd.DataFrame(chunk_data, columns=["Timestamp", "Open", "High", "Low", "Close", "Volume"])
            data.append(chunk_df)
            start_timestamp = chunk_data[-1]["timestamp"]  # Update start time for next chunk
        else:
            break  # No data fetched, exit loop

    if data:
        full_data = pd.concat(data, ignore_index=True)
        full_data["Timestamp"] = pd.to_datetime(full_data["Timestamp"], unit="s")
        return full_data
    else:
        return pd.DataFrame()

# Streamlit app
def main():
    st.title("Bitcoin Historical Data Fetcher")
    
    # Date inputs for start and end date
    start_date = st.date_input("Start Date", datetime(2022, 1, 1))
    end_date = st.date_input("End Date", datetime(2025, 1, 1))
    
    if st.button("Fetch Data"):
        st.write(f"Fetching data from {start_date} to {end_date}...")
        
        # Fetch the data
        data = get_bitcoin_data(str(start_date), str(end_date))
        
        if not data.empty:
            st.write(f"Fetched {len(data)} rows of data.")
            
            # Show a preview of the data
            st.write(data.head())
            
            # Allow user to download the data as CSV
            csv = data.to_csv(index=False)
            st.download_button("Download CSV", csv, "bitcoin_data.csv", "text/csv")
        else:
            st.error("No data fetched. Please check the date range.")

if __name__ == "__main__":
    main()
