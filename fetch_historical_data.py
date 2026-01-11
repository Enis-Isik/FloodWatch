import pandas as pd
import requests
import numpy as np
import datetime
import time

print("⏳ Fetching historical data using direct APIs...")

# Configuration
end_date = datetime.date.today().strftime("%Y-%m-%d")
start_date = (datetime.date.today() - datetime.timedelta(days=365)).strftime("%Y-%m-%d")

def fetch_open_meteo(lat, lon, city_name):
    """Fetches global data. Includes FALLBACK for Maputo if river model returns 0."""
    print(f"   📍 Fetching {city_name} (Open-Meteo)...")
    try:
        # Weather rain data
        url_weather = "https://archive-api.open-meteo.com/v1/archive"
        params_w = {
            "latitude": lat, "longitude": lon,
            "start_date": start_date, "end_date": end_date,
            "hourly": "rain"
        }
        r_w = requests.get(url_weather, params=params_w).json()
        rain_values = np.array(r_w['hourly']['rain'])
        
        # Flood or River Discharge data
        url_flood = "https://flood-api.open-meteo.com/v1/flood"
        params_f = {
            "latitude": lat, "longitude": lon,
            "start_date": start_date, "end_date": end_date,
            "daily": "river_discharge"
        }
        r_f = requests.get(url_flood, params=params_f).json()
        river_daily = np.array(r_f['daily']['river_discharge'])
        
        # Expand river daily to hourly by repeating each value 24 times
        river_hourly = np.repeat(river_daily, 24)
        
        # align lengths
        min_len = min(len(rain_values), len(river_hourly))
        
        df = pd.DataFrame()
        df['city'] = [city_name] * min_len
        df['rain_1h'] = rain_values[:min_len]
        df['water_level'] = river_hourly[:min_len]
        
        if df['water_level'].max() < 0.1:
            print(f"      ⚠️ {city_name} River data is empty/zero. Simulating based on Rain History.")
            # Heuristic: River level = Rolling sum of rain (3-day lag)
            df['water_level'] = df['rain_1h'].rolling(72).sum().fillna(0) * 0.1
        
        return df
    except Exception as e:
        print(f"   ❌ Error {city_name}: {e}")
        return pd.DataFrame()

def fetch_usgs_direct(site_id, city_name, lat, lon):
    """Fetches USGS data using DIRECT JSON API (Bypassing libraries)"""
    print(f"   📍 Fetching {city_name} (USGS Direct)...")
    try:
        # Fetch Daily Value for Gage Height or discharge 
        url = "https://waterservices.usgs.gov/nwis/dv/?format=json"
        params = {
            "sites": site_id,
            "startDT": start_date,
            "endDT": end_date,
            "parameterCd": "00065,00060", # Try both
            "siteStatus": "all"
        }
        r = requests.get(url, params=params)
        data = r.json()
        
        # Parse USGS JSON structure
        ts_list = data['value']['timeSeries']
        if not ts_list:
            print(f"      ⚠️ No data found for {city_name}")
            return pd.DataFrame()
            
        # Prefer 00065 (Height), fallback to 00060 (Discharge)
        selected_ts = None
        for ts in ts_list:
            if "00065" in ts['variable']['variableCode'][0]['value']:
                selected_ts = ts
                break
        if not selected_ts:
            selected_ts = ts_list[0] # Take whatever is there
            
        # Extract values
        values = [float(item['value']) for item in selected_ts['values'][0]['value']]
        
        # Create dataframe
        df = pd.DataFrame()
        df['city'] = [city_name] * len(values)
        df['water_level'] = values
        
        # Expand to Hourly (Repeat each daily value 24 times)
        df = df.loc[df.index.repeat(24)].reset_index(drop=True)
        
        # fetch rain for US City from Open-Meteo
        url_w = "https://archive-api.open-meteo.com/v1/archive"
        params_w = {"latitude": lat, "longitude": lon, "start_date": start_date, "end_date": end_date, "hourly": "rain"}
        r_w = requests.get(url_w, params_w).json()
        rain_vals = r_w['hourly']['rain']
        
        # Trim to match
        min_len = min(len(df), len(rain_vals))
        df = df.iloc[:min_len].copy()
        df['rain_1h'] = rain_vals[:min_len]
        
        return df
        
    except Exception as e:
        print(f"   ❌ Error {city_name}: {e}")
        return pd.DataFrame()

# Main
datasets = []

# Maputo (Fixed Coords & Simulation Logic)
datasets.append(fetch_open_meteo(-26.17, 32.42, "Maputo"))

# Kristianstad
datasets.append(fetch_open_meteo(56.03, 14.16, "Kristianstad"))

# US Cities
datasets.append(fetch_usgs_direct("01646500", "Washington DC", 38.90, -77.04))
datasets.append(fetch_usgs_direct("07010000", "St. Louis", 38.63, -90.20))
datasets.append(fetch_usgs_direct("07374000", "New Orleans", 29.95, -90.07))
datasets.append(fetch_usgs_direct("14128870", "Portland", 45.52, -122.68))

# Combine
full_df = pd.concat(datasets)

# auto lableing (95th Percentile)
print("\n📊 Auto-Labeling Real Data...")

# Calculate Rain 24h
full_df['rain_24h'] = full_df.groupby('city')['rain_1h'].transform(lambda x: x.rolling(24).sum().fillna(0))

def auto_label(group):
    # Find the local "High Water" mark (Top 5% of year)
    threshold = group['water_level'].quantile(0.95)
    
    # Safety: If flatline 0 threshold is 0 then avoid division
    if threshold == 0: threshold = 1.0 
    
    # Label 1 if above threshold
    group['label'] = (group['water_level'] > threshold).astype(float)
    group['level_percent'] = group['water_level'] / threshold
    
    # Output stats for user to put in producer.py
    print(f"   > {group['city'].iloc[0]}: Flood Threshold (95%) = {threshold:.2f}")
    return group

final_df = full_df.groupby('city').apply(auto_label)
final_df = final_df.reset_index(drop=True)
final_df = final_df[['city', 'rain_1h', 'water_level', 'rain_24h', 'level_percent', 'label']]

final_df.to_csv("real_flood_history.csv", index=False)
print(f"\n✅ Success! Saved {len(final_df)} rows. Check the thresholds above!")