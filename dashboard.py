import streamlit as st
import pandas as pd
import glob
import os
import time
import pydeck as pdk

st.set_page_config(layout="wide", page_title="FloodWatch Global")

# PATHS
OUTPUT_DIR = "./prediction_output/*.json"

def load_data():
    """Reads the JSON files to get the LATEST status for EACH city"""
    list_of_files = glob.glob(OUTPUT_DIR)
    if not list_of_files:
        return pd.DataFrame()
    
    # Sort by time to get latest
    latest_files = sorted(list_of_files, key=os.path.getctime)[-50:] 
    
    all_records = []
    for file in latest_files:
        try:
            with open(file, 'r') as f:
                for line in f:
                    all_records.append(pd.read_json(line, lines=True, orient='records').iloc[0])
        except:
            continue
            
    if not all_records:
        return pd.DataFrame()
        
    df = pd.DataFrame(all_records)
    return df.groupby('location_name').tail(1).reset_index(drop=True)

# UI layout

st.title("🌍 FloodWatch: Global Monitoring System")
st.markdown("Real-time flood prediction using **Kafka, Spark, and Live APIs**.")

# Sidebar 
df = load_data()

if df.empty:
    st.warning("Waiting for data stream... (Start Producer & Spark)")
    st.stop()

# Sidebar options
city_options = ["Overview (All Cities)"] + df['location_name'].unique().tolist()
selected_view = st.sidebar.selectbox("Select Monitor View", city_options)

# main view
if selected_view == "Overview (All Cities)":
    st.subheader("System Status: All Sensors")
    
    # Styled Table
    # We rename columns to be pretty for the table
    display_df = df[['location_name', 'water_level', 'rain_1h', 'rain_24h', 'risk_status']].copy()
    display_df.columns = ['City', 'Water Level', 'Rain (1h)', 'Rain (24h)', 'Status']
    
    def color_risk(val):
        color = 'red' if val == 'DANGER' else 'green'
        return f'background-color: {color}; color: white'

    st.dataframe(display_df.style.applymap(color_risk, subset=['Status']), use_container_width=True)
    
    # Map of ALL cities
    df['color'] = df['risk_status'].apply(lambda x: [255, 0, 0, 200] if x == 'DANGER' else [0, 128, 255, 200])
    
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position="[lon, lat]",
        get_color="color",
        get_radius=30000, 
        pickable=True,
    )
    # Zoom out to see the world
    view_state = pdk.ViewState(latitude=10, longitude=0, zoom=1)
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip={"text": "{location_name}\nStatus: {risk_status}"}))

else:
    # Single city view
    city_data = df[df['location_name'] == selected_view].iloc[0]
    
    st.subheader(f"📍 Live Feed: {city_data['location_name']}")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Water Level", f"{city_data['water_level']:.2f}")
    col2.metric("Rain (Last 1h)", f"{city_data['rain_1h']:.2f} mm")
    col3.metric("Rain (Last 24h)", f"{city_data['rain_24h']:.2f} mm")
    
    if city_data['risk_status'] == 'DANGER':
        col4.error("🚨 HIGH RISK")
        map_color = [255, 0, 0, 200]
        msg = "⚠️ **CRITICAL WARNING:** flooding conditions predicted."
    else:
        col4.success("✅ SAFE")
        map_color = [0, 128, 255, 200]
        msg = "✅ Conditions are predicted to be stable for the next 2 hours."

    st.markdown(msg)

    # Zoomed in Map
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=pd.DataFrame([city_data]),
        get_position="[lon, lat]",
        get_color=map_color,
        get_radius=5000,
        pickable=True,
        pulsing=True if city_data['risk_status'] == 'DANGER' else False
    )
    view_state = pdk.ViewState(latitude=city_data['lat'], longitude=city_data['lon'], zoom=9)
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state))

time.sleep(2)
st.rerun()