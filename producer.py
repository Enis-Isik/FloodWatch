import time
import json
import requests
from kafka import KafkaProducer

# CONFIGURATION
KAFKA_TOPIC = 'flood_data'
BOOTSTRAP_SERVERS = 'localhost:9092'
OWM_API_KEY = '9905f8b5944ce3f364bf91041715d3da'

SENSORS = [
    {
        "id": "dc_potomac",
        "name": "Washington, D.C. (Potomac River)",
        "lat": 38.8951, "lon": -77.0364,
        "type": "usgs", "site_id": "01646500",
        "flood_threshold": 25700.00 
    },
    {
        "id": "stl_mississippi",
        "name": "St. Louis, MO (Mississippi River)",
        "lat": 38.6270, "lon": -90.1994,
        "type": "usgs", "site_id": "07010000",
        "flood_threshold": 18.74 
    },
    {
        "id": "nola_mississippi",
        "name": "New Orleans, LA (Mississippi River)",
        "lat": 29.9511, "lon": -90.0715,
        "type": "usgs", "site_id": "07374000",
        "flood_threshold": 39.71 
    },
    {
        "id": "moz_maputo",
        "name": "Maputo, Mozambique (Maputo River)",
        "lat": -26.1700, "lon": 32.4200,
        "type": "open_meteo",
        "flood_threshold": 2.99 
    },
    {
        "id": "swe_kristianstad",
        "name": "Kristianstad, Sweden (Helge å)",
        "lat": 56.0294, "lon": 14.1567,
        "type": "open_meteo",
        "flood_threshold": 0.24 
    }
]

def get_current_rain_owm(lat, lon):
    try:
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OWM_API_KEY}"
        data = requests.get(url).json()
        return data.get('rain', {}).get('1h', 0.0)
    except: return 0.0

def get_historical_rain_om(lat, lon):
    try:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {"latitude": lat, "longitude": lon, "hourly": "rain", "past_days": 1, "forecast_days": 0}
        r = requests.get(url, params=params).json()
        return sum(r.get('hourly', {}).get('rain', []))
    except: return 0.0

def get_water_level(sensor, rain_24h):
    try:
        if sensor['type'] == 'usgs':
            url = f"https://waterservices.usgs.gov/nwis/iv/?format=json&sites={sensor['site_id']}&parameterCd=00065"
            r = requests.get(url).json()
            return float(r['value']['timeSeries'][0]['values'][0]['value'][0]['value'])
        
        elif sensor['type'] == 'open_meteo':
            url = "https://flood-api.open-meteo.com/v1/flood"
            params = {"latitude": sensor['lat'], "longitude": sensor['lon'], "daily": "river_discharge", "forecast_days": 1}
            r = requests.get(url, params=params).json()
            discharge = r['daily']['river_discharge'][0]

            # Fallback for Maputo if sensor blind but it's raining
            if discharge < 0.1 and rain_24h > 10.0:
                print(f" {sensor['name']} Simulated Level (due to heavy rain)")
                return (rain_24h / 5.0) + 1.0
            return discharge 
    except: return 0.0

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"🌊 FloodWatch Hybrid Producer Active. Monitoring {len(SENSORS)} cities.")

    while True:
        for sensor in SENSORS:
            rain_1h = get_current_rain_owm(sensor['lat'], sensor['lon'])
            rain_24h = get_historical_rain_om(sensor['lat'], sensor['lon'])
            level = get_water_level(sensor, rain_24h)
            
            data = {
                "timestamp": time.time(),
                "location_id": sensor['id'], "location_name": sensor['name'],
                "lat": sensor['lat'], "lon": sensor['lon'],
                "water_level": level, "flood_threshold": sensor['flood_threshold'],
                "rain_1h": rain_1h, "rain_24h": rain_24h
            }
            producer.send(KAFKA_TOPIC, data)
            print(f"Sent: {sensor['name']} | Lvl: {level:.2f} | Rain 1h: {rain_1h} | Rain 24h: {rain_24h:.2f}")
        
        print("-" * 30)
        time.sleep(15)

if __name__ == "__main__":
    run_producer()
