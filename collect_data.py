# import the necessary libraries
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd

def get_weather() -> list:
    """ 
    Fetches weather information for ten belgian cities
    Returns a list of dictionaries with the weather conditions for each location
    """
    

    root_url = 'https://api.openweathermap.org'
    endpoint = 'data/2.5/weather'
    api_key = '8e78a2dec9bfb456033d9e0e5922ae6b'
    cities = ['Antwerp', 'Bruges', 'Brussels', 'Charleroi', 'Ghent', 'Leuven', 'Liege', 'Mons', 'Namur', 'Ostend']

    responses = []

    for city in cities:
        params = {
            'q': city,
            'APPID' : api_key,
            'units' : 'metric'    
        }
        response = requests.get(f"{root_url}/{endpoint}", params=params)
        if response.status_code == 200:
            responses.append(response.json())
    return responses


def convert_time(unix_timestamp) -> tuple:
    """ 
    Converts unix timestamp to date and time

    PARAMS
    unix_timestamp -int: timestamp in unix format

    RETURNS
    date, time -tuple: (date, time) in str format
    """

    local_timezone = ZoneInfo("Europe/Brussels")  
    dt = datetime.fromtimestamp(unix_timestamp, local_timezone)

    date = dt.strftime('%Y-%m-%d')
    time = dt.strftime('%H:%M:%S')
    
    return date, time


def extract_info(results):

    unix_timestamp = results.get('dt') 
    sunrise_unix = results.get('sys').get('sunrise')
    sunset_unix = results.get('sys').get('sunset')

    return {
            "id": results.get('id'),
            "city": results.get('name'),
            'longitude': results.get('coord').get('lon'),
            'latitude' : results.get('coord').get('lat'),
            "weather": results.get('weather')[0].get('main'),
            "description": results.get('weather')[0].get('description'),
            "cloud_percentage": results.get('clouds').get('all'),
            "temperature": results.get('main').get('temp'),
            "feels_like": results.get('main').get('feels_like'),
            "min_temp": results.get('main').get('temp_min'),
            "max_temp": results.get('main').get('temp_max'),
            "visibility": results.get('visibility'),
            "wind_speed": round(results.get('wind').get('speed') * 60 * 60 / 1000, 2),
            "wind_deg": results.get('wind').get('deg'),
            "pressure": results.get('main').get('pressure'),
            "humidity": results.get('main').get('humidity'),
            'date': convert_time(unix_timestamp)[0], 
            'time': convert_time(unix_timestamp)[1], 
            "sunrise": convert_time(sunrise_unix)[1],  
            "sunset": convert_time(sunset_unix)[1]     
        }

if __name__ == "__main__":
    results = get_weather()
    df = pd.DataFrame([extract_info(entry) for entry in results])
    df.to_csv('weather_data.csv', mode='a', header=False, index=False)
