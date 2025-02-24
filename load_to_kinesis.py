import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import json
import boto3

"""
Before running this script: 
Create an API key from openweather.org
Enter your API key from openweather.org on the
api_key parameter of function get_weather()

"""

# create client to connect to AWS
kinesis_client = boto3.client('kinesis')

# Function to convert unix timestamps to actual date/time
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


# Function to extract only the relevant data
def extract_info(results):
    """
    Extracts the relevant weather information from the API response

    PARAMS
    results -dict: the response received from the weather API

    RETURNS
    weather_info -dict: weather conditions for a particular city
    """

    unix_timestamp = results.get('dt') 
    sunrise_unix = results.get('sys').get('sunrise')
    sunset_unix = results.get('sys').get('sunset')

    weather_info=  {
            "id": results.get('id'),
            "city": results.get('name'),
            "weather": results.get('weather')[0].get('main'),
            "description": results.get('weather')[0].get('description'),
            "cloud_pct": results.get('clouds').get('all'),
            "temperature": results.get('main').get('temp'),
            "feels_like": results.get('main').get('feels_like'),
            "min_temp": results.get('main').get('temp_min'),
            "max_temp": results.get('main').get('temp_max'),
            "wind_speed": round(results.get('wind').get('speed') * 60 * 60 / 1000, 2),
            "humidity": results.get('main').get('humidity'),
            'date': convert_time(unix_timestamp)[0], 
            'time': convert_time(unix_timestamp)[1], 
            "sunrise": convert_time(sunrise_unix)[1],  
            "sunset": convert_time(sunset_unix)[1]     
        }
    return weather_info


def get_weather() -> list:
    
    """ 
    Fetches weather information for ten belgian cities
    and loads it to an instance of AWS Kinesis
    Returns a list of strings with the weather conditions for each location
    """
    
    root_url = 'https://api.openweathermap.org'
    endpoint = 'data/2.5/weather'
    api_key = '***ENTER YOUR openweather.org API KEY HERE***'
    cities = ['Antwerp', 'Bruges', 'Brussels', 'Charleroi', 'Ghent', 'Leuven', 'Liege', 'Mons', 'Namur', 'Ostend']

    responses = []

    for city in cities:
        params = {
            'q': city,
            'APPID' : api_key,
            'units' : 'metric'    
        }
        # Send request
        response = requests.get(f"{root_url}/{endpoint}", params=params)
        
        if response.status_code == 200:
            response_dict = response.json() # Convert response into a dictionary
            data = extract_info(response_dict) # Extract the relevant data
            
            # Add to list of responses
            responses.append(data)

            # Send to Kinesis:
            add_record = kinesis_client.put_record(
                StreamName='weather_stream',
                Data=json.dumps(data),
                PartitionKey='city')

    return responses

if __name__ == "__main__":
    get_weather()