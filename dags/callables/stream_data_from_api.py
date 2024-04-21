import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('API_KEY')

def get_api_data(endpoint):
    response = requests.get(f"https://airlabs.co/api/v9/{endpoint}?api_key={API_KEY}")
    response = response.json()
    response = response.get('response')
    return response
    
def format_data(flights_array):
    #gets an array of flights
    flights = []
    flight_data = {}
    
    for fli in flights_array:
        flight_data['hex'] = fli.get('hex',None)
        flight_data['reg_number'] = fli.get('reg_number',None)
        flight_data['flag'] = fli.get('flag',None)
        flight_data['flight_number'] = fli.get('flight_number',None)
        flight_data['flight_icao'] = fli.get('flight_icao',None)
        flight_data['flight_iata'] = fli.get('flight_iata',None)
        flight_data['dep_icao'] = fli.get('dep_icao',None)
        flight_data['dep_iata'] = fli.get('dep_iata',None)
        flight_data["arr_icao"] = fli.get('arr_icao',None)
        flight_data["arr_iata"] = fli.get('arr_iata',None)
        flight_data["airline_icao"] = fli.get('airline_icao',None)
        flight_data["aircraft_icao"] = fli.get('aircraft_icao',None)
        flight_data["type"] = fli.get('type',None)
        
        flights.append(flight_data)
        print(flight_data)
        print(len(flights))
        
    return

def stream_data():
    response = get_api_data('flights')
    format_data(response)
    

stream_data()