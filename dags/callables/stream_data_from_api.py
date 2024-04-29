import requests
import json
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()
API_KEY = os.getenv('API_KEY')

def get_api_data(endpoint):
    response = requests.get(f"https://airlabs.co/api/v9/{endpoint}?api_key={API_KEY}")
    response = response.json()
    response = response.get('response')
    return response if isinstance(response, list) else []
      
def format_data(flights_array):
    flights = []
    fli = {}
    
    for fli in flights_array:
        
        flight_data = {}
        flight_data['hex'] = fli.get('hex',None)
        flight_data['reg_number'] = fli.get('reg_number',None)
        flight_data['flag'] = fli.get('flag',None)
        flight_data['flight_number'] = fli.get('flight_number',None)
        flight_data['flight_icao'] = fli.get('flight_icao',None)
        flight_data['flight_iata'] = fli.get('flight_iata',None)
        flight_data['lat'] = fli.get('lat',None)
        flight_data['lng'] = fli.get('lng',None)
        flight_data['alt'] = fli.get('alt',None)
        flight_data['dir'] = fli.get('dir',None)
        flight_data['speed'] = fli.get('speed',None)
        flight_data['dep_icao'] = fli.get('dep_icao',None)
        flight_data['dep_iata'] = fli.get('dep_iata',None)
        flight_data["arr_icao"] = fli.get('arr_icao',None)
        flight_data["arr_iata"] = fli.get('arr_iata',None)
        flight_data["airline_icao"] = fli.get('airline_icao',None)
        flight_data["aircraft_icao"] = fli.get('aircraft_icao',None)
        flight_data["type"] = fli.get('type',None)
        
        flights.append(flight_data)
        
    return flights

def format_airports(airports_array):
    airports = []
    airprt = {}
    
    for airprt in airports_array:
        airport_data = {}
        airport_data['name'] = airprt.get('name',None)
        airport_data['iata_code'] = airprt.get('iata_code',None)
        airport_data['iaco_code'] = airprt.get('iaco_code',None)
        airport_data['lat'] = airprt.get('lat',None)
        airport_data['lng'] = airprt.get('lng',None)
        airport_data['city'] = airprt.get('city',None)
        airport_data['city_code'] = airprt.get('city_code',None)
        airport_data['un_locode'] = airprt.get('un_locode',None)
        airport_data['timezone'] = airprt.get('timezone',None)
        airport_data['country_code'] = airprt.get('country_code',None)
        airport_data['webzone'] = airprt.get('webzone',None)
        airport_data['is_international'] = airprt.get('is_international',None)
        
        airports.append(airport_data)
    return airports
        
def stream_data():
    response = get_api_data('flights')
    res = format_data(response)
    producer = KafkaProducer(bootstrap_servers=['broker:29092'],max_block_ms=5000)
    for obj in res:
        producer.send('flights',json.dumps(obj).encode('utf-8'))

def stream_airports():
    response = get_api_data('airports')
    res = format_airports(response)
    producer = KafkaProducer(bootstrap_servers=['broker:29092'],max_bloc_ms=5000)
    for obj in res:
        producer.send('airports',json.dumps(obj).encode('utf-8'))

stream_data()