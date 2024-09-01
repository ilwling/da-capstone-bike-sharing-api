import sqlite3
import requests
from tqdm import tqdm

from flask import Flask, request
import json 
import numpy as np
import pandas as pd

from flask import Flask, request
app = Flask(__name__) 

# Define a function to create connection for reusability purpose
def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

# Make a connection
conn = make_connection()

@app.route('/')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()
    
def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

@app.route('/trips/<id>')
def route_trips_id(id):
    conn = make_connection()
    trip = get_trip_id(id, conn)
    return trip.to_json()

def get_trip_id(id, conn):
    query = f"""SELECT * FROM trips WHERE id = {id}"""
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/json', methods=['POST']) 
def json_example():
    
    req = request.get_json(force=True) # Parse the incoming json data as Dictionary
    
    name = req['name']
    age = req['age']
    address = req['address']
    
    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return query
    conn.commit()
    return 'OK'

@app.route('/trips/average_duration')
def avg_duration_minutes():
    conn = make_connection()
    avg_duration = get_average(conn)
    return avg_duration.to_json()

def get_average(conn):
    query = f"""SELECT AVG(duration_minutes) AS avg_duration_minutes FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/trips/average_duration/<bike_id>')
def average_duration_bikeid(bike_id):
    conn = make_connection()
    avg_duration = get_average_duration_bikeid(bike_id, conn)
    return avg_duration.to_json()

def get_average_duration_bikeid(bike_id, conn):
    query = f"""SELECT AVG(duration_minutes) AS average_duration_bikeid FROM trips WHERE bikeid = '{bike_id}'"""
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/examplepost', methods=['POST']) 
def examplepost():
    input_data = request.get_json() # Get the input as dictionary
    specified_date = input_data['period'] # Select specific items (period) from the dictionary (the value will be "2015-08")
    # Return the result
    conn = make_connection()
    result = functionexample(specified_date, conn)
    return result.to_json()

def functionexample(specified_date, conn):
    # Subset the data with query 
    query = f"SELECT * FROM trips WHERE start_time LIKE ('{specified_date}%')"
    selected_data = pd.read_sql_query(query, conn)

    # Make the aggregate
    result = selected_data.groupby('start_station_id').agg({
        'bikeid' : 'count', 
        'duration_minutes' : 'mean'
    })
    return result

@app.route('/trips/countofdays', methods=['POST']) 
def get_total_bike_start_station_by_day():
    input_data = request.get_json() # Get the input as dictionary
    specified_date = input_data['period'] # get item periode
    id_station = input_data['id_station'] # get item id_station
    # Return the result
    conn = make_connection()
    result = getcountday(specified_date, id_station, conn)
    return result.to_json()

def getcountday(specified_date, id_station, conn):
    query = f"""
        SELECT
        CASE CAST (STRFTIME('%w', SUBSTR( start_time, 1, 10 )) AS integer)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            ELSE 'Saturday' END AS weekday,
        COUNT(id) AS total, start_station_id, start_station_name
        FROM trips WHERE start_time LIKE  ('{specified_date}%')
        AND start_station_id = {id_station}
        GROUP BY start_station_id, weekday
        ORDER BY count(id) DESC
    """
    selected_data = pd.read_sql_query(query, conn)
    return selected_data


if __name__ == '__main__':
    app.run(debug=True, port=5000)