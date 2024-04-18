from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests
from airflow import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.python import PythonOperator

from airflow.hooks.base_hook import BaseHook

import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import boto3
from io import StringIO


def get_weather_data():
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    connection = BaseHook.get_connection('aws_s3')
    aws_access_key_id = connection.login
    aws_secret_access_key = connection.password
    region_name = connection.extra_dejson.get('region_name', 'eu-west-3')

    session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
    )

    S3 = session.client('s3')

    df_data = pd.DataFrame()

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/meteofrance"

    city_coords = [
        {"city": "Lille", "lat": 50.6365654, "lon": 3.0635282},
        {"city": "Paris", "lat": 48.8588897, "lon": 2.320041},
        {"city": "Bordeaux", "lat": 44.841225, "lon": -0.5800364},
        {"city": "Marseille", "lat": 43.2961743, "lon": 5.3699525},
        {"city": "Lyon", "lat": 45.7578137, "lon": 4.8320114},
        {"city": "Toulouse", "lat": 43.6044622, "lon": 1.4442469},
        {"city": "Nantes", "lat": 47.2186371, "lon": -1.5541362},
        {"city": "Montpellier", "lat": 43.610476, "lon": 3.87048},
        {"city": "Calais", "lat": 50.9524769, "lon": 1.8538446},
        {"city": "Dijon", "lat": 47.3215806, "lon": 5.0414701},    
    ]

    for coord in city_coords:

        params = {
            "latitude": coord["lat"],
            "longitude": coord["lon"],
            "hourly": [
                "temperature_2m",
                "wind_speed_10m",
                "wind_direction_10m",
                "sunshine_duration",
                "precipitation"
            ]
        }
        responses = openmeteo.weather_api(url, params=params)

        # Process first location. Add a for-loop for multiple locations or weather models
        response = responses[0]


        # Assume `response` has been correctly defined and obtained earlier
        hourly = response.Hourly()

        # Prepare the time index for the dataframe
        date_range_hourly = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
                )
        # Initialize the dictionary for DataFrame with the date range
        hourly_data = {"date": date_range_hourly}

        # Populate the dictionary for param HOURLY
        for i, hourly_param in enumerate(params["hourly"]):

            hourly_data_array = hourly.Variables(i).ValuesAsNumpy()
            hourly_data[hourly_param] = hourly_data_array
        
        hourly_data["city"] = coord["city"]

        # Convert dictionary to DataFrame
        hourly_dataframe = pd.DataFrame(data=hourly_data)

        df_data = pd.concat([df_data, hourly_dataframe], ignore_index=True)

        csv_buffer = StringIO()
        df_data.to_csv(csv_buffer, index=False)

        csv_buffer.seek(0)

        S3 = boto3.client("s3")
        S3.upload_fileobj(csv_buffer, "weather-data-wcs", "weather.csv")



with DAG(
    dag_id="get_and_store_data_dag",
    start_date=datetime(2024, 4, 16),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    http_sensor_check = HttpSensor(
        task_id="http_sensor_check",
        http_conn_id="open_meteo_api",
        endpoint="v1/meteofrance",
        request_params={
            "latitude": 52.52,
            "longitude": 13.41,
            "hourly": "temperature_2m"
        },
        poke_interval=5,
    )

    get_weather_task = PythonOperator(
        task_id="get_weather_data",
        python_callable=get_weather_data,
    )

    http_sensor_check >> get_weather_task

