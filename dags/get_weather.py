import logging
import pendulum
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.weather_fetcher import WeatherAPI

# Configure logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

default_args = {
    'owner': 'anuragthakur',
    'start_date': pendulum.now("UTC").subtract(days=1),
}

# Python function to fetch weather data using the WeatherAPI class
def get_weather():
    try:
        api = WeatherAPI()
        weather = api.fetch_weather()
        
        if 'error' in weather:
            logger.error(f"Error while fetching weather: {weather['error']}")
        else:
            logger.info(f"Weather data retrieved successfully: {weather}")
        
        return weather

    except Exception as e:
        logger.error(f"Failed to fetch weather: {e}")
        return {"error": "Failed to fetch weather data"}

# Define the DAG
with DAG(
    dag_id='get_weather_dag',
    description='DAG to fetch weather information from OpenWeatherMap API',
    default_args=default_args,
    schedule_interval='@daily',  # Runs once every day
    tags=['weather', 'api', 'python']
) as dag:

    # Define the task to get weather
    weather_task = PythonOperator(
        task_id='get_weather',
        python_callable=get_weather, 
        dag=dag
    )
