import logging
import pendulum
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.weather_fetcher import WeatherAPI

# Configure logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'anuragthakur',
    'start_date': pendulum.now("UTC").subtract(days=1),
}

# Python function to fetch weather data using the WeatherAPI class
def get_weather_data():
    try:
        api_key = Variable.get("open_weather_api_key")
        api = WeatherAPI(api_key=api_key)
        weather = api.fetch_weather()
        
        if 'error' in weather:
            logger.error(f"Error while fetching weather: {weather['error']}")
        else:
            logger.info(f"Weather data retrieved successfully: {weather}")
        
        return weather

    except Exception as e:
        logger.error(f"Failed to fetch weather: {e}")
        return {"error": "Failed to fetch weather data"}

def format_weather_report(ti):
    value = ti.xcom_pull(task_ids='get_weather')
    
    if not value or 'error' in value:
        logger.warning("No valid weather data to format.")
        return "Weather data unavailable."
    
    logger.info("Weather data formatted correctly!")
    return (
        f"In {value['city']}, the weather is currently {value['temperature']}°C "
        f"with {value['description']}, {value['humidity']}% humidity, and a pressure of {value['pressure']} hPa."
    )
    

# Define the DAG
with DAG(
    dag_id='get_weather_dag_xcom',
    description='DAG to fetch weather information from OpenWeatherMap API',
    default_args=default_args,
    schedule_interval='@daily',  # Runs once every day
    tags=['weather', 'api', 'python', 'xcom']
) as dag:

    # Define the task to get weather
    weather_task = PythonOperator(
        task_id='get_weather',
        python_callable=get_weather_data, 
        dag=dag
    )
    
    format_weather_task = PythonOperator(
        task_id='format_weather',
        python_callable=format_weather_report,
        dag=dag
    )

weather_task >> format_weather_task
