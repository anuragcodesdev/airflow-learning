import os
import logging
import yaml
import requests

os.environ["no_proxy"]="*"
class WeatherAPI:
    """
    A class to interact with the OpenWeatherMap API to fetch weather data from the
    desired city.
    
    Params:
        :param api_key (str): API key for OpenWeatherMap.
        :param city (str): City for which weather data is fetched.
        :base_url (str): Base URL for OpenWeatherMap API.
    """
    
    def __init__(self, city: str = 'Melbourne') -> None:
        """
        Initialise the WeatherAPI instance.
        
        Params:
            :param city (str): City for which weather data is fetched.
        
        Exceptions:
            :raises  EnvironmentError: If WEATHER_API_KEY is not set in environment 
            variables.
        """
        self.api_key = ''
        self.city = city
        self.base_url = "http://api.openweathermap.org/data/2.5/weather"
        
        if not self.api_key:
            raise EnvironmentError("WEATHER_API_KEY environment variable not set")

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
    
    
    def fetch_weather(self):
        """
        Fetches weather data from OpenWeatherMap API.

        Returns:
            dict: Weather data if successful, error message if failed.
        """
        params = {
            "q": self.city,
            "appid": self.api_key,
            "units": "metric"
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status() 

            data = response.json()
            if response.status_code != 200:
                return {"error": data.get("message", "Failed to retrieve weather data")}

            main = data.get("main", {})
            weather = {
                "city": self.city,
                "temperature": main.get("temp"),
                "humidity": main.get("humidity"),
                "pressure": main.get("pressure"),
                "description": data.get("weather", [{}])[0].get("description")
            }

            return weather

        except requests.RequestException as e:
            return {"error": "Failed to retrieve weather data due to network issues"}
        except Exception as e:
            return {"error": "Unexpected error occurred"}
