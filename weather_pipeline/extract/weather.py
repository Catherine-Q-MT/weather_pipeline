from typing import Any, Dict

import requests


class WeatherForLocationNotFoundError(Exception):
    def __init__(self, message="Weather for Location not found"):
        self.message = message
        super().__init__(self.message)


locations = {
    'London': {'lon': -0.1257, 'lat': 51.5085},
    'Bristol': {'lon': -2.5967, 'lat': 51.4552},
    'Manchester': {'lon': -2.2374, 'lat': 53.4809},
    'Glasgow': {'lon': -4.2576, 'lat': 55.8652},
    'Swansea': {'lon': -3.9432, 'lat': 51.6208},
}


def main(api_key):
    main_url = 'https://api.openweathermap.org/data/2.5'
    # data = get_weather_data()

def format_url(main_url, lat, lon, api_key) -> str:
    return f"{main_url}/weather?lat={lat}&lon={lon}&appid={api_key}".format(main_url=main_url, lat=lat, lon=lon,
                                                                            api_key=api_key)


def get_weather_data(url):
    response = requests.get(url)
    if response.status_code != 200:
        raise WeatherForLocationNotFoundError(url)

    return response.json()


if __name__ == "__main__":
    main()
