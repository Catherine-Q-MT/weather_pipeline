import pytest
from requests_mock.mocker import Mocker

from weather_pipeline.exceptions.not_found import WeatherForLocationNotFoundError
from weather_pipeline.extract.weather import format_url, get_weather_data


class TestWeather:
    @pytest.fixture
    def sample_json_response(self):
        return {"coord": {"lon": -0.1257, "lat": 51.5085},
                "weather": [{"id": 801, "main": "cloudy with a chance of meatballs", "description": "few clouds",
                             "icon": "02d"}],
                "base": "stations",
                "main": {"temp": 8.19, "feels_like": 6.28, "temp_min": 6.86, "temp_max": 10.02, "pressure": 1018,
                         "humidity": 66}, "visibility": 10000, "wind": {"speed": 3.09, "deg": 0}, "clouds": {"all": 20},
                "dt": 1682415691,
                "sys": {"type": 2, "id": 268730, "country": "GB", "sunrise": 1682397876, "sunset": 1682449928},
                "timezone": 3600, "id": 2643743, "name": "London", "cod": 200}

    @pytest.fixture
    def test_url(self):
        return "https://test_url"

    def test_get_weather_returns_response(self, sample_json_response, test_url):
        with Mocker() as m:
            m.get(test_url, json=sample_json_response, status_code=200)
            response = get_weather_data(test_url)

        assert response == sample_json_response

    def test_get_weather_raises_for_non_200_response(self, test_url):
        with pytest.raises(WeatherForLocationNotFoundError):
            with Mocker() as m:
                m.get(test_url, json={}, status_code=404)
                get_weather_data(test_url)

    def test_format_url_returns_expected_url(self):
        main_url = 'https://api.openweathermap.org/data/2.5'
        lat = -1
        lon = 25.5
        api_key = 'xyz'

        expected_result = "https://api.openweathermap.org/data/2.5/weather?lat=-1&lon=25.5&appid=xyz"
        actual_result = format_url(main_url, lat, lon, api_key)
        assert expected_result == actual_result
