import json
import os.path
import tempfile
from pathlib import Path

import pytest
from pyspark.shell import spark
from pyspark.sql import functions as f

from weather_pipeline.city_mapping import read_json_from_s3, convert_dict_to_data_frame, explode_coordinates_map, \
    write_to_parquet_overwriting_previous_data


class TestCityMapping:
    @pytest.fixture
    def sample_data(self):
        return [
            {
                "id": 2643741,
                "name": "City of London",
                "state": "",
                "country": "GB",
                "coord": {
                    "lon": -0.09184,
                    "lat": 51.512791
                }
            },
        ]

    @pytest.fixture
    def sample_dataframe(self, sample_data):
        return spark.createDataFrame(data=sample_data)

    @pytest.fixture
    def sample_json(self, sample_data):
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, 'city_list.json')
            data = sample_data
            with open(file_path, 'w') as f:
                json.dump(data, f)

            yield file_path

    def test_load_json(self, sample_json, sample_data):
        data_in_file = read_json_from_s3(sample_json)

        assert sample_data == data_in_file

    def test_convert_dict_to_data_frame(self, sample_data, sample_json):
        df = convert_dict_to_data_frame(sample_data)
        rows = df.collect()[0]
        assert rows.coord == {'lon': -0.09184, 'lat': 51.512791}
        assert rows.country == 'GB'
        assert rows.name == 'City of London'
        assert rows.state == ''

    def test_explode_coordinates_map(self, sample_dataframe):
        df = explode_coordinates_map(df=sample_dataframe, map_col_name='coord',
                                     latitude_key_name='lat', longitude_key_name='lon')
        rows = df.collect()[0]
        assert rows.coord == {'lon': -0.09184, 'lat': 51.512791}
        assert rows.country == 'GB'
        assert rows.name == 'City of London'
        assert rows.state == ''
        assert rows.lat == 51.512791
        assert rows.lon == -0.09184

    def test_write_to_parquet(self, sample_dataframe):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir =Path(tmpdir)/'output'

            write_to_parquet_overwriting_previous_data(sample_dataframe, str(output_dir))
            assert output_dir.is_dir()
            assert (output_dir/'_SUCCESS').exists()
