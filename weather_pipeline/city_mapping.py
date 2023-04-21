import argparse
import json
from typing import Dict, Any

import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, MapType, FloatType


def main(source_filepath: str, destination_filepath: str):
    spark = SparkSession.builder.appName("city_processing_script").getOrCreate()
    json_data = read_json_from_s3(source_filepath)
    df = convert_dict_to_data_frame(json_data, spark)
    df_with_exploded_values = explode_coordinates_map(df, 'coord', 'lat', 'lon')
    write_to_parquet_overwriting_previous_data(df_with_exploded_values, destination_filepath)


def read_json_from_s3(s3_path: str) -> Dict[Any, Any]:
    print('1')
    with open(s3_path) as f:
        data = json.load(f)

        return data


def convert_dict_to_data_frame(data: Dict[Any, Any], spark) -> pyspark.sql.DataFrame:
    print('2')
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("state", StringType(), True),
        StructField("coord", MapType(StringType(), FloatType()), True),
    ])
    df = spark.createDataFrame(data=data, schema=schema)
    return df


def explode_coordinates_map(df, map_col_name, latitude_key_name, longitude_key_name):
    print('3')

    df=df.withColumn(latitude_key_name, f.col(map_col_name).getItem(latitude_key_name)).withColumn(
        longitude_key_name, f.col(map_col_name).getItem(longitude_key_name))
    df.show()
    print(df.schema)
    return df


def write_to_parquet_overwriting_previous_data(df, output_dir, partition_keys=[]):
    print('4')

    df.write.mode("overwrite").partitionBy(*partition_keys).parquet(output_dir)


def collect_arguments(*args):
    parser = argparse.ArgumentParser()
    for arg in args:
        parser.add_argument(
            arg[0],
            help=arg[1],
            required=True,
        )

    parsed_args, _ = parser.parse_known_args()

    return (vars(parsed_args)[arg[0][2:]] for arg in args)


if __name__ == "__main__":

    main(source_filepath="s3://weatherpipeline/city_list.json", destination_filepath="s3://weatherpipeline/output")

    print("Spark job 'cit ref data load' complete")
