"""
extract.py
------------
This code piece is to just extract the data from the source and persist it to a data frame.
For big piece of codes, we need separate extractor,transformer,loader classes to define the functionalities clearly.
"""
from functools import reduce

from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql import Row
from pyspark.sql.types import *
import os
ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '..\\..\\..'))


def extract_data(spark, input_path, log):
    """Load data from CSV file format.
    :param spark: Spark session object.
    :param input_path : Location for the input path where csv files are present
    :param log : log session available in start_spark
    :return: Spark DataFrame.
    """

    try :
        filerange=[]
        for file in os.listdir(os.path.join(ROOT_DIR, input_path)) :
            filerange.append(f"{file}")

        dataframes = map(lambda r: spark.read.json(os.path.join(ROOT_DIR,input_path,r)),filerange)
        input_df = reduce(lambda df1, df2: df1.unionAll(df2), dataframes)

        log.warn("Dataframe successfully fetched from input csv location")
    except FileNotFoundError:
        raise FileNotFoundError(f'{os.path.join(ROOT_DIR, input_path)} Not found')
        log.error("f'{output_path} Not found'")

    return input_df






