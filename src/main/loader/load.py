"""
load.py
------------
This code piece is to load the data from the spark dataframe to csv file in output.
For big piece of codes, we need separate extractor,transformer,loader classes to define the functionalities clearly.
"""

import os
ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '..\\..\\..'))


def load_data(transformed_df, output_path, log):
    """Load data from spark dataframe to CSV output location
    :param transformed_df: Datafrmae obtained after transforming the data
    :param output_path : Location for the output path where to write csv file
    :param log : log session available in start_spark
    :return: None
    """
    try:
        not transformed_df.coalesce(1)\
                .write.option("header",True)\
                .option("delimiter","|")\
                .csv(os.path.join(ROOT_DIR, output_path))
        log.warn("Successfully loaded final dataframe to output location")

    except FileNotFoundError:
        raise FileNotFoundError(f'{os.path.join(ROOT_DIR, output_path)} Not found')
        log.error("f'{output_path} Not found'")





