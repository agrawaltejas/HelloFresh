"""
This code piece contains the main entry point of the Application.
It consolidates the data from all modules, executes and saves the final output.
"""

import sys
import os

ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '..\\..'))
sys.path.append(ROOT_DIR)

from dependencies.spark import start_spark_session, load_config_file
from src.main.extractor.extract import extract_data
from src.main.transformer.transform import transform_data
from src.main.loader.load import load_data

def main():
    """Main ETL script definition.
    :return: None
    """

    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark_session(
        app_name='etl_job'
        )

    config_files = load_config_file(os.path.join(ROOT_DIR,'config', 'etl_config.json'))

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # start ETL Pipeline
    # Extract data
    input_df = extract_data(spark,config_files['input_path'],log)
    input_df.persist()

    # Transform data
    final_df = transform_data(input_df, log)

    # Load data
    load_data(final_df,config_files['output_path'], log)

    log.warn('etl_job is finished')
    spark.stop()

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
