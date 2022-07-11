"""
etl_tests.py
------------
This code piece is to test the functions and methods created for the pipeline
We are testing the transformer part in this file, as it includes extract method also.
Last step is load, writing to csv.

Tests have been written with Python unitest case methods. Using setUpClass and tearDown
"""


import os
import sys

ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '..\\..'))
sys.path.append(ROOT_DIR)

import unittest
from pyspark.sql.types import *
from dependencies.spark import start_spark_session
from dependencies.spark import load_config_file
from src.main.extractor.extract import extract_data
from src.main.transformer.transform import transform_data


class SparkETLTestCase(unittest.TestCase):
    """Test suite for extraction method"""

    @classmethod
    def setUpClass(self):

        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        """Start Spark, define config and path to test data"""
        self.spark, self.log, self.config = start_spark_session('Unit-Tests')
        self.config_file = load_config_file(os.path.join(ROOT_DIR, 'src', 'test', 'test_data', 'config', 'etl_config.json'))

    @classmethod
    def tearDown(self):
        """Stop Spark"""
        self.spark.stop()

    # def test_extract_method(self):
    #     input_rows = self.input_df.count()
    #     self.assertEqual(6,input_rows)

    def test_transform_method(self):

        input_df = extract_data(self.spark,self.config_file['input_path'], self.log)
        transformed_df = transform_data(input_df,self.log)

        # transformed_df2 = transformed_df.select('difficulty','avg_total_cook_time')

        expected_schema = StructType([
            StructField('difficulty', StringType(), True),
            StructField('avg_total_cook_time', DoubleType(), True)
        ])

        expected_data = [('easy', 20.5)]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        expected_df.show()

        #4. Assert the output of the transformation to the expected data frame.
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        # Compare schema of transformed_df and expected_df
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)

        # assert
        self.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))


if __name__ == '__main__':
    unittest.main()