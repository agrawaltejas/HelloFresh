"""
transform.py
------------
This code piece is to transform the data from the persisted input_df created after extracting the data from source.
For big piece of codes, we need separate extractor,transformer,loader classes to define the functionalities clearly.
"""
import logging

from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as sql_fun


def transform_data(input_df,log):
    """Transform data on top of Input dataframe
    :param input_df : Input Dataframe contained after extracting the data from input location
    :param log : log session available in start_spark
    :return: Transformed Spark DataFrame
    """

    df_with_beef = input_df.filter(sql_fun.lower(input_df.ingredients).contains("beef"))
    log.warn('Dataset filtered with ingredients containing Beef')

    # Converting cookTime and prepTime in seconds
    df2 = df_with_beef.withColumn(
        'cookTime',
        sql_fun.coalesce(sql_fun.regexp_extract('cookTime', r'(\d+)H', 1).cast('int'), sql_fun.lit(0)) * 3600 +
        sql_fun.coalesce(sql_fun.regexp_extract('cookTime', r'(\d+)M', 1).cast('int'), sql_fun.lit(0)) * 60 +
        sql_fun.coalesce(sql_fun.regexp_extract('cookTime', r'(\d+)S', 1).cast('int'), sql_fun.lit(0))
    ).withColumn(
        'prepTime',
        sql_fun.coalesce(sql_fun.regexp_extract('prepTime', r'(\d+)H', 1).cast('int'), sql_fun.lit(0)) * 3600 +
        sql_fun.coalesce(sql_fun.regexp_extract('prepTime', r'(\d+)M', 1).cast('int'), sql_fun.lit(0)) * 60 +
        sql_fun.coalesce(sql_fun.regexp_extract('prepTime', r'(\d+)S', 1).cast('int'), sql_fun.lit(0))
    )
    log.warn("Converted cookTime and prepTime in seconds")

    # calculating total_cook_time in minutes
    df3 = df2.withColumn(
        'total_cook_time',
        (expr("cookTime + prepTime")/60).cast(IntegerType())
             )
    log.warn("Calculated total cooking time")

    # calculating difficulty on the basis of total_cook_time
    df4 = df3.withColumn("difficulty", when(df3.total_cook_time < 30, "easy")
                        .when((df3.total_cook_time >= 30) & (df3.total_cook_time < 60), "medium")
                        .when(df3.total_cook_time >= 60, "hard"))
    log.warn("Calculated difficulty of cooking time")

    output_df = df4.groupBy("difficulty").agg(mean("total_cook_time").alias("avg_total_cook_time"))
    log.warn("Df aggregated on difficulty basis and avg cooking time")

    final_df = output_df.select('difficulty', sql_fun.round('avg_total_cook_time', 2).alias('avg_total_cook_time'))
    log.warn("Successfully created final df with : difficulty,avg_total_cook_time ")

    return final_df
