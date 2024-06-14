"""
Some header that has no meaningful information
"""

# --- Import Functions from other Libraries ---#

from pyspark.sql.functions import col, udf, avg, lit, to_date
from pyspark.sql.functions import mean, stddev, min, max, count
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import regexp_extract, regexp_replace
from pyspark.sql.functions import when
from pyspark.sql.functions import from_utc_timestamp, to_date

import os
import math
import json
import pickle

# --- Functions to Import Data --- #

def loadTwitterData(filePath):
    """
    Loads all Twitter data .json files into Spark from a directory

    Inputs:
        - filePath: a valid filePath
    Other Functions Called: NULL
    Outputs:
        - df: a spark dataFrame
    Example Usage:
        data_path = 'alluxio://master001:19998/twitter-chicago/DeerSet9/'
        my_data   = loadTwitterData(data_path)
    """
    # --- Spark Set up --- #
    import atexit
    import os
    import platform

    import py4j

    import pyspark
    from pyspark.context import SparkContext
    from pyspark.sql import SparkSession, SQLContext
    from pyspark.storagelevel import StorageLevel

    if os.environ.get("SPARK_EXECUTOR_URI"):
        SparkContext.setSystemProperty("spark.executor.uri", os.environ["SPARK_EXECUTOR_URI"])

    SparkContext._ensure_initialized()

    try:
        # Try to access HiveConf, it will raise exception if Hive is not added
        SparkContext._jvm.org.apache.hadoop.hive.conf.HiveConf()
        spark = SparkSession.builder\
            .getOrCreate()
    except py4j.protocol.Py4JError:
        spark = SparkSession.builder.getOrCreate()
    except TypeError:
        spark = SparkSession.builder.getOrCreate()

    sc = spark.sparkContext
    sql = spark.sql
    atexit.register(lambda: sc.stop())

    print(spark)

    # Here's all the dirs to load from
    data_dirs = [ iLine.rstrip('/ \n') for iLine
               in open('src/in_data/twitterFolders.txt')]
    print(data_dirs)

    type(data_dirs)

    STOP

    df = spark.read.option("basePath", filePath)\
         .parquet(filePath)\
         .withColumn("file_name", 
                     input_file_name()
                    )

    return df

def fixMovieName(df):
    """
    Some movie names are set incorrectly in the Chicago sourced
    data.
    This step corrects this by using the input file name as the movie name
    for these movies
    """

    # finds an alternate movieName if the input file
    # starts with Deer. 
    # these are the Chicago data
    df = df\
          .withColumn('movieName_alt', 
                     regexp_extract(col('file_name'), 
                                    '.Deer(\w+)', 1)
                    )\            
            .withColumn('movieName_alt',
                        regex_replace(col('movieName_alt'),
                            "\\[", "")
                        )
            .withColumn('movieName_alt',
                        regex_replace(col('movieName_alt'),
                            "\\]", "")
                        )
    
    df = df.withColumn("movieName", \
              when(df["movieName"] == '[movie]', 
                   df["movieName_alt"])\
                   .otherwise(df["movieName"])
                   )
    
    print(df.columns)

    return df

def returnTweetID(df):
    """
    The unique IDs of the tweets are nested deep in a column
    Here we retrieve them
    """
    df = df\
        .withColumn('tweet_id', 
                    regexp_extract(col('id'), 
                                  '(\d+):(\d+)', 2)
                    )
    
    print(df.columns)

    return df

def timeShiftEastCoast(df):
    """
    Switch time zone of all tweets to US/East Coast time

    This is the date we want to aggregate on
    """
    # give a datetime
    df = df\
        .withColumn('postedTime_EST', 
                    from_utc_timestamp(df['postedTime'], "EST")
                    )
    # and a date
    df = df.withColumn('postedDate', 
            to_date(df['postedTime_EST'])
            )
    
    print(df.columns)

    return df

def selectExportCols(df):
    """
    Columns we need to export for further analysis
    """

    columnNames = ["tweet_id",
        "postedDate",
        "postedTime_EST",
        "movieName",
        "vaderScore",
        "vaderClassifier"]

    df2 = df.select(*columnNames)

    return df2

def data2parquet(dataset, outPath):
    # partition by movie, date
    #dataset = dataset.withColumn('date', to_date(col('postedTime')))
    print('saving to ', outPath)
    dataset.write.partitionBy('movieName', 'postedDate').parquet(outPath)

# 

def tidyTweets(df):
    """
    call the tidying functions
    """
    df = fixMovieName(df)
    df = returnTweetID(df)
    df = timeShiftEastCoast(df)
    #df = selectExportCols(df)

    return df

def runTidyTweets(dataPath, outPath):
    """
    Run the analysis
    """
    # Load Data
    print('Loading the data from ', dataPath)
    df = loadTwitterData(dataPath)

    # Run analysis
    df = tidyTweets(df)

    # save
    data2parquet(df, outPath)
