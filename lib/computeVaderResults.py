"""
Some header that has no meaningful information
"""

# --- Import Functions from other Libraries ---#

from pyspark.sql.functions import col, udf, avg, lit
from pyspark.sql.functions import mean, stddev, min, max, count
# sentiment analysis
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
# classifying vader scores into bins
from pyspark.ml.feature import Bucketizer

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
    df = spark.read.json(filePath + '*.gz')
    return df

def selectRelevantColumns(df):
    """
    Select relevant columns of twitter data and clean them up by
        1. Filtering English Language tweets
        2. Dropping na valued tweets
        3. Type casting the columns correctly
        4. Renaming columns

    Inputs:
        - df: a Spark dataFrame
    Other Functions Called: NULL
    Outputs:
        - df2: a spark dataFrame
    Example Usage:
        data_path   = 'alluxio://master001:19998/twitter-chicago/DeerSet9/'
        my_data     = loadTwitterData(filePath)
        small_data  = selectRelevantColumns(my_data)
    """
    columnNames = ['body','gnip.matching_rules.tag', \
                    'gnip.matching_rules.value', \
                    'postedTime', 'retweetCount']

    # Select relevant columns & filter out english language tweets
    df2 = df.select(*columnNames)
    df2 = df2.filter(df2.twitter_lang == "en").na.drop()

    # Type cast
    df2 = df2.withColumn('date', df2['postedTime'].cast('date'))
    df2 = df2.withColumn('tag', df2['tag'].cast('string'))
    df2 = df2.withColumn('value', df2['value'].cast('string'))

    # rename
    df2 = df2.withColumnRenamed("tag", "movieName")
    df2 = df2.withColumnRenamed("value", "searchPattern")
    return df2

def importTwitterData(filePath):
    """
    Takes a filePath and returns a cleaned up version of
        the twitter data to perform sentiment analysis.

    Inputs:
        - filePath: a valid filePath
    Other Functions Called:
        - loadTwitterData()
        - selectRelevantColumns(df)
    Outputs:
        - smallData: a cleaned up spark DataFrame
    Example Usage:
        data_path  = 'alluxio://master001:19998/twitter-chicago/DeerSet9/'
        my_data    = importTwitterData(dataPath)
    """
    dataSet = loadTwitterData(filePath)
    smallData = selectRelevantColumns(dataSet)
    return smallData

# --- Text Classification with VADER --- #
