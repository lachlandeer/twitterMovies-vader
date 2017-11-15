"""
Some header that has no meaningful information
"""

# --- Import Functions from other Libraries ---#

from pyspark.sql.functions import col, udf, avg, lit
from pyspark.sql.functions import mean, stddev, min, max, count
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
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

analyzer = SentimentIntensityAnalyzer()

def getCompoundScore(text):
    """
    Compute the VADER Sentiment Score of an individual Tweet.
    To be wrapped in a udf to send to SparkSQL data

    Inputs:
        - text = a Spark column of tweets
    Other Functions Called:
        -  vaderSentiment polarity analyzer: imported as analyzer
    Outputs:
        - compoundScore = sum of the valence scores of each
        words in the lexicon, and normalized result to be between
        -1 (most extreme negative) and +1 (most extreme positive)
    Example Usage:
        compound_udf = udf(getCompoundScore)
    """

    compoundScore = analyzer.polarity_scores(text).get('compound')
    return compoundScore

## convert getCompoundScore to UDF
getCompoundUDF = udf(getCompoundScore)

def returnCompoundScore(dataset, textColumn = 'body',
        outputColumn = 'vaderScore'):
    """
    Return the VADER compound score for each tweet as a
    column attached to the data

    Inputs:
        - dataset: a spark DataFrame
        - textColumn: the column where the Tweet is stored
            - default: 'body'
        - outputColumn: the column to put the output into
            - default: 'vaderScore'
    Other Functions Called:
        - getCompoundUDF()
    Outputs:
        - sentiment: a spark DataFrame with each tweet's
            VADER sentiment score attached
    Example Usage:
        sentiment_data = returnCompoundScore(my_data,
                                textColumn = 'tweet_text')
    """
    print('Computing VADER Scores for each tweet')
    sentiment = dataset.withColumn(outputColumn,
                    getCompoundUDF(col(textColumn))\
                    .cast('Double'))
    return sentiment

def vaderClassify(dataset, vScore = 'vaderScore',
                    outCol = 'vaderClassifier'
                    thresholds = [-1.0, -0.5, 0.5, 1.0]):
    """
    Returns whether a Tweet is classified as positive,
    negative or neutral based on VADER Sentiment Scores and
    threshold values for the cutoffs

    Inputs:
        - dataset: a Spark dataFrame with Sentiment Scores
        - vScore: the column where VADER sentiment scores are
        - outCol: column to write the classification out
        - thresholds: where to split the data into categories
    Other Functions Called:
        - NULL
    Outputs:
        - bucketedData: a spark DataFrame with the
            VADER classifcation added as a new column

    Example Usage:
        my_thresholds = [-1.0, -0.33, 0.33, 1.0]
        classifiedData = vaderClassify(my_data, vScore = 'vaderScore',
                            outCol = 'tweetClassification',
                            thresholds = my_thresholds)
    """
    print ('Classifying all tweets in to buckets using the cutoffs',
                thresholds[1], 'and', thresholds [2])
    # pass thresholds and input and output column to
    # define a classification function
    bucketizer = Bucketizer(splits = thresholds,
                        inputCol = vScore, outputCol = outCol)
    print("Bucketizer output with %d buckets" % (len(bucketizer.getSplits())-1))
    # classify the data
    bucketedData = bucketizer.transform(dataset)
    return bucketedData
