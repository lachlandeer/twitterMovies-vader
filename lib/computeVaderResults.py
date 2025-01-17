"""
Some header that has no meaningful information
"""

# --- Import Functions from other Libraries ---#

from pyspark.sql.functions import col, udf, avg, lit
from pyspark.sql.functions import mean, stddev, min, max, count
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.ml.feature import Bucketizer
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

    #spark = SparkSession(sc)
    print(spark)
    # for compatibility
    sqlContext = spark._wrapped
    sqlCtx = sqlContext

    df = spark.read.format("com.databricks.spark.json")\
        .option("badRecordsPath", "/tmp/badRecordsPath")\
        .option("mode", "DROPMALFORMED").json(filePath + '*.gz')
    return df

def selectRelevantColumns(df, filePath):
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
                    'postedTime', 'retweetCount', \
                    'twitter_lang']

    # Select relevant columns & filter out english language tweets
    df2 = df.select(*columnNames)
    df2 = df2.filter(df2.twitter_lang == "en").na.drop()

    # Type cast
    df2 = df2.withColumn('date', df2['postedTime'].cast('date'))
    df2 = df2.withColumn('tag', df2['tag'].cast('string'))
    df2 = df2.withColumn('value', df2['value'].cast('string'))

    # rename
    if "gnip" in filePath:
        print('working on GNIP Data...')
        df2 = df2.withColumnRenamed("value", "movieName")
    elif "chicago" in filePath:
        print('working on Chicago Data...')
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
    smallData = selectRelevantColumns(dataSet, filePath)
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
                    outCol = 'vaderClassifier',
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

## --- Find What Movies are contained in twitter data --- ##
## Works for data inside 'twitter-chicago' directories

def uniqueMovies(dataset, movieKey):
    """
    Finds the unique movies mentioned in the tweets within dataset

    In the searches run at UChicago we attached a key to
    identify which movie each search we ran is about. This
    function finds the unique values of that variable in
    a dataset.

    Inputs:
        - dataset: a spark DataFrame of movie tweets
            loaded from a UC Booth folder
        - movieKey: the column that identifies what search
            a tweet was returned
    Other Functions Called:
        - NULL
    Outputs:
        - moviesUnique: list of unique movies in the  data
    Example Usage:
        moviesUnique = uniqueMovies(my_data, 'movie_name')
    """
    movies = dataset.select(movieKey) \
                .where(~col(movieKey) \
                    .like('%,%')) \
                .distinct().collect()
    # strip the markup to return the name only
    moviesUnique = [str(iMovie.movieName[1:-1])
                        for iMovie in movies]
    return moviesUnique

# --- Functions to Analyse data from an Individual Movie --- #

def singleMovieTweets(dataset, identifier):
    """
    Filter out the tweets for an individual movie identified
    in the twitter data

    Inputs:
        - dataset: a spark dataFrame
        - identifier: a string to match to the movie name
    Other Functions Called:
        - NULL
    Outputs:
        singleMovie: a spark dataFrame of all tweets about
            a single movie
    Example Usage:
        wolverine_tweets = singleMovieTweets(my_data,
                            'wolverine')
    """
    singleMovie = dataset.filter(dataset.movieName \
                                    .like('%{0}%' \
                                    .format(identifier)
                                    )
                                )
    return singleMovie

def vaderCountsByClassification(dataset, identifier):
    """
    Calcuates the number of positive, negative and neutral
    tweets for an individual movie per day

    Inputs:
        - dataset: a spark DataFrame
        - identifier: the name of a movie
    Other Functions Called:
        - NULL
    Outputs:
        - vaderCounts: spark DataFrame of number of tweets
          per day per classification bucket

    Example Usage:
        vaderCountsByClassification(wolverine_tweets,
                                'wolverine')
    """
    vaderCounts = dataset.groupby(dataset.date,
                        dataset.vaderClassifier).count()
    vaderCounts = vaderCounts.withColumnRenamed("count",
                        "nTweets")
    # write the movie name into the rows
    vaderCounts = vaderCounts.withColumn('movieName',
                        lit(identifier))
    vaderCounts = vaderCounts.orderBy(['date',
                                        'vaderClassifier']
                        , ascending=False)
    return vaderCounts

def vaderStats(dataset, identifier, vaderCol = 'vaderScore'):
    """
    Compute Daily Sentiment Statistics for a movie

    Inputs:
        - dataset: a spark DataFrame with sentiment score attached
        - identifier: the movie we want to compute stats for
        - vaderCol: the column containing vader sentiment scores
    Other functions Called:
        - NULL
    Outputs:
        - dailyStats: summary stats per movie-day
    Example Usage:
        vaderStats(wolverine_tweets,
                    'wolverine')
    """
    # aggregate functions
    aggStats    = [mean, stddev, min, max, count]
    aggVariable = [vaderCol]
    exprs       = [iStat(col(iVariable)) for iStat in aggStats \
                    for iVariable in aggVariable]
    # summary stats
    dailyStats = dataset.groupby('date').agg(*exprs)
    # rename cols
    autoNames  = dailyStats.schema.names
    newNames   = ["date", "avgScore", "stdDev", "minScore",
                    "maxScore", "totalTweets"]
    # rename all columns to be meaningful
    dailyStats = reduce(lambda dailyStats, idx: \
                    dailyStats.withColumnRenamed(autoNames[idx],
                        newNames[idx]),
                        xrange(len(autoNames)),
                        dailyStats
                        )
    # write movie name to data
    dailyStats = dailyStats.withColumn('movieName',
                    lit(identifier))
    dailyStats = dailyStats.orderBy(['date'],
                    ascending=False)
    return dailyStats

def computeMovieStats(dataset, movieName):
    """
    Calcuates the number of positive, negative and neutral
    tweets for an individual movie per day and summary stats
    from a list of movies.

    Inputs:
        - dataset: a spark DataFrame with tweets
            classified into buckets
        -moveiName: a to compute stats for
    Other Functions Called:
        - singleMovieTweets()
        - vaderStats()
        - vaderCountsByClassification()
    Outputs:
        - indivCounts: a spark DataFrame with each
            movie-days number of tweets per classification
    Example Usage:
        computeMovieStats2(classified_data, movie_name)
    """
    # get tweets for one movie
    indivTweets = singleMovieTweets(dataset, movieName)
    # tweets Stats per day
    indivStats = vaderStats(indivTweets, movieName)
    # tweet counts by type
    indivCounts = vaderCountsByClassification(indivTweets, movieName)

    return indivCounts, indivStats


# --- CSV Writers --- #

def data2csv(dataset, outPath):
    dataset.to_csv(outPath, index=False, encoding='utf-8')
    print('saved to ', outPath)

# --- GNIP specific functions --- #

## Chunking Up List of Unique Movies

def chunks(longList, chunkSize):
    # For item i in a range that is a length of l,
    for i in range(0, len(longList), chunkSize):
        # Create an index range for l of n items:
        yield longList[i:i+chunkSize]

def movieListSave(movieList, outPath, outFile):

    with open(outPath + outFile, 'w') as fileName:
        pickle.dump(movieList, fileName)

def identifyMovies(filePath):

    print('Loading the data from ', filePath)
    df = importTwitterData(filePath)

    # identify unique movies
    moviesUnique = uniqueMovies(df, 'movieName')
    print('I found ', len(moviesUnique), ' movies in ', filePath)
    print('The movies are:')
    print('\n'.join(str(iMovie) for iMovie in moviesUnique))

    return moviesUnique

def getMovieChunks(moviesUnique, outPath):

    nMovies   = len(moviesUnique)
    chunkSize = 10
    chunkedList = list(chunks(moviesUnique, chunkSize))

    for idx, iChunk in enumerate(chunkedList):
        outFile = 'gnipChunk_' + str(idx) + '.pickle'
        movieListSave(iChunk, outPath, outFile)


def processGNIPFilters(dataPath, outPath):

    allFilters = identifyMovies(dataPath)
    getMovieChunks(allFilters, outPath)


# --- Run VADER analysis ---#

def parseMovieData(dataPath, outStats, outCounts,
                        movieList, textCol = 'body',
                        thresholds = [-1.0, -0.5, 0.5, 1.0]):

    # Load Data
    print('Loading the data from ', dataPath)
    df = importTwitterData(dataPath)

    # Compute Sentiment and Classify
    df = returnCompoundScore(df, textCol)
    df = vaderClassify(df, vScore = 'vaderScore',
                        outCol = 'vaderClassifier', thresholds=thresholds)

    #print('The value of movie List is: ', movieList)

    if movieList == 'None':
        print('No movieList passed across, finding unique movies in data')
        movies = uniqueMovies(df, 'movieName')
        print(len(movies), ' movies in ', dataPath)
        print('The movies are:')
        print('\n'.join(str(iMovie) for iMovie in movies))
    else:
        print('movieList passed across, loading from file')
        # load list of movies from pickle
        f = open(movieList, 'r')
        movies = pickle.load(f)
        f.close()

    for iMovie in movies:
        # recover counts and summary stats
        vaderCounts, vaderStats = computeMovieStats(df, iMovie)

        # add to data set or create them if they dont exist
        # first, vader counts
        if 'allVaderCounts' not in locals() or 'allVaderCounts' in globals():
            allVaderCounts = vaderCounts
        if 'allVaderCounts' in locals() or 'allVaderCounts' in globals():
            allVaderCounts = allVaderCounts.union(vaderCounts)
        # second, the summary stats
        if 'allVaderStats' not in locals() or 'allVaderStats' in globals():
            allVaderStats = vaderStats
        if 'allVaderStats' in locals() or 'allVaderStats' in globals():
            allVaderStats = allVaderStats.union(vaderStats)

    # saving via pandas merge
    # (slow, but writes to local directory which other methods dont)
    print('Converting daily stats to Pandas DF, this may take a while...')
    pandasVaderStats = allVaderStats.toPandas()
    data2csv(pandasVaderStats, outStats)
    del pandasVaderStats, vaderStats

    print ('Converting Count Data to Pandas DF, this may take a while...')
    pandasVaderCounts = allVaderCounts.toPandas()
    data2csv(pandasVaderCounts, outCounts)
    del pandasVaderCounts, vaderCounts
