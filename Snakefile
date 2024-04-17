## Main Workflow: twitterMovies-vader
## Contributor: @lachlandeer

import glob, os

# --- Importing Configuration Files --- #
configfile: "config.yaml"

# --- Set up Dictionaries --- #
# CHICAGODATA = [ iLine.rstrip('/ \n') for iLine
#                in open(config['src_data'] + 'twitterFolders.txt')]
CHICAGODATA = ['DeerSpectre']

# --- Thresholds for VADER analysis --- #
THRESHOLDS = "-1.00 -0.05 0.05 1.00"

# --- Spark Submit Command --- #
RUN_PYSPARK = "spark-submit --master spark://lachlan-tower:7077"

# --- Build Rules --- #
## runDailyAnalysis:   compute all daily statistucs
# rule runDailyAnalysis:
#     input:
        # gnipStats  = dynamic(config["out_gnip_counts"] +
        #                         "daily-0.05/" + "{iChunk}.csv"),
        # gnipCounts = dynamic(config["out_gnip_stats"]  +
        #                         "daily-0.05/" + "{iChunk}.csv")
        # chicagoStats = expand(config["out_chicago_counts"] +
        #                         "daily-0.05/" + "{iFolder}.csv", \
        #                         iFolder = CHICAGODATA),
        # chicagoCounts = expand(config["out_chicago_stats"] +
        #                         "daily-0.05/" + "{iFolder}.csv", \
        #                         iFolder = CHICAGODATA)

## runChicagoDaily:     run sentiment analysis on Chicago data
# rule runChicagoDaily:
#     input:
#         dataCounts = expand(config["out_chicago_counts"] + "daily-0.05/" + "{iFolder}.csv", \
#                             iFolder = CHICAGODATA),
        # dataCounts = expand(config["out_chicago_stats"] + "daily-0.05/" + "{iFolder}.csv", \
        #                     iFolder = CHICAGODATA)

# chicagoDaily: vader Sentiment analysis at the daily level on twitter data from Chicago
rule chicagoDaily:
    input:
        script      = config["src_main"] + "driver_dailySentiment.py",
        library     = "tweetVader.zip",
    params:
        folder     = 'twitter-chicago/' + "{iFolder}" + '/',
        thresholds = THRESHOLDS,
        dataPath   = config["data_mount"]
    output:
        outCounts = config["out_chicago_counts"] + "daily-0.05/" + "{iFolder}.csv",
       #outStats  = config["out_chicago_stats"]  + "daily-0.05/" + "{iFolder}.csv"
    log: config["out_log"] + "daily-0.05/" + str("{iFolder}") + "_" + \
                         "daily.txt"
    shell:
        "{RUN_PYSPARK} \
            --py-files {input.library} \
            {input.script} --dataPath {params.dataPath} \
            --folder {params.folder} \
            --thresholds {params.thresholds} \
            --outCounts {output.outCounts} > {log}"

## 
# runGnipMovieLists: process the gnip data and save lists of movies
# rule runGnipMovieLists:
#    input:
#        pickles = directory(config["out_list"] + "{iChunk}.pickle"),

# gnipMovieLists: recipe to create movie lists from GNIP data
checkpoint gnipMovieLists:
   input:
       script      = config["src_main"] + "driver_chunkGNIP.py",
       library     = "tweetVader.zip",
   params:
       folder        = 'twitter-gnip/downloads/',
       dataPath      = config["data_mount"],
       outListFolder = config["out_list"]
   output:
       outLists = directory(config["out_list"])
       #outLists  = directory(config["out_list"] + "{iChunk}.pickle"),
   log: config["out_log"] + "gnip/gnip_lists.txt"
   shell:
       "{RUN_PYSPARK} \
           --py-files {input.library} \
           {input.script} --dataPath {params.dataPath} \
           --folder {params.folder} \
           --outListFolder {output.outLists} \
           > {log}"

# rule runGnipDaily:
#     input:
#         counts = expand(config["out_gnip_counts"] + "daily-0.05/" + "{iChunk}.csv",
#                             iChunk=glob_wildcards(os.path.join(checkpoints.gnipMovieLists.get(**wildcards).output[0], "{i}.pickle")).i
#                             )
        # i=glob_wildcards(os.path.join(checkpoint_output, "{i}.txt")).i)
#     input:
        # dataStats  = dynamic(config["out_gnip_counts"] +
        #                         "daily-0.05/" + "{iChunk}.csv"),
        # dataCounts = dynamic(config["out_gnip_stats"]  +
        #                         "daily-0.05/" + "{iChunk}.csv")

# gnipDaily: vader Sentiment analysis at the daily level on twitter data from GNIP
rule gnipDaily:
    input:
        script      = config["src_main"] + "driver_dailySentiment.py",
        movieList   = config["out_list"] + "{iChunk}.pickle",
        library     = "tweetVader.zip",
    params:
        folder     = 'twitter-gnip/downloads/',
        thresholds = THRESHOLDS,
        dataPath   = config["data_mount"]
    output:
        outCounts = config["out_gnip_counts"] + "daily-0.05/" + "{iChunk}.csv",
        #outStats  = config["out_gnip_stats"]  + "daily-0.05/" + "{iChunk}.csv"
    log: config["out_log"] + "daily-0.05/" + "{iChunk}_vaderDaily.txt"
    shell:
        "{RUN_PYSPARK} \
            --py-files {input.library} \
            {input.script} --dataPath {params.dataPath} \
            --folder {params.folder} \
            --thresholds {params.thresholds} \
            --outCounts {output.outCounts} \
            --movieList {input.movieList} > {log}"

def aggregate_gnip_sentiment(wildcards):
    # get output names
    checkpoint_output = checkpoints.gnipMovieLists.get(**wildcards).output[0]
    # create the list of outputs we expect after running sentiment algs
    file_names = expand(config["out_gnip_counts"] + "daily-0.05/" + "{iChunk}.csv",
                        iChunk = glob_wildcards(os.path.join(checkpoint_output, "{iChunk}.pickle")).iChunk
                        )
    return file_names

rule finishGnip:
    input: aggregate_gnip_sentiment
    output: "finished_gnip_sentiments.txt"
    shell:
        "touch {output}"

rule chicagoVader:
    input:
        script      = config["src_main"] + "driver_compute_vader.py",
        library     = "tweetVader.zip",
    params:
        folder     = 'twitter-chicago/DeerMadMax/',
        thresholds = THRESHOLDS,
        dataPath   = config["data_mount"]
    output:
        data = directory(config["out_chicago_vader"] + "DeerMadMax"),
    log: 
        config["out_log"] + "chicago/DeerMadMax.txt"
    shell:
        "{RUN_PYSPARK} \
            --conf spark.sql.files.ignoreCorruptFiles=true \
            --py-files {input.library} \
            {input.script} --dataPath {params.dataPath} \
            --folder {params.folder} \
            --thresholds {params.thresholds} \
            --outVader {output.data} \
            > {log}"

rule gnipVader:
    input:
        script      = config["src_main"] + "driver_compute_vader.py",
        library     = "tweetVader.zip",
    params:
        folder     = 'twitter-gnip/downloads/',
        thresholds = THRESHOLDS,
        dataPath   = config["data_mount"]
    output:
        data = directory(config["out_gnip_vader"]),
    log: 
        config["out_log"] + "gnip/gnip_vader.txt"
    shell:
        "{RUN_PYSPARK} --conf spark.driver.maxResultSize=8g \
            --py-files {input.library} \
            {input.script} --dataPath {params.dataPath} \
            --folder {params.folder} \
            --thresholds {params.thresholds} \
            --outVader {output.data} \
            > {log}"

rule zipPyModules:
    input:
        library = config["lib"] + "computeVaderResults.py"
    output:
        zipDir = "tweetVader.zip"
    shell:
        "zip -jr  {output.zipDir} {input.library}"

# --- Restart Rules ---#
rule restart_spark:
    shell:
        "sudo systemctl restart spark-master.service"

rule restart_alluxio:
    shell:
        "sudo /usr/lib/alluxio/bin/alluxio-start.sh all"

# --- Clean Rules --- #
## cleanOut:   clean output directory
rule cleanOut:
    shell:
        "rm -rf out/*"

# cleanZip:   clean out any zipped python modules from ROOT directory
rule cleanZip:
    shell:
        "rm *.zip"

# --- Help Rules --- #
## help:                 provide simple info about each rule
rule help:
    input:
        mainWorkflow = "Snakefile"
    shell:
        "sed -n 's/^##//p' {input.mainWorkflow}"
