## Main Workflow: twitterMovies-vader
## Contributor: @lachlandeer

import glob, os

# --- Importing Configuration Files --- #
configfile: "config.yaml"

# --- Set up Dictionaries --- #
#CHICAGODATA = [ iLine.rstrip('/ \n') for iLine
#                in open(config['src_data'] + 'twitterFolders.txt')]
CHICAGODATA = ['DeerSet9']

# --- Thresholds for VADER analysis --- #
THRESHOLDS = "-1.00 -0.333 0.333 1.00"

# --- Spark Submit Command --- #
RUN_PYSPARK = "spark-submit --master spark://master001:7077"

# --- Rules --- #
## runChicagoDaily:     run sentiment analysis on Chicago data

rule runChicagoDaily:
    input:
        dataStats = expand(config["out_counts"] + "{iFolder}.csv", \
                            iFolder = CHICAGODATA),
        dataCounts = expand(config["out_stats"] + "{iFolder}.csv", \
                            iFolder = CHICAGODATA)

## {params.folder} \
#    {params.thresholds} {output.outCounts} {output.outStats} > {log}
# chicagoDaily: vader Sentiment analysis at the daily level on twitter data from Chicago
rule chicagoDaily:
    input:
        script      = config["src_main"] + "driver_dailySentiment.py",
        library     = config["ROOT"]  + "usrLib.zip",
    params:
        folder     = 'twitter-chicago/' + "{iFolder}" + '/',
        thresholds = THRESHOLDS,
        dataPath   = config["data_mount"]
    output:
        outCounts = config["out_counts"] + "{iFolder}.csv",
        outStats  = config["out_stats"] + "{iFolder}.csv"
    log: config["out_log"] + str("{iFolder}") + "_" + \
                         "daily.txt"
    shell:
        "{RUN_PYSPARK} \
            --py-files {input.library} \
            {input.script} --dataPath {params.dataPath} \
            --folder {params.folder} \
            --thresholds {params.thresholds} \
            --outCounts {output.outCounts} \
            --outStats {output.outStats} > {log}"

rule zipPyModules:
    input:
        library = config["lib"]
    output:
        zipDir = config["ROOT"] + "usrLib.zip"
    shell:
        "zip -x *.pyc -r {output.zipDir} {input.library}"


## clean
rule cleanOut:
    shell:
        "rm -rf out/*"
