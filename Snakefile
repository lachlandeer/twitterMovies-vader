## Main Workflow: twitterMovies-vader
## Contributor: @lachlandeer

import glob, os

# --- Importing Configuration Files --- #
configfile: "config.yaml"

# --- Set up Dictionaries --- #
CHICAGODATA = [ iLine.rstrip('/ \n') for iLine
               in open(config['src_data'] + 'twitterFolders.txt')]
#CHICAGODATA = ['DeerSpectre']

# --- Thresholds for VADER analysis --- #
THRESHOLDS = "-1.00 -0.05 0.05 1.00"

# --- Spark Submit Command --- #
RUN_PYSPARK = "spark-submit --master spark://lachlan-tower:7077"


# --- Tidy Vader Output --- #
rule tidyVaderTweets:
    input: 
        script = "src/main/driver_tidy_tweets.py",
        library = "tidyTweets.zip",
        # not specifying data as inpouts to separate the 
        # DAG while we play around
        #data_chicago =
        #data_gnip = 
    params:
        dataPath = "out/data/vader/chicago/DeerAntMan/"
    output:
        data = directory("out/data/tidyVader/chicago/"),
    log: 
        config["out_log"] + "tidyVader/tidyVader_chicago.txt"
    shell:
        "{RUN_PYSPARK} \
            --py-files {input.library} \
            {input.script} --dataPath {params.dataPath} \
            --outPath {output.data} \
            > {log}"    


# --- Vader Classifiers ---- #
rule runChicagoVader:
    input:
        data = expand(config["out_chicago_vader"] + "{iFolder}", \
                            iFolder = CHICAGODATA),

rule chicagoVader:
    input:
        script      = config["src_main"] + "driver_compute_vader.py",
        library     = "tweetVader.zip",
    params:
        folder     = 'twitter-chicago/' + "{iFolder}/",
        thresholds = THRESHOLDS,
        dataPath   = config["data_mount"]
    output:
        data = directory(config["out_chicago_vader"] + "{iFolder}"),
    log: 
        config["out_log"] + "chicago/" + "{iFolder}" + ".txt"
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

rule zipTidyModule:
    input:
        library = "lib2/tidyTweets.py"
    output:
        zipDir = "tidyTweets.zip"
    shell:
        "zip -jr  {output.zipDir} {input.library}"

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
