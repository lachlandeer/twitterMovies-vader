# --- Import Libraries --- #
## Native Python
import os
import sys
from pathlib import Path
import time
import csv
import argparse

## User written
import tidyTweetsDaily as ttd

# ---  Define command line options --- #
# this also generates --help and error handling
CLI=argparse.ArgumentParser()
CLI.add_argument(
  "--dataPath",  # name on the CLI - drop the `--` for positional/required parameters
  nargs   = "*",  # 0 or more values expected => creates a list
  type    = str,
  default ='alluxio://master001:19998/',  # default if nothing is provided
)

CLI.add_argument(
  "--outPath",
  nargs   = "*",
  type    = str,  # any type/callable can be used here
  default = ['./'],
)

# --- Parse the Command Line Options --- #
args = CLI.parse_args()
print('Running PySpark in batch mode...')
print("-------------------------------------------")
print("Here are the specs for this job:")
# access CLI options
print("dataPath  : %r" % args.dataPath)
print("outVader : %r" % args.outPath)
print("-------------------------------------------")

dataPath        = args.dataPath[0]
outPath         = args.outPath[0]

# --- Run analysis --- #
startTime = time.time()
print('Starting Job')
print("Full path is")
print(fullPath)

ttd.runTidyTweets(dataPath, outPath)

print('Job Completed!')
totalTime = time.time() - startTime
print('Job took:', totalTime / 60, 'minutes to complete!')
print('Done! - Goodbye')