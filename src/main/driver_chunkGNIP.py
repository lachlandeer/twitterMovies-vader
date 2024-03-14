"""
Constructs a set of lists that contain movies.

These lists are then used to construct VADER output in 
  another part of the workflow
"""
# --- Import Libraries --- #
## Native Python
import os
import sys
from pathlib import Path
import time
import csv
import argparse

## User written
#sys.path.append(str(Path('.').absolute()))
import computeVaderResults as cvr

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
  "--folder",
  nargs   = "*",
  type    = str,  # any type/callable can be used here
  default = [],
)
CLI.add_argument(
  "--outListFolder",
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
print("dataPath      : %r" % args.dataPath)
print("folder        : %r" % args.folder)
print("Lists saved to: %r" % args.outListFolder)
print("-------------------------------------------")

dataPath        = args.dataPath[0]
folderPath      = args.folder[0]
fullPath        = dataPath + folderPath
outListFolder   = args.outListFolder[0]

# --- Run analysis --- #
startTime = time.time()
print('Starting Job')

if not os.path.exists(outListFolder):
  # Create a new directory because it does not exist
  os.makedirs(outListFolder)
  print("The directory", outListFolder, "is created!")
else:
  print("The directory", outListFolder, "already exists")

cvr.processGNIPFilters(fullPath, outListFolder)

print('Job Completed!')
totalTime = time.time() - startTime
print('Job took:', totalTime / 60, 'minutes to complete!')
print('Done! - Goodbye')
