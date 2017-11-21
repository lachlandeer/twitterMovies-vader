"""
Here is some basic text
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
sys.path.append(str(Path('.').absolute()))
import lib.computeVaderResults as cvr

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
  "--thresholds",
  nargs   ="*",
  type    = float,  # any type/callable can be used here
  default = [-1.0, -0.5, 0, 0.5, 1.0],
)
CLI.add_argument(
  "--outCounts",
  nargs   = "*",
  type    = str,  # any type/callable can be used here
  default = ['./'],
)
CLI.add_argument(
  "--outStats",
  nargs   = "*",
  type    = str,  # any type/callable can be used here
  default = ['./'],
)

# --- Parse the Command Line Options --- #

args = CLI.parse_args()
# access CLI options
print("dataPath  : %r" % args.dataPath)
print("folder    : %r" % args.folder)
print("thresholds: %r" % args.thresholds)
print("outCounts : %r" % args.outCounts)
print("outStats  : %r" % args.outStats)

dataPath   = args.dataPath[0]
folderPath = args.folder[0]
outCounts  = args.outCounts[0]
outStats   = args.outStats[0]

# --# Run analysis --- #

rowText = [1,2,3, "hat"]

ofile  = open(outCounts, "w")
writer = csv.writer(ofile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
writer.writerow(rowText)
ofile.close()

ofile  = open(outStats, "w")
writer = csv.writer(ofile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
writer.writerow(rowText)
ofile.close()
