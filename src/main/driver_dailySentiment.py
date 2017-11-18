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

# User written
sys.path.append(str(Path('.').absolute()))
#import lib.computeVaderResults as cvr

# --- Import CL arguments:
dataPath    = sys.argv[1]
analysisSet = sys.argv[2]
thresholds  = sys.argv[3]
outCounts   = sys.argv[4]
outStats    = sys.argv[5]

print(dataPath)
print(analysisSet)
print(thresholds)
print(outCounts)
print(outStats)

file1 = csv.writer(open(outCounts, "wb"))
file2 = csv.writer(open(outStats, "wb"))
