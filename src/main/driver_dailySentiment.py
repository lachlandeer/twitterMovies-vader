"""
Here is some basic text
"""
# --- Import Libraries --- #

## Native Python
import os
import sys
from pathlib import Path
import time

# User written
sys.path.append(str(Path('.').absolute()))
import lib.computeVaderResults as cvr

# --- Import CL arguments:

# run some stuff
my_text = sys.argv[1]
print(my_text)

#stuff_to_print(sys.argv[1])
cvr.stuff_to_print(sys.argv[1])
