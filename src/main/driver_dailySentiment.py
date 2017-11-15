"""
Here is some basic text
"""

import os
import sys
from pathlib import Path

sys.path.append(str(Path('.').absolute()))
#print(sys.path)
# Import local packages
import lib.computeVaderResults as cvr

# run some stuff
my_text = sys.argv[1]
print(my_text)

#stuff_to_print(sys.argv[1])
cvr.stuff_to_print(sys.argv[1])
