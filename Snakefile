## Main Workflow: twitterMovies-vader
## Contributor: @lachlandeer

import glob, os

# --- Importing Configuration Files --- #

configfile: "config.yaml"


# --- Set up Dictionaries --- #
FOLDERLIST = [iLine.rstrip('\n') for iLine
                in open(config['src_data'] + 'twitterFolders.txt')]

print(FOLDERLIST)

# --- Rules --- #
# TBD
