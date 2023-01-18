#!/usr/bin/env bash

# Accepts a newline-separated text file of Thoth work IDs
# and calls Thoth Dissemination Service to acquire their
# PDF file and metadata and upload them to Internet Archive.
#
# Usage: ./bulkupload.sh [idfile]
# Outputs: 1) list of successfully uploaded IDs to file ./uploaded.txt
#          2) Dissemination Service log messages to stderr
# To send log messages to a file: ./bulkupload.sh [idfile] 2>> [logfile]
#
# NOTE accepting/outputting streams rather than files would be preferable

# Name of file in current directory where work IDs of
# successfully uploaded items will be saved
UPLOADED="$(dirname 0)/uploaded.txt"

# Catch ctrl+C keypresses and make them stop the main script -
# otherwise they will be caught by the docker command and the loop will continue
trap 'echo Script stopped by user; exit 130' INT

while read work_id
do
    # Check if ID appears in file of already-uploaded items (if present)
    if [ -f "$UPLOADED" ] && grep -Fxq "$work_id" $UPLOADED; then
        # Already done - don't re-upload
        continue
    fi
    # Call Dissemination Service (must have been built as docker image named "testdissem")
    docker run --rm testdissem ./disseminator.py --work $work_id --platform InternetArchive
    # Alternative method of calling Dissemination Service
    # (no docker required, assumes we are running in a subfolder of thoth-dissemination)
    # python3 ../disseminator.py --work $work_id --platform InternetArchive
    exitstatus=$?
    if [ $exitstatus == 0 ]
        # Upload successful - save work ID to file of already-uploaded items
        then echo "$work_id" >> $UPLOADED
    fi
# Take list of work IDs from text file given in command argument
done < "${1:-/dev/stdin}"
