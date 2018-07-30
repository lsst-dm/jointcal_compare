#!/bin/bash
#
# Convert a bunch of rst files to html.
# Run this with the first argument as the directory containing the .rst files
# you want to convert. The results will go into that dir/html/.
#
# Adapted from here:
# https://superuser.com/questions/742836/batch-rst2html-conversion-in-a-bash-script

directory=$1

for i in $(find "$directory" -type f -name "*.rst")
do
    RST_FILE="$i"
    HTML_BASE=$(basename $RST_FILE)
    HTML_FILE="$directory/html/${HTML_BASE%.rst}.html"
    echo $i $RST_FILE $HTML_FILE
    rst2html.py "$i" "$HTML_FILE"
done
