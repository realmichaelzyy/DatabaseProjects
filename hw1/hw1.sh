#!/bin/bash
# bash command-line arguments are accessible as $0 (the bash script), $1, etc.
echo "Running" $0 "on" $1
python ebookcsv.py $1