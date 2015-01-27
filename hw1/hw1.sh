#!/bin/bash
# bash command-line arguments are accessible as $0 (the bash script), $1, etc.
echo "Running" $0 "on" $1
echo "Generating ebook.csv and tokens.csv"
python ebookcsv.py $1
echo "Done. Gonna create token_counts.csv"
sed '1d' tokens.csv | sort -t',' -k2,2 | cut -d ',' -f 2 | uniq -c > temp_token_count.csv
echo "Reformatting the count and name_counts.csv"
python formatCounts.py
echo "Deleting temp file"
rm -f temp_token_count.csv
exit 0