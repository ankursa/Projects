#!/usr/bin/env python

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into columns 
    columns = line.split('\x01')
    nCols = len(columns)
    csv_cols = columns[0]
    for i in range(1,nCols):
    	csv_cols = csv_cols + ',' + columns[i]
    # increase counters
    # printing item_id and corresponding data row to stdout
    print '%s\t%s' % (columns[0], csv_cols)
