#!/usr/bin/env python

"""
Convert CSV file to libsvm format. Works only with numeric variables.
Put -1 as label index (argv[3]) if there are no labels in your file.
Expecting no headers. If present, headers can be skipped with argv[4] == 1.

"""

import sys
import csv
import operator
from collections import defaultdict

def construct_line(label, line, labels_dict):
    new_line = []
    label = int(label)
    if label in labels_dict:
        new_line.append(labels_dict.get(label))
    else:
        label_id = str(len(labels_dict))
        labels_dict[label] = label_id
        new_line.append(label_id)

    for i, item in enumerate(line):
        if item == '' or float(item) == 0.0:
            continue
        elif item=='NaN':
            item="0.0"
        new_item = "%s:%s" % (i + 1, item)
        new_line.append(new_item)
    new_line = " ".join(new_line)
    new_line += "\n"
    return new_line

# ---

input_file = "filtered_train.csv"
try:
    output_file = "trainLibSVM.csv"
except IndexError:
    output_file = input_file+".out"


try:
    label_index = int( 7 )
except IndexError:
    label_index = 0

try:
    skip_headers = True
except IndexError:
    skip_headers = 0

i = open(input_file, 'rt')
o = open(output_file, 'wb')

reader = csv.reader(i)

if skip_headers:
    headers = reader.__next__()

labels_dict = {}
for line in reader:
    if label_index == -1:
        label = '1'
    else:
        label = line.pop(label_index)

    new_line = construct_line(label, line, labels_dict)
    o.write(new_line.encode('utf-8'))