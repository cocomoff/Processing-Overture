#!/bin/zsh

for c in `cat names.txt`;
do
    # get filename as `c`
    if [ -f "tables/${c}_tokyo.dill" ]; then
        echo "File exists: tables/${c}_tokyo.csv"
    else
        if [ -f "indices/${c}_index_tokyo.dill" ]; then
            python src/read_index.py -n $c
        fi
    fi
done