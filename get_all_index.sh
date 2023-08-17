#!/bin/zsh

for c in `cat names.txt`;
do
    # get filename as `c`
    if [ -f "indices/${c}_index_tokyo.dill" ]; then
        echo "File exists: indices/${c}_index_tokyo.dill"
    else
        python src/get_index.py -n $c
    fi
done