#!/bin/bash

if [[ ! -f config.ini ]]; then
    echo "Please make sure you are in the repo root directory before running."
    echo -e "\t Currently in:" $PWD
    exit 1
fi

release=$1

timestamp=$2

prev=`cat ./latest/version.txt`


mkdir -p ./previous/$prev

cp -r ./latest/* ./previous/$prev/

rm -r ./latest/*

cp -r ./output_data/$release/results/$timestamp/geojsons ./latest/
cp ./output_data/$release/results/$timestamp/all_combined_global.gpkg.zip ./latest/

echo ${release}-${timestamp} > ./latest/version.txt
