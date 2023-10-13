#!/bin/bash

if [[ ! -f tuff_osm.py ]]; then
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

zip --junk-paths - ./output_data/$release/results/$timestamp/all_combined_global.geojson > ./latest/all_combined_global.geojson.zip
zip --junk-paths - ./output_data/$release/results/$timestamp/development_combined_global.geojson > ./latest/development_combined_global.geojson.zip
zip --junk-paths - ./output_data/$release/results/$timestamp/huawei_combined_global.geojson > ./latest/huawei_combined_global.geojson.zip
zip --junk-paths - ./output_data/$release/results/$timestamp/military_combined_global.geojson > ./latest/military_combined_global.geojson.zip

echo ${release}_${timestamp} > ./latest/version.txt
