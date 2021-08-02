#!/bin/bash


release=$1

timestamp=$2

prev=`cat ${HOME}/tuff_osm/latest/version.txt`


mkdir -p ${HOME}/tuff_osm/previous/$prev

cp -r ${HOME}/tuff_osm/latest/* ${HOME}/tuff_osm/previous/$prev/

rm -r ${HOME}/tuff_osm/latest/*

cp -r ${HOME}/tuff_osm/output_data/$release/results/$timestamp/geojsons ${HOME}/tuff_osm/latest/

zip --junk-paths - ${HOME}/tuff_osm/output_data/$release/results/$timestamp/combined.geojson > ${HOME}/tuff_osm/latest/combined.geojson.zip 

echo ${release}_${timestamp} > ${HOME}/tuff_osm/latest/version.txt
