# Examples

This folder contains examples of how this dataset, and specifically the OpenStreetMap (OSM) geospatial features, can be used.

Please note that these are examples only are may require additional steps to replicate.
- The Python packages utilized in these examples may not be included in the requirements
or environment setup process for replicating the dataset preparation documented in this
repository. However, they can all be easily installed using Anaconda.
- Examples may require additional data from other sources such as VIIRS nighttime lights data (Please see https://github.com/aiddata/geo-datasets/tree/master/viirs_ntl for more information on how AidData downloads and prepares this and other data).
- Additional elements of the examples and scripts (e.g., data paths) may need to be adjusted
as well before using.
- Software such as [QGIS](https://www.qgis.org/) may be used to visualize or explore data. QGIS is free and open source, and there are many tutorials available online to help with installation and use


## Understanding How Project Geospatial Features are Derived from OpenStreetMap ([features_intro](features_intro))

In this example we will explore the basic methodology used to extract features from OpenStreetMap and produce the geospatial features associated with projects in AidData's Global Chinese Development Finance Dataset. We will begin by exploring the types and format of geospatial data available from OSM (e.g., nodes, ways, relations, as well as driving directions), and then review how that data is converted into a standardized collection of MultiPolygon features.


## Laos Maps ([laos_maps](laos_maps))

This example shows how [QGIS](https://qgis.org) can be used to produce visualizations of geospatial features from AidData's Global Chinese Development Finance Dataset. A subset of project data for Laos is included, along with a pre-built QGIS project that can be loaded and explored on your own computer. Using pre-built QGIS layouts, we produce visualizations of projects in Laos at the country level as well as around the capital region of Vientiane.


## Pakistan Maps ([pakistan_maps](pakistan_maps))

This example shows how [QGIS](https://qgis.org) can be used to produce visualizations of geospatial features from AidData's Global Chinese Development Finance Dataset. A subset of project data for Pakistan is included, along with a pre-built QGIS project that can be loaded and explored on your own computer. Using pre-built QGIS layouts, we produce visualizations of projects in Pakistan at the country level as well as around Islamabad, Gwadar, and Karachi.


## Nighttime Lights Analysis ([ntl_demo](ntl_demo))

This examples provides a demonstration of how the geospatial features provided with AidData's Global
Chinese Finance Dataset can be used to generate a set of visualizations and statistics associated with
changed in nighttime lights (NTL).

We will use a small set of features associated with Chinese funded projects,
consisting of a tranmissions line connecting a dam / power generation site and
a power substation. The impact of these activities on the surrounding area will
be evaluating using NTL data within a 5km buffer of the project sites.
Based on the approximate implementation date of 2016/2017, we will evaluate trends
and levels of NTL between 2014-2017 and 2017-2020.

The example will produce a set of statistical interpretations of the impact of the project
on NTL levels, as well as visualizations of the change in NTL trends over time.

## Generate Buffers Around Project Features ([generate_buffers](generate_buffers))

The Python code provided in this example generated buffers of varying sizes around the geospatial features and saves them as new GeoJSON files.


## Project Feature Rasterization ([rasterize](rasterize))

The Python code in this example can be used to produce gridded surfaces (rasters) based on the dollar value of project commitments. Only projects with geospatial features can be rasterized. The rasterized surface is produced for all projects with geospatial features, as well as subsets based on sector.
