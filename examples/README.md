# Examples

This folder contains examples of how this dataset, and specifically the OpenStreetMap geospatial features, can be used.

Please note that these are examples only are may require additional steps to replicate.
- The Python packages utilized in these examples may not be included in the requirements
or environment setup process for replicating the dataset preparation documented in this
repository. However, they can all be easily installed using Anaconda.
- Examples may require additional data from other sources such as VIIRS nighttime lights data (Please see https://github.com/aiddata/geo-datasets/tree/master/viirs_ntl for more information on how AidData downloads and prepares this and other data).
- Additional elements of the examples and scripts (e.g., data paths) may need to be adjusted
as well before using.
- Software such as [QGIS](https://www.qgis.org/) may be used to visualize or explore data. QGIS is free and open source, and there are many tutorials available online to help with installation and use

## Nighttime Lights Analysis (ntl_demo)

This examples provides a demonstration of how the OpenStreetMap features provided with AidData's
Chinese Finance dataset can be used to generate a set of visualizations and statistics associated with
changed in nighttime lights (NTL).

We will use a small set of features associated with Chinese funded projects,
consisting of a tranmissions line connecting a dam / power generation site and
a power substation. The impact of these activities on the surrounding area will
be evaluating using NTL data within a 5km buffer of the project sites.
Based on the approximate implementation date of 2016/2017, we will evaluate trends
and levels of NTL between 2014-2017 and 2017-2020.

The example will produce a set of statistic interpretations of the impact of the project
on NTL levels, as well as visualizations of the change in NTL trends over time.



