# AidData's Geospatial Global Chinese Development Dataset -- v3.0

![image](https://github.com/aiddata/gcdf-geospatial-data/assets/9094862/4adcafc1-c3ad-4e43-87a9-1a569f5f965f)

Welcome to the GitHub repository for [AidData's](https://www.aiddata.org/) Geospatial Global Chinese Development Finance Dataset (GeoGCDF)! The GeoGCDF provides [geospatial features](examples/features_intro) defining the location of Chinese financed projects, and expands the type of analysis which can be conducted using the tabular, project level data from AidData's Global Chinese Development Finance Dataset (GCDF).

Please also visit AidData's [dedicated page for work related to China](https://www.aiddata.org/china), where you can find blog posts, announcements, and more.


This repository allows you to:
1. Download the complete GeoGCDF dataset, including ready-to-use geospatial features.
2. Explore examples, guidelines, other material related to using the GeoGCDF and geospatial features.
3. Access the code used to extract and process geospatial features from OSM in order to replicate the GeoGCDF or create your own dataset.


<br/>

### [Download the geospatial data: AidData’s Geospatial Global Chinese Development Finance Dataset](https://github.com/aiddata/gcdf-geospatial-data/releases/latest)

### [Read the accompanying academic publication in Nature's Scientific Data](https://www.nature.com/articles/s41597-024-03341-w)

### [Download the project level data: AidData’s Global Chinese Development Finance Dataset v3.0](https://www.aiddata.org/data/aiddatas-global-chinese-development-finance-dataset-version-3-0)




<br/>

## Dataset Description

The 3.0 version of AidData’s _Global Chinese Development Finance Dataset_ (__GCDF v3__) records the known universe of projects supported by official financial and in-kind commitments (or pledges) from China between 2000 and 2021. The Geospatial Global Chinese Development Finance Dataset, Version 3.0 (__GeoGCDF v3__) detailed in this repository captures the geospatial features of 9,405 projects across 148 low- and middle-income countries supported by Chinese grant and loan commitments worth more than USD 830 billion. The dataset provides details of 6,266 projects containing spatial definitions of roads, railways, power plants, transmission lines, buildings, and other precisely geocoded features. It identifies approximate and administrative-level locations for 3,139 additional projects.

Geographic features associated with the location of project activities were identifed through [OpenStreetMap (OSM)](https://www.openstreetmap.org/). Projects for which OSM features are available are labeled in the main dataset and can be linked with geospatial data from this GitHub repository, or used directly from the data provided here. Geospatial data is available in bulk as a [GeoPackage](https://www.geopackage.org/) available with each [Release](https://github.com/aiddata/gcdf-geospatial-data/releases/latest) or as individual [GeoJSONs](https://geojson.org/) saved in the [latest/geojsons](latest/geojsons) folder (with one GeoJSON per project named according to the project ID). For example, Project 35756 can be viewed via [latest/geojsons/35756.geojson](latest/geojsons/35756.geojson).


Examples, guides, best practices, and other material related to utilizing the geospatial data in the GeoGCDF can be found in the [Examples](examples) folder. For information on using the project data, please explore the tabs of the GCDF dataset (see download link for the latest GCDF dataset above).



<br/>

## License

The original dataset produced by AidData is licensed using the [Open Data Commons Attribution License (ODC-By)](https://opendatacommons.org/licenses/by/1-0/). Geospatial data extracted from OpenStreeMaps (OSM) is licensed using the [Open Data Commons Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/1-0/).

Any use of the dataset and its content must include appropriate attribution to AidData, while uses incorporating the geospatial features from OSM must also attribute OSM and be made available using ODbL.

Please see the [LICENSE file](LICENSE.md) for human-readable summaries of both ODC-By and ODbL, and links to the full licenses.

The following citation should be used to credit AidData for the GeoGCDF v3 (i.e., the geospatial features and associated information)
```
Goodman, S., Zhang, S., Malik, A.A., Parks, B.C., Hall, J. AidData’s Geospatial Global Chinese Development Finance Dataset. Sci Data 11, 529 (2024). https://doi.org/10.1038/s41597-024-03341-w
```

In addition, use of the geospatial data should credit OpenStreetMap. OpenStreetMap may be credited using `© OpenStreetMap contributors` or by linking to their copyright page ([https://www.openstreetmap.org/copyright](https://www.openstreetmap.org/copyright))


The following citations should be used to credit AidData for the GCDF v3 data if used (i.e., the underlying project level data). Please note: Both works listed below count as the official citation for the GCDF v3 dataset:
```
Custer, S., Dreher, A., Elston, T.B., Escobar, B., Fedorochko, R., Fuchs, A., Ghose, S., Lin, J., Malik, A., Parks, B.C., Solomon, K., Strange, A., Tierney, M.J., Vlasto, L., Walsh, K., Wang, F., Zaleski, L., and Zhang, S. 2023. Tracking Chinese Development Finance: An Application of AidData’s TUFF 3.0 Methodology. Williamsburg, VA: AidData at William & Mary.

Dreher, A., Fuchs, A., Parks, B. C., Strange, A., & Tierney, M.J. 2022. Banking on Beijing: The Aims and Impacts of China’s Overseas Development Program. Cambridge, UK: Cambridge University Press.
```


<br/>

## Contributing

Whether you have questions about usage, discover a bug, or just want to engage with others using the data, there is a place for you!

[GitHub Discussions](https://github.com/aiddata/gcdf-geospatial-data/discussions) are a great place to ask questions, see what others are talking about, and share research, applications, and ideas. If you'd prefer to get involved in more technical aspects, want to suggest improvements, discover a bug in the code, or run into issues with the data, then please utilize [GitHub's Issues](https://github.com/aiddata/gcdf-geospatial-data/issues).



<br/>

## Replication

At AidData we believe in transparency and making our work replicable. All code and data necessary to rebuild the GeoGCDF v3 is contained within this repository. Documentation on how to replicate the environment, configure settings, and manage components of the data pipeline are provided in the [replication overview readme](examples/replication/overview.md). Once your environment and settings are ready, you can follow the steps in the [gcdf_v3 replication readme](examples/replication/gcdf_v3.md) to replicate the dataset. If you encounter any bugs or other issues, please contact us as described above in the **Contributing** section.
