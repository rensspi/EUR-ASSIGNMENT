# Architecture Design

## System Overview

The PDF-file ResultingDatapipeline in this folder gives an overview of the resulting data pipeline

### Data Ingestion
We read the history tables via an Oracle Spatial connection in FME and wrote the data as geoParquet files

### Data Processing
The pipeline splits up in the two tests we plan. We tried to do the processing in Python, but eventually we used FME for the Timeliness tests. We saved the results as files: JSON and CSV's. The idea is these results can be reused in a later stage by data scientists

### Data Analysing
Eventually the data processing took more time than anticipated, so we didn't have as much time as we wanted to analyse the results. We did some first analyses using Excel and ArcGIS Pro. We provide scripting to present the results in an excel file. This helps with sharing the results to our data stewards.

### Data Visualisation
Data Visualisation is done in ArcGIS Portal. We are GIS-experts so we wanted to show some geographical results.ss