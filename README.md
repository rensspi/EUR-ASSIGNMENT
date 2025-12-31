# Data and AI Engineering - Baseline data quality

## This repository contains the data pipeline build as assignment for the course Data and AI Engineering at the EQI

The repository contains parts of a data pipeline. The pipeline is designed to calculate the completeness and timeliness of a set of tables. For those who stumbled on this repository in search for a complete solution, we have to dissappoint you. Parts of the solution are implemented in Safe Software's FME. 

## Project Structure

```
data-and-ai-engineering/
├── README.md                 # This file - project overview
├── requirements.txt          # Ddependencies
├── data/                     # Data examples
├── src/                      # Source code modules
└── docs/                     # Project documentation
```

## Design starting points

The pipeline is the result of a course assignment. Some design choises are made with educational priorities instead of good design. We check on two base data quaality requirements: Completeness and timeliness.

Completeness is defined by the amount of NULL-values in the table. Actuality is defined using the ESRI-archiving columns. The concept is explained in the accompanying paper.

### Data Ingestion
Data Ingestion is done with FME. The repository contains some screenshots of the process and the Python code used within FME's Python creators and Python callers.

### Data Procesing

We aimed to do most of the data processing in Python. We did eventually have to use FME for hardware reasons.

### Data Validation
Most data validation is done in ArcGIS Portal, but there was some Python used to create an excel-file for data exploration.



## License

This is the result of a course. The resulting lines of codes do work. However, we do not reccomend using this project in a production setting. Design choises have been made for educational purposes and adaptations have been made in later stages to make it work. The authors learned a lot and would not design it this way a second time.
