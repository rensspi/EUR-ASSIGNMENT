# Data Ingestion
We chose not to work directly on the database. We want to create a first assessment of the baseline quality criteria as defined in the proposal. Since, we do not want to disturb our production process with this Proof of Concept we decided to extract the data first to a local file system. Another reason to this, is that it provides us with the opportunity to test our aquired skills. Our source data are tables with a geometry collumn. These tables are stored in an Oracle database with ESRI's Spatial Data Engine (SDE) (ESRI, 1999). The tables are configured with the versioning and archiving mechanics provided in the ESRI SDE. The historic rows are stored in a different table and we need to read these tables. We used ETL-software FME from Safe Software (Safe Software, 2025).

# Selecting the tables to read
First step is to create a list of the tables which use archiving. We have to use an ESRI python package wich can access the SDE. This package is arcpy. 

References:
ESRI, 1999, Spatial Database Engine - technical paper, https://support.esri.com/en-us/technical-paper/spatial-database-engine-sde-270 
Safe Software, 2025, Website introducing FME, https://fme.safe.com/ visited on 26-11-2025