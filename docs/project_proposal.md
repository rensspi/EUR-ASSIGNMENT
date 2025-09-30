# Project Proposal Template

## Students Information
- Name: Rens Spierings
- Student ID: 792563
- Email: rensspi@live.nl
  

- Name: Maarten Baas
- Student ID: 792561
- Email: m.baas@wsrl.nl

## Business Context
Describe the business or domain you're working with:
- Industry: Local government in water management
- Company/Organization: Waterschap Rivierenland
- Business Problem: Data quality of core data in our organization is unknown and unquantifiable. This makes is hard for users to assess the usability of the data. For gorvernments transparency is valued. It is guaranteed with the law 'Wet Open Overheid'. If a decision is made based on a certain dataset, other parties have the right to ask why a decision is made and based on which data. As a government you have to be able to explain how the quality is being guaranteed and based on what requirements. Checks on completeness, timeliness and accuracy are a good place to start describing data quality. To find irregular patterns in our data we can use machine learning. This way we can classify the datasets based on data quality. At least that is the idea we propose to test in this proposal.

## Data Sources
List the data sources you plan to use:
1. **Primary Source**: Spatial database DAMO in which we register our assets. DAMO is an abbreviation of Data Afspraken Modelmatig Ondersteund (DAMO-website: https://damo.hetwaterschapshuis.nl/DAMO%202.5/Objectenhandboek%20DAMO%202.5/html/DAMO%20Objectenhandboek.html, visited 30-09-2025). In our local database DAMO is adjusted with additional local tables and attributes. The RDBMS consists out of 140 object-types which are related to one and each other.
2. **Secondary Source**: Potential use of Basisregistratie Grootschalige Topografie (BGT). The BGT is a nationwide standard to store geographical data based on real location. The dataset is managed by all the governments together and is part of the Dutch system of so called base registries (Geonovum: https://www.geonovum.nl/geo-standaarden/bgt-imgeo, visited 30-09-2025). 
3. **Supporting Data**: GegevensKnooppunt Waterschappen configuration files. These mapping files are used to export the data as open data and contain information on the use of the data, which might be useful for assessing data quality.

## Project Objectives
What will your data pipeline accomplish?
- **Primary Goal**: Quantifying data quality for three specific quality requirments. Completeness, timeliness, accuracy (spatial). 
- **Secondary Goals**: Finding spatial pattern in data quality
- **Success Metrics**: We will define three data quality requirements to test our model. For each requirement we check usability of the results, the reusability of the model on other objects, the use of technical resources by the model. We know certain objects are not managed but were filled once. These objects should be exposed by the model.

## Technical Approach
- **Data Ingestion**: We use our ETL-software FME (website FME: https://fme.safe.com/, visited 30-09-2025) to read en preprocess the data.
- **Data Processing**: We will convert our data to a dataset readible for the model. This means we will use a copy of the database. Some numeric fields are not ordinal. For some tests it might be necessary to apply one-hot encoding. For the ordinal fields min-max scaling might be useful. 
- **Data Storage**: PostGIS-database. We want to use an open source alternative for the ESRI ArcSDE on Oracle database we use now. As a public organsation we want to prevent vedor locking so experiencing alternatives helps keeping options in mind. 
- **Analytics/ML**: We will create a model which is capable of finding spatial, temporal and other patterns in our DAMO-database. 

## Timeline
Plan your 12-week project timeline based on your chosen components:

- **Week 1-3**: Writing proposal and writing off a lot of alternative ideas
- **Week 4-6**: Determining the three specific data quality requirements to train our model on. Preproccesing the data and setting up a PostGIS copy of the database. 
- **Week 7-9**: Training our model and planning the pipeline
- **Week 10-12**: Analysing the results and finishing the documentation including data visualization

## Approval
**Instructor Approval**: _________________ **Date**: _________
