## Using Luigi to Build a Django Dashboard to Visualize Employee Reviews

This module is responsible for building an ETL pipeline that extracts data from the Postgres Database, 
cleans and preprocesses the data and then runs analysis on the data to be displayed on a 
Django web dashboard.

Following is the structure of this module displaying the important files and directories used here:
 ```bash
<glassdoor_dashboard>                      # application root folder
    |
    |---processdata/__init__.py             # Django app that processes the employee reviews from the Postgres database
    |---processdata/
    |       |---<migrations>                # Model migrations - not used for this app since data was retrieved locally from Postgres database
    |       |---<utils>
    |       |       |---<location_regex>    # Provides regex functions to retrieve state and country from the location data retrieved
    |       |       |---<parquet_target>    # Dask Target for Luigi that can be used to output parquet files (a copy from csci_utils)
    |       |       |---<postgres_target>   # Postgres Target for Luigi to check that a Postgres table in a given database exists
    |       |---apps.py                     # Includes application configurations for this app
    |       |---tasks.py                    # Includes luigi tasks to be run to extract data from Postgres Dataabse for analysis
    |       |---urls.py                     # Defines the mapping of the url 'index.html' to the views created in this app
    |       |---views.py                    # Renders the web responses that triggers the build of luigi tasks defined in tasks.py
    |------------------------------------
```
## Key Features
### Implementation of Luigi Target - PostgresTableTarget
For the purpose of this project, data from the Glassdoor API had to be retrieved and stored locally in a PostgreSQL Database.
In order to build an ETL pipeline to extract data from the database I required a Luigi Target which would check for the existence of a table. 
This was not provided within the Luigi modules so I implemented a PostgresTableTarget that would check if a table exists before
going ahead with the next tasks. This has been implemented in [processdata/utils/postgres_target](https://github.com/csci-e-29/glassdoor-dashboard/blob/master/processdata/utils/postgres_target/__init__.py)

### Implementation of Luigi Task - QueryPostgresTable
This abstract class was implemented to simplify the process of querying columns from the database, cleaning and preprocessing the data, and storing the data as a parquet output. 
Since the data analysis mainly dealt with columnar data, dask was used for the analytics and parquet was used for columnar storage. As a result of this class
a Luigi task could be easily created by inheriting this class and modifying the required columns needed to be queried and 
implementing the preprocess function within this class. The implementation of this class can be found in [processdata/tasks.py](https://github.com/csci-e-29/glassdoor-dashboard/blob/master/processdata/tasks.py) 
