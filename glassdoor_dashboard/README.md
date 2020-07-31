# Visualizing Employee Satisfaction

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Objective
The purpose of this project is to 
1. Build an ETL pipeline using Luigi to load data from the Glassdoor partner API, save it to Postgres SQL Database and perform data cleaning for data analysis to be conducted 
2. Create a Django web dashboard with summaries and insights about the reviews using Dask for UI Analytics and Plotly for data visualization
 
## Important Notes
Please note that this tool is company specific. In order to utilize the features of this tool, your company needs to be a Glassdoor Partner and will need to request the following information from Glassdoor: 

1. PARTNER_ID
2. PARTNER_KEY
3. EMPLOYER_ID 

You will also need access to a PostgreSQL Database where the reviews from Glassdoor will be stored. Please set up the following environment variables to access Postgres SQL database:

1. POSTGRES_DB (Database Name)
2. POSTGRES_USER (User Name)

Please see the .env.sample file for further environment variables that need to be listed and their default values

## Getting Started
### Prerequistes
 * Python 3.7
 * Pipenv
 * PostgreSQL


### Installation
Get the project up and running locally in 5 simple steps

 1. Create a personal Fork of this repository
 
 2. Clone the fork with HTTPS, using your local terminal to a preferred location and cd into that project.
 ```bash
git clone https://github.com/your_username/glassdoor-dashboard.git

Cloning into 'glassdoor-dashboard'...

cd glassdoor-dashboard/
```
3. Activate your pipenv environment
 ```bash
pipenv install
```
4. Store the employee reviews pulled from Glassdoor API in  the PostgreSQL Database by running:
```bash
pipenv run python -m glassdoor_data
```
5. Run the local server and DONE!
 ```bash
pipenv run python manage.py runserver

May 11, 2020 - 18:03:38
Django version 3.0.6, using settings 'core.settings'
Starting development server at http://127.0.0.1:8000/
Quit the server with CONTROL-C.
```

You will now be able to view the Django Dashboard displaying the information retrieved from the employee reviews on Glassdoor
![Dashboard screenshot 1](https://github.com/csci-e-29/glassdoor-dashboard/blob/master/core/static/assets/img/dashboard1.png)
![Dashboard screenshot 2](https://github.com/csci-e-29/glassdoor-dashboard/blob/master/core/static/assets/img/dashboard2.png)

## Tools Used
* [PostgreSQL](https://www.postgresql.org/) is a free and open-source relational database management system emphasizing extensibility and SQL compliance.
* [Luigi](https://luigi.readthedocs.io/en/stable/) is a Python package developed by Spotify that helps build complex pipelines of batch jobs
* [Django](https://www.djangoproject.com/) is a high-level Web framework that encourages rapid development and clean, pragmatic design.
* [Appseed](https://appseed.us/) provides boilerplate code for building Django dashboard and many other full-stack apps for multipurpose application which are ready for deployment
* [Dask](https://dask.org/) is an open source library for parallel computing written in Python.
* [Plotly](https://plotly.com/) is the leading front-end for ML & data science models in Python, R, and Julia.


