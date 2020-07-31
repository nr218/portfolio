import requests
from luigi import Task
import luigi
from ..api_request.__init__ import get_params
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import os
from datetime import datetime

# Sources considered
# https://markhneedham.com/blog/2017/03/25/luigi-externalprogramtask-example-converting-json-csv/
# https://towardsdatascience.com/how-to-handle-large-datasets-in-python-with-pandas-and-dask-34f43a897d55


class MakeDB(Task):
    # allow the task to take in a city parameter if user does not want the full dataset
    city = luigi.Parameter(default="")

    def get_reviews(self):
        """
        Iterates over paginated API call to get all reviews
        :return:
        """
        url = "https://api.glassdoor.com/api/api.htm"
        headers = {"user-agent": "Mozilla/5.0"}

        first_page = requests.get(url, params=get_params(), headers=headers).json()
        yield first_page

        num_pages = first_page["response"]["totalNumberOfPages"]

        for page in range(2, num_pages + 1):
            next_page = requests.get(
                url, params=get_params(city= self.city, pageNumber=page), headers=headers
            ).json()
            yield next_page


    def run(self):
        # define our user name, username, and database name, dbname:
        # dbname will include the name the task is run
        dbname = os.environ["POSTGRES_DB"]+"_"+datetime.today().strftime('%Y%m%d')
        username = os.environ["POSTGRES_USER"]

        # Then we create the database engine using the above command with the values filled in.
        # Notice below how we do not include the password since we don't want to use one.
        # We also make the host to be localhost since we are going to have the database local on our machine.
        engine = create_engine("postgresql+psycopg2://%s@localhost/%s" % (username, dbname))
        print(engine.url)

        # Now create the database, if it doesn't already exist
        if not database_exists(engine.url):
            create_database(engine.url)
        print(database_exists(engine.url))

        # connect to our database
        psycopg2.connect(database=dbname, user=username)
        conn = psycopg2.connect(database=dbname, user=username)

        cur = conn.cursor()
        # SQL Commands
        # Create table with the column names (review fields on Glassdoor) and the object type
        create_table = """CREATE TABLE IF NOT EXISTS Reviews
                            ( 
                            attributionURL TEXT,
                            id INTEGER,
                            currentJob BOOLEAN,
                            reviewDateTime TEXT,
                            jobTitle VARCHAR,
                            location VARCHAR,
                            jobTitleFromDb VARCHAR,
                            headline TEXT,
                            pros TEXT,
                            cons TEXT,
                            isFeaturedReview BOOLEAN,
                            lengthOfEmployment VARCHAR,
                            employmentStatus VARCHAR,
                            jobInformation VARCHAR,
                            newReviewFlag BOOLEAN,
                            advice TEXT,
                            businessOutlook VARCHAR,
                            overallNumeric FLOAT,
                            overall VARCHAR,
                            cultureAndValuesRating FLOAT,
                            seniorLeadershipRating FLOAT,
                            compensationAndBenefitsRating FLOAT,
                            careerOpportunitiesRating FLOAT,
                            workLifeBalanceRating FLOAT,
                            ceoRating INTEGER,
                            ceoApproval VARCHAR, 
                            recommendToFriend BOOLEAN, 
                            helpfulCount INTEGER,
                            notHelpfulCount INTEGER,
                            totalHelpfulCount INTEGER,
                            employerResponse TEXT)
                            """
        cur.execute(create_table)

        # Populate the SQL table with the reviews obtained from the API call
        for review in self.get_reviews():

            review_list = review["response"]["reviews"]

            for review_dict in review_list:
                columns = ", ".join(str(x).replace("/", "_") for x in review_dict.keys())
                values = ", ".join(
                    "'" + str(x).replace("/", "_").replace("'", "''") + "'"
                    for x in review_dict.values()
                )

                sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ("Reviews", columns, values)
                cur.execute(sql)

        # converting type datetime to date for analysis
        create_date_col = """ALTER TABLE reviews ADD reviewDate DATE; UPDATE reviews SET reviewDate=TO_DATE(reviewDateTime, 'YYYY/MM/DD');"""
        cur.execute(create_date_col)
        conn.commit()
        conn.close()