import os
import datetime

import numpy as np
import dask.dataframe as dd
from luigi import ExternalTask, Task, Parameter, ListParameter, DateParameter

from .utils.postgres_target import PostgresTableTarget
from .utils.parquet_target import ParquetTarget
from .utils.location_regex import get_state, get_country


class GetPostgresTable(ExternalTask):
    """
    External Task to check if Postgres Table exists to input into the luigi pipeline
    """

    # takes in the name of the table to be checked in the database
    table = Parameter()

    # takes in date because the database name includes the date it was created
    date = DateParameter(default=datetime.datetime.today())

    def output(self):
        return PostgresTableTarget(
            host="localhost",
            database=os.environ["POSTGRES_DB"] + "_" + self.date.strftime("%Y%m%d"),
            user=os.environ["POSTGRES_USER"],
            password="",
            table=self.table,
        )


class QueryPostgresTable(Task):
    """
    Abstract Class that extracts required columns from the Postgres DB, cleans the data and outputs parquet file
    """

    # string of uri to connect to database
    connection = Parameter(
        default="postgresql+psycopg2://{user}@localhost/{database}".format(
            user=os.environ["POSTGRES_USER"], database=os.environ["POSTGRES_DB"]
        )
    )

    # name of table to retrieve from database
    table = Parameter(default="reviews")

    # columns to retrieve from database
    query_columns = ListParameter(default=[])

    def requires(self):
        return GetPostgresTable(self.table)

    def output(self):
        # as a form of salting add the date to the name of the parquet output
        return ParquetTarget(
            path="data/{}_{}/".format(self.__class__.__name__, datetime.date.today())
        )

    def get_output(self):
        """Returns a computed dask dataframe to make it easier for running analysis"""
        return self.output().read_dask().compute()

    @staticmethod
    def preprocess(self, ddf):
        """To be implemented by the child class depending on the type of preprocessing required"""
        return NotImplementedError

    def run(self):
        # reads in the columns required from the table as a dask dataframe
        ddf = dd.read_sql_table(
            table=self.table,
            uri=self.connection,
            index_col="id",
            columns=self.query_columns,
        )

        # makes a call to self.preprocess where the required processing for selected data is performed
        ddf = self.preprocess(ddf)

        # write the dask dataframe as a parquet output
        self.output().write_dask(ddf, compression="gzip")


class QueryRatings(QueryPostgresTable):
    """
    Extracts only the ratings column (all numerical values) from the table for future aggregated analysis
    """

    # columns to retrieve from database
    query_columns = ListParameter(
        default=[
            "cultureandvaluesrating",
            "seniorleadershiprating",
            "compensationandbenefitsrating",
            "careeropportunitiesrating",
            "worklifebalancerating",
            "overallnumeric",
            "ceorating",
        ]
    )

    def preprocess(self, ddf):
        # replace NA values with 0 which will later be excluded from aggregated analysis depending on the column
        # rating values can only be from 1-5
        return ddf.fillna(0)


class QueryRatingsByCountry(QueryPostgresTable):
    """
    Creates a dask dataframe with mean rating values and total review count grouped according to country
    """

    # columns to retrieve from database
    query_columns = ListParameter(
        default=[
            "location",
            "overallnumeric",
            "cultureandvaluesrating",
            "seniorleadershiprating",
            "compensationandbenefitsrating",
            "careeropportunitiesrating",
            "worklifebalancerating",
        ]
    )

    def preprocess(self, ddf):
        # extract country from location column using custom regex function and replace empty string with NA
        ddf["country"] = ddf.location.apply(get_country, meta=str).replace("", np.nan)

        # add new column count assigning value 1 to get sum of reviews per country
        ddf["count"] = 1

        # drop location since it is no longer needed
        ddf = ddf.drop("location", axis=1)

        # drop any country rows with NA values
        ddf = ddf.dropna(subset=["country"])

        # groupby country where the count column is added and ratings column averaged
        cols = [val for val in ddf.columns if val != "count"]
        ddf = dd.concat(
            [
                ddf.groupby(["country"])["count"].sum(),
                ddf.groupby(["country"])[cols].mean().round(1),
            ],
            axis=1,
        )
        return ddf


class QueryJobInfoOutlook(QueryPostgresTable):
    """Creates a dask dataframe that columns business outlook, satisfaction level and adds country column """

    # columns to retrieve from database
    query_columns = ListParameter(
        default=[
            "location",
            "businessoutlook",  # col values are: getting better, getting worse, staying the same
            "overall",  # col values are: very dissatisfied, dissatisfied, neutral, satisfied, very satisfied
        ]
    )

    def preprocess(self, ddf):
        # extract the country from the location column using custom regex function
        ddf["country"] = ddf.location.apply(get_country, meta=str).fillna("").dropna()

        # drop location col since it is no longer needed
        ddf = ddf.drop("location", axis=1)

        # add count column to keep track of reviews for future analysis
        ddf["count"] = 1
        return ddf


class QueryRatingsByState(QueryPostgresTable):
    """Creates a dask dataframe with mean rating values and total review count grouped according to US state"""

    # columns to retrieve from database
    query_columns = ListParameter(
        default=[
            "location",
            "overallnumeric",
            "cultureandvaluesrating",
            "seniorleadershiprating",
            "compensationandbenefitsrating",
            "careeropportunitiesrating",
            "worklifebalancerating",
        ]
    )

    def preprocess(self, ddf):
        # extract the state from location with custom regex function
        ddf["state"] = ddf.location.apply(get_state, meta=str).replace("", np.nan)

        # add a count column to keep track of number of reviews per state
        ddf["count"] = 1

        # drop location since not required anymore
        ddf = ddf.drop("location", axis=1)

        # drop any missing values from state column
        ddf = ddf.dropna(subset=["state"])

        # group by state with the ratings columns average and the count column added up
        cols = [val for val in ddf.columns if val != "count"]
        ddf = dd.concat(
            [
                ddf.groupby(["state"])["count"].sum(),
                ddf.groupby(["state"])[cols].mean().round(1),
            ],
            axis=1,
        )
        return ddf
