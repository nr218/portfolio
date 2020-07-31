from luigi import Target
from psycopg2 import errorcodes, ProgrammingError, connect

# https://groups.google.com/forum/#!topic/luigi-user/Nax5tm2QAx8
# note in order to follow luigi's base code, I have adapted the code from luigi.contrib.postgres.PostgresTarget
# and modified it to meet my objective


class PostgresTableTarget(Target):
    """
    Target for a table in a Postgres Database.
    This will usually be instantiated as an External Task to check if a Postgres Table exists.
    """

    # default Postgres Port if not provided
    DEFAULT_DB_PORT = 5432

    def __init__(self, host, database, user, password, table, port=None):
        """
        Args:
            host (str): Postgres server address. Possibly a host:port string.
            database (str): Database name
            user (str): Database user
            password (str): Password for specified user
            table (str): Table in Database which the target checks if it exists
        """
        if ":" in host:
            self.host, self.port = host.split(":")
        else:
            self.host = host
            self.port = port or self.DEFAULT_DB_PORT
        self.database = database
        self.user = user
        self.password = password
        self.table = table

    def connect(self):
        """
        Get a psycopg2 connection object to the database where the table is.
        """
        connection = connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )
        connection.set_client_encoding("utf-8")
        return connection

    def exists(self, connection=None):
        """
        Check if the table in the Postgres Database exists
        """
        # if connection is not provided create postgres connection
        if connection is None:
            connection = self.connect()
            connection.autocommit = True
        cursor = connection.cursor()

        # check if first row of data exists in the database table
        try:
            cursor.execute(
                """SELECT * FROM {table} LIMIT 1;""".format(table=self.table)
            )
            row = cursor.fetchone()

        # if data doesn't exist throw error
        except ProgrammingError as e:
            if e.pgcode == errorcodes.UNDEFINED_TABLE:
                row = None
            else:
                raise

        return row is not None
