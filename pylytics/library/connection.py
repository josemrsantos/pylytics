"""
Utilities for making database connections easier.
"""


import MySQLdb

from pylytics.library.exceptions import classify_error
from pylytics.library.settings import Settings


# Load settings from one or more settings modules.
#
settings = Settings()
#
# Each settings module will be loaded in turn and applied over the top of
# those previously loaded. There can be 'settings' modules (which should
# not be committed to the code repository) and 'default_settings' modules
# (which should). The former provides local overrides for those within the
# latter and settings with a broader scope are loaded before those with a
# narrower scope.
#
# The order of settings modules listed below is therefore important and
# should be maintained.
#
module_names = [
    "default_settings",       # default application settings
    "settings",               # local application settings
    "test.default_settings",  # default test settings
    "test.settings",          # local test settings
]
for module_name in module_names:
    try:
        more_settings = Settings.from_module(module_name)
    except ImportError:
        pass
    else:
        settings.update(more_settings)


def run_query(database, query):
    """
    Very high level interface for running database queries.

    Example usage:
    response = run_query('ecommerce', 'SELECT * from SOME_TABLE')

    """
    with DB(database) as database:
        response = database.execute(query)
        return response


class DB(object):
    """
    Create a connection to a database in settings.py.

    High level usage:
    with DB('ecommerce') as db:
        response = db.execute('SELECT * FROM SOME_TABLE')

    Lower level usage:
    example = DB('example')
    example.connect()
    content = example.execute('SELECT * FROM SOME_TABLE')
    example.close()

    """

    # List of SQL types
    field_types = {
        0: 'DECIMAL',
        1: 'INT(11)',
        2: 'INT(11)',
        3: 'INT(11)',
        4: 'FLOAT',
        5: 'DOUBLE',
        6: 'TEXT',
        7: 'TIMESTAMP',
        8: 'INT(11)',
        9: 'INT(11)',
        10: 'DATE',
        11: 'TIME',
        12: 'DATETIME',
        13: 'YEAR',
        14: 'DATE',
        15: 'VARCHAR(255)',
        16: 'BIT',
        246: 'DECIMAL',
        247: 'VARCHAR(255)',
        248: 'SET',
        249: 'TINYBLOB',
        250: 'MEDIUMBLOB',
        251: 'LONGBLOB',
        252: 'BLOB',
        253: 'VARCHAR(255)',
        254: 'VARCHAR(255)',
        255: 'VARCHAR(255)',
    }

    def __init__(self, database):
        if database not in (settings.DATABASES.keys()):
            raise ValueError("The database {} isn't recognised - check "
                             "your settings in settings.py".format(database))
        else:
            self.database = database
            self.connection = None

    def connect(self):
        if not self.connection:
            db_settings = settings.DATABASES[self.database]
            self.connection = MySQLdb.connect(**db_settings)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close(self):
        """You should always call this after opening a connection."""
        if self.connection:
            self.connection.commit()
            self.connection.close()

    def execute(self, query, values=None, many=False, get_cols=False):
        """ Executes the given `query` through the currently open connection.

        There must be a connection established before calling this method.

        `values` should contain the data to be inserted when issuing `INSERT`
        or `REPLACE` queries. If the `many` flag is set to `True`, `values` is
        expected to be an iterable of iterables. Otherwise, `values` should
        contain the data directly.
        """
        if not self.connection:
            raise IOError("Cannot execute without a database connection")

        data = None
        cols_names = None
        cols_types = None
        cursor = self.connection.cursor()

        try:
            if not values:
                # SELECT query
                cursor.execute(query)
                data = cursor.fetchall()
            else:
                # INSERT or REPLACE query
                if many:
                    cursor.executemany(query, values)
                else:
                    cursor.execute(query, values)
        except MySQLdb.ProgrammingError as error:
            classify_error(error)
            raise

        if get_cols:
            # Get columns list
            if values:
                raise ValueError("Cannot return columns if INSERT/REPLACE "
                                 "values are also specified")
            cols_names, cols_types_ids = zip(*cursor.description)[0:2]
            try:
                cols_types = [self.field_types[i] for i in cols_types_ids]
            except KeyError as error:
                raise LookupError("The column type '{}' cannot be found in "
                                  "the field_types dictionary".format(error))

        cursor.close()

        if get_cols:
            return data, cols_names, cols_types
        else:
            return data

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.close()
