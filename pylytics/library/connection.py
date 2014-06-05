"""
Utilities for making database connections easier.
"""
import math
import ConfigParser

import warnings

import MySQLdb

try:
    import settings
except ImportError:
    # Fall back to empty settings to make testing easier.
    warnings.warn("No settings defined")

    class Settings(object):
        pylytics_db = None
        DATABASES = {}

    settings = Settings()


class UnknownColumnTypeError(Exception):

    def __init__(self, error):
        self.error = error

    def __str__(self):
        return "The type code {}, which has been retrieved from " \
               "a SELECT query, doesn't exist in the " \
               "'field_types' dictionary.".format(self.error)


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
            raise Exception("The database {} isn't recognised! Check your "
                            "settings in settings.py.".format(database))
        else:
            self.database = database
            self.connection = None

    def connect(self):
        if not self.connection:
            self.connection = MySQLdb.connect(
                    **settings.DATABASES[self.database])

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close(self):
        """You should always call this after opening a connection."""
        if self.connection:
            self.connection.commit()
            self.connection.close()

    def _step_through_batch(self, query, values):
        """
        This inserts each row one by one, recording errors along the way, and
        returning the errors.

        """
        errors = []
        for value in values:
            try:
                self.execute(query, [value])
            except Exception as exception:
                errors.append(exception)
        return errors

    def _get_packet_size(self, query, values):
        """
        Approximates the packet size.
        
        This follows the same method as used in MySQLdb to generate the
        query.
        
        TODO Might be better off not using executemany at all here.
        Just use execute?
        
        """
        m = MySQLdb.cursors.insert_values.search(query)
        p = m.start(1)
        e = m.end(1)
        qv = m.group(1)
        q = [qv % self.connection.literal(a) for a in values]
        r = '\n'.join([query[:p], ',\n'.join(q), query[e:]])
        return len(r)

    def insert_many(self, query, values):
        """
        This is a special case for inserting a large number of rows.
        
        It tries to insert a batch of rows. If any of them fail, it steps
        through and inserts them one at a time. It then tries to insert the
        next batch of rows, and so on until all rows are inserted.

        A list of errors is returned at the end.

            batch_size: The number of rows inserted in each batch.

        """
        server_variables_raw = self.execute(
            "SHOW VARIABLES LIKE 'max_allowed_packet'")

        server_variables = dict(server_variables_raw)

        if 'max_allowed_packet' in server_variables.keys():
            server_max_allowed_packet = int(server_variables['max_allowed_packet'])
        else:
            raise Exception(
                'Unable to retrieve max_allowed_packet from server.')

        # TODO This is too late ... needs to be loaded in earlier ...
        # However, can still introspect here for now.
        if hasattr(settings, 'CLIENT_CONFIG_FILE'):
            client_config_file_location = settings.CLIENT_CONFIG_FILE
        else:
            raise Exception('CLIENT_CONFIG_FILE is missing from settings.py.')
        
        client_config_file_location = settings.CLIENT_CONFIG_FILE
        
        parser = ConfigParser.SafeConfigParser()
        parser.read(client_config_file_location)
        
        client_max_allowed_packet = parser.get('mysqld', 'max_allowed_packet')
        
        unit_prefix = {
            'K': 3,
            'M': 6,
            'G': 9,
            }

        client_max_allowed_packet = client_max_allowed_packet.upper()

        if client_max_allowed_packet.endswith(tuple(unit_prefix.keys())):
            base = client_max_allowed_packet[:-1]
            prefix = client_max_allowed_packet[-1]
            client_max_allowed_packet = int(base) * int(math.pow(10, unit_prefix[prefix]))
        else:
            client_max_allowed_packet = int(client_max_allowed_packet)

        max_allowed_packet = min(client_max_allowed_packet,
            server_max_allowed_packet)
        
                
        # TODO Need unit tests for this too - actually try inserting a billion rows.
        # will pdb help here???
        

        packet_size = self._get_packet_size(query, values)

        packet_iterations = packet_size / max_allowed_packet

        if packet_size % max_allowed_packet != 0:
            packet_iterations += 1
        
        batch_size = len(values) / (packet_iterations + 1)
        
        errors = []
        batches = [values[x: x + batch_size] for x in xrange(0, len(values),
                                                             batch_size)]
        for i, batch in enumerate(batches):
            print i
            try:
                self.execute(query, batch, many=True)
            except:
                errors.append(self._step_through_batch(query, batch))
        
        return errors

        # TODO We still haven't solved one of the core problems either, which is
        # isolating values which fail to insert so they all don't fail ...




    def _insert_many(self, query, values):
        """
        This is a special case for inserting a large number of rows.

        It tries to batch insert all the values, 

        """
        # TODO problem now is I'm not gaining very much from just using
        # executemany, halving, trying executemany, and repeat until 
        # it stops raising a packet error ... anyway - carry on with this
        # for now.
        # Lets imagine we do a query which is 1 GB. The limit is 1 MB.
        # Halving each time ... we would take ... 10 tries to reach the
        # limit. And that's the most extreme case.
        #
        
        
        # A complicating factor here is that ...
        # you can get this error too when packet size is exceeded:
        # _mysql_exceptions.OperationalError: (2006, 'MySQL server has gone away')
        
        
        
        self.execute(query, values, many=True)
        
                
        # errors = []
        # batches = [values[x: x + batch_size] for x in xrange(0, len(values),
        #                                                      batch_size)]
        # for i, batch in enumerate(batches):
        #     print i
        #     try:
        #         self.execute(query, batch, many=True)
        #     except:
        #         errors.append(self._step_through_batch(query, batch))
        # 
        # return errors





    def execute(self, query, values=None, many=False, get_cols=False):
        cursor = None
        data = None
        cols_names = None
        cols_types = None

        if not self.connection:
            raise Exception('You must connect first!')
        else:
            cursor = self.connection.cursor()

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

            if get_cols:
                # Get columns list
                if values:
                    raise Exception("Only works on a SELECT query.")
                cols_names, cols_types_ids = zip(*cursor.description)[0:2]
                try:
                    cols_types = [self.field_types[i] for i in cols_types_ids]
                except Exception as e:
                    raise UnknownColumnTypeError(e)

            cursor.close()

        if get_cols:
            return (data, cols_names, cols_types)
        else:
            return data

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.close()
