"""This is an example settings.py file."""

import os


# specify which of your DBs will hold the data created by pylytics
pylytics_db = "example"

# define all databases
DATABASES = {
    'example': {
        'host': 'localhost',
        'user': 'test',
        'passwd': 'test',
        'db': 'example',
    }
}

# An optional client configuration file location (i.e. my.cnf).
# For details of what this file does, visit:
# http://dev.mysql.com/doc/refman/5.1/en/option-files.html
# Use this to tune performance (e.g. max_allowed_packet), but keep connection
# parameters in DATABASES.
CLIENT_CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'my.cnf')
