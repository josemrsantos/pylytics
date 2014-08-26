
class TableMetaclass(type):
    """ Metaclass for constructing all Table classes. This applies number
    of magic attributes which are used chiefly for reflection.
    """

    def __new__(mcs, name, bases, attributes):
        attributes.setdefault("__tablename__", _camel_to_snake(name))

        column_set = _ColumnSet()
        for base in bases:
            column_set.update(base.__dict__)
        column_set.update(attributes)

        attributes["__columns__"] = column_set.columns
        attributes["__primarykey__"] = column_set.primary_key

        cls = super(TableMetaclass, mcs).__new__(mcs, name, bases, attributes)

        # These attributes apply to subclasses so should only be populated
        # if an attribute exists with that name. We do this after class
        # creation as the attribute keys will probably only exist in a
        # base class, not in the `attributes` dictionary.
        if "__dimensionkeys__" in dir(cls):
            cls.__dimensionkeys__ = column_set.dimension_keys
        if "__metrics__" in dir(cls):
            cls.__metrics__ = column_set.metrics
        if "__naturalkeys__" in dir(cls):
            cls.__naturalkeys__ = column_set.natural_keys

        return cls


class Table(object):
    """ Base class for all Table classes. The class represents the table
    itself and instances represent records for that table.

    This class has two main subclasses: Fact and Dimension.

    """
    __metaclass__ = TableMetaclass

    # All these attributes should get populated by the metaclass.
    __columns__ = NotImplemented
    __primarykey__ = NotImplemented
    __tablename__ = NotImplemented

    # These attributes aren't touched by the metaclass.
    __source__ = None
    __tableargs__ = {
        "ENGINE": "InnoDB",
        "CHARSET": "utf8",
        "COLLATE": "utf8_bin",
    }

    INSERT = "INSERT"

    @classmethod
    def build(cls):
        """ Create this table. Override this method to also create
        dependent tables and any related views that do not already exist.
        """
        try:
            # If this uses the staging table or similar, we can
            # automatically build this here too.
            cls.__source__.build()
        except AttributeError:
            pass
        cls.create_table(if_not_exists=True)

    @classmethod
    def create_table(cls, if_not_exists=False):
        """ Create this table in the current data warehouse.
        """
        if if_not_exists:
            verb = "CREATE TABLE IF NOT EXISTS"
        else:
            verb = "CREATE TABLE"
        columns = ",\n  ".join(col.expression for col in cls.__columns__)
        sql = "%s %s (\n  %s\n)" % (verb, cls.__tablename__, columns)
        for key, value in cls.__tableargs__.items():
            sql += " %s=%s" % (key, value)
        Warehouse.execute(sql, commit=True)

    @classmethod
    def drop_table(cls, if_exists=False):
        """ Drop this table from the current data warehouse.
        """
        if if_exists:
            verb = "DROP TABLE IF EXISTS"
        else:
            verb = "DROP TABLE"
        sql = "%s %s" % (verb, cls.__tablename__)
        Warehouse.execute(sql, commit=True)

    @classmethod
    def table_exists(cls):
        """ Check if this table exists in the current data warehouse.
        """
        connection = Warehouse.get()
        return cls.__tablename__ in connection.table_names

    @classmethod
    def fetch(cls, since=None):
        """ Fetch data from the source defined for this table and
        yield as each is received.
        """
        if cls.__source__:
            try:
                for inst in cls.__source__.select(cls, since=since):
                    yield inst
            except Exception as error:
                log.error("Error raised while fetching data: (%s: %s)",
                          error.__class__.__name__, error,
                          extra={"table": cls.__tablename__})
                raise
            else:
                # Only mark as finished if we've not had errors.
                cls.__source__.finish(cls)
        else:
            raise NotImplementedError("No data source defined")

    @classmethod
    def insert(cls, *instances):
        """ Insert one or more instances into the table as records.
        """
        if instances:
            columns = [column for column in cls.__columns__
                       if not isinstance(column, AutoColumn)]
            sql = "%s INTO %s (\n  %s\n)\n" % (
                cls.INSERT, escaped(cls.__tablename__),
                ",\n  ".join(escaped(column.name) for column in columns))
            link = "VALUES"
            for instance in instances:
                values = []
                for column in columns:
                    value = instance[column.name]
                    values.append(dump(value))
                sql += link + (" (\n  %s\n)" % ",\n  ".join(values))
                link = ","
            Warehouse.execute(sql, commit=True)

    @classmethod
    def update(cls, since=None):
        """ Fetch some data from source and insert it directly into the table.
        """
        instances = list(cls.fetch(since=since))
        count = len(instances)
        log.info("Fetched %s record%s", count, "" if count == 1 else "s",
                 extra={"table": cls.__tablename__})
        cls.insert(*instances)

    def __getitem__(self, column_name):
        """ Get a value by table column name.
        """
        for key in dir(self):
            if not key.startswith("_"):
                column = getattr(self.__class__, key, None)
                if isinstance(column, Column) and column.name == column_name:
                    value = getattr(self, key)
                    return None if value is column else value
        raise KeyError("No such table column '%s'" % column_name)

    def __setitem__(self, column_name, value):
        """ Set a value by table column name.
        """
        for key in dir(self):
            if not key.startswith("_"):
                column = getattr(self.__class__, key, None)
                if isinstance(column, Column) and column.name == column_name:
                    setattr(self, key, value)
                    return
        raise KeyError("No such table column '%s'" % column_name)