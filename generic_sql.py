from visidata import *

__all__ = [ 'openurl_mssql', 'openurl_oracle', 'openurl_mysql', 'SqlSchemasSheet' , 'SqlTablesSheet', 'SqlSheet']

@VisiData.api
def openurl_oracle(vd, url, filetype=None):
    return SqlSchemasSheet(db = database(url.given))

@VisiData.api
def openurl_mysql(vd, url, filetype=None):
    return SqlSchemasSheet(db = database(url.given))

@VisiData.api
def openurl_mssql(vd, url, filetype=None):
    return SqlSchemasSheet(db = database(url.given))

class SqlSchemasSheet(Sheet):
    rowtype = 'schemaInfo' #rowdef: tuple of schema name and count of tables owned by schema
    columns = [
        Column('schema_name', type = str, getter = lambda col,row: row[0]),
        Column('table_count', type = int, getter = lambda col,row: row[1])
    ]

    @asyncthread
    def reload(self):
        self.rows = []
        for row in self.db.get_schema_stats():
            try:
                r = row
            except Exception as e:
                r = e
            self.addRow(r)

    def openRow(self, row):
        return SqlTablesSheet(schema=row[0], db=self.db)


class SqlTablesSheet(Sheet):
    rowtype = 'tableInfo' #rowdef: table class instance with methods for getting row values
    columns = [
            Column('table_name', type = str, getter = lambda col,row: row.tableName),
            Column('column_count', type = int, getter = lambda col,row: row.col_count),
            Column('row_count', type = int, getter = lambda col,row: row.row_count)
            ]

    @asyncthread
    def reload(self):
        self.rows = []
        self.db.set_schema(self.schema)

        for itable in self.db.get_schema_tables():
            try:
                r = table(sqlTable = itable, db = self.db)
            except Exception as e:
                r = e
            self.addRow(r)

    def openRow(self, row):
        return SqlSheet(table=row)

class SqlSheet(Sheet):
    rowtype = 'databaseRow' #rowdef: tuple of database row values

    @asyncthread
    def reload(self):
        from sqlalchemy import select
        self.columns = []
        for col in self.table.gen_sheet_cols():
            self.addColumn(col)

        self.rows = []

        for row in self.table.get_table_rows():
            try:
                r = row
            except Exception as e:
                r = e
            self.addRow(r)

class table:
    def __init__(self, sqlTable, db):
        self.sqlTable = sqlTable
        self.tableName = sqlTable.name
        self.db = db
        self.col_count = self.get_column_count()
        self.row_count = self.get_row_count()

    def get_column_count(self):
        return len(self.sqlTable.columns)

    def get_row_count(self):
        from sqlalchemy import func
        from sqlalchemy import select
        session = self.db.Session()

        countq = select([func.count()]).select_from(self.sqlTable)
        return session.execute(countq).fetchone()[0]

    def get_table_rows(self):
        from sqlalchemy import select
        session = self.db.Session()

        q = select([self.sqlTable])
        return session.execute(q)

    def gen_sheet_cols(self):
        outCols = []
        for i,col in enumerate(self.sqlTable.columns):
            outCols.append(ColumnItem(col.name,i))

        return outCols    


class database:
    def __init__(self, url):
        from sqlalchemy import create_engine
        from sqlalchemy.engine import reflection
        from sqlalchemy.orm import sessionmaker

        self.engine = create_engine(url)
        self.inspector = reflection.Inspector.from_engine(self.engine)
        self.Session = sessionmaker()
        self.Session.configure(bind=self.engine)

    def get_schema_stats(self):
        for schema in Progress(self.inspector.get_schema_names()):
            yield (schema, len(self.inspector.get_table_names(schema = schema)))

    def set_schema(self, schema):
        self.active_schema = schema

    def get_schema_tables(self):
        from sqlalchemy import MetaData, Table

        meta = MetaData(schema = self.active_schema)
        for tabName in Progress(self.inspector.get_table_names(schema = self.active_schema)):
            tab = Table(tabName, meta)
            self.inspector.reflecttable(tab, None)
            yield tab

    def set_meta_schema(self, schema):
        from sqlalchemy import MetaData
        import warnings
        from sqlalchemy import exc as sa_exc

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category = sa_exc.SAWarning)
            self.meta = MetaData()
            self.meta.reflect(bind=self.engine, schema = schema, resolve_fks=False, views=True)
