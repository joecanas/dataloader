import psycopg2

from config import config


class Database():

    def __init__(self):
        # Default config() values: filename='database.cfg', section='postgresql'
        params = config()
        self.conn = psycopg2.connect(**params)

        # Autocommit implemented for convenience during development
        # Consider benchmarking insert_rows method using explicit commit()
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cursor.close()
        self.conn.close()

    def create_table(self, table_name: str, spec_schema: list):
        columns = []

        # Extract each column value from the line
        for index, column_name in enumerate(spec_schema['column name']):
            # Apply datatype conversion as needed
            #   column_name_1 DATATYPE,
            #   column_name_2 DATATYPE [...]
            # TODO: Modify column creation to include NOT NULL attributes
            # plus other 'required' columns (e.g., ID, created, drop_date)
            columns.append(column_name + ' ' + spec_schema['datatype'][index])

        query = 'CREATE TABLE IF NOT EXISTS {table_name} ({columns});'.format(
            table_name=table_name, columns=', '.join(columns))

        try:
            self.cursor.execute(query)

            return [True, 'Table created: ' + table_name]

        except Exception as e:
            return [False, 'Database exception: Could not create table ' +
                    table_name + ' (' + str(e) + ')']

    def insert_rows(self, table_name: str, columns: list,
                    data_rows: list, spec_schema: list, create_table: bool):

        create_table_message = []

        table_exists_result = self.table_exists(table_name)

        # Table not found:
        if table_exists_result[0] is False:

            # If app's configurable value is true, create table for new fileformat/spec file
            if create_table is True:
                create_table_result = self.create_table(table_name, spec_schema)

                # Table creation failed: cannot insert data
                if create_table_result[0] is False:
                    return create_table_result
                else:
                    create_table_message.append('Table created: ' + table_name + '\n')

        # Table exists:
        # Convert list of columns to SQL-friendly column string
        columns_for_sql = ', '.join(columns)

        # Convert list of tuples to SQL-friendly VALUES for bulk insertion
        data_rows_for_sql = ', '.join(map(str, data_rows))

        # Bulk load method #1:
        # Prepare single insert statement with multiple VALUES tuples
        query = 'INSERT INTO {table_name} ({columns}) VALUES {data_rows}'.format(
            table_name=table_name, columns=columns_for_sql, data_rows=data_rows_for_sql)

        try:
            self.cursor.execute(query)
            return [True, str(create_table_message[0]) + 'Data entered into ' + table_name]

        except Exception as e:
            return [False, str(create_table_message[0]) +
                    'Database exception: Could not insert data into table: ' +
                    table_name + ' (' + str(e) + ')']

        # Bulk load method #2:
        # postgreSQL + StringIO + copy_from to create in-memory file for inserts
        # Requires import statement:  from io import StringIO
        # Potential performance improvement for larger data loads
        # -----------------------------------------------------------------------
        # f = StringIO()
        # f.write('\n'.join('\t'.join(str(field) for field in row) for row in data_rows))
        # f.seek(0)
        # self.cur.copy_from(f, table_name, null='null')

    def table_exists(self, table_name: str):
        query = 'SELECT relname FROM pg_class WHERE relname = "{table_name}");'.format(
            table_name=table_name)

        try:
            self.cursor.execute(query)

            if self.cursor.fetchone() is not None:
                return [True, 'Table exists: ' + table_name]
            else:
                return [False, 'Database error: Table does not exist: ' + table_name]

        except Exception as e:
            return [False, 'Database exception: ' + str(e)]

    # Convenience method for tests
    def select_rows(self, columns: str, table_name: str):
        query = "SELECT {columns} from {table_name};".format(columns=columns, table_name=table_name)

        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            return rows

        except Exception as e:
            return [False, 'Database exception: query failed:\n' + query + '\n' + str(e)]

    # Convenience method for tests
    def remove_table(self, table_name: str):
        query = 'DROP TABLE {table_name};'.format(table_name=table_name)

        try:
            self.cursor.execute(query)
            return [True, 'Table removed from database: ' + table_name]

        except Exception as e:
            return [False, 'Database exception: Could not remove table: ' +
                    table_name + ' (' + str(e) + ')']
