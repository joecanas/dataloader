from os import path
import shutil
import unittest

from dataloader import DataLoader
from database import Database


class TestDataLoader(unittest.TestCase):

    def test_exercise_example(self):

        specfile = 'specs/testformat1.csv'
        # "column name",width,datatype
        # name,10,TEXT
        # valid,1,BOOLEAN
        # count,3,INTEGER

        datafile = 'data/testformat1_2015-06-28.txt'
        # Foonyor   1  1
        # Barzane   0-12
        # Quuxitude 1103

        datafile_processed = 'data/processed/testformat1_2015-06-28.txt'

        table_name = 'testformat1'

        columns = "name, valid, count"

        spec_schema = {
            'column name': ['name', 'valid', 'count'],
            'width': ['10', '1', '3'],
            'datatype': ['TEXT', 'BOOLEAN', 'INTEGER']}

        result_rows = [
            ('Foonyor', True, 1),
            ('Barzane', False, -12),
            ('Quuxitude', True, 103)
        ]

        # copy specfile and datafile from tests/ directory
        shutil.copy2('tests/' + specfile, specfile)
        shutil.copy2('tests/' + datafile, datafile)

        db = Database()

        # drop and create table
        db.remove_table(table_name)
        db.create_table(table_name, spec_schema)

        # launch application
        dataloader = DataLoader()
        dataloader()

        # check database for data
        db_data = db.select_rows(columns, table_name)

        self.assertEqual(db_data, result_rows)

        # verify data file was moved from data/ to data/processed/
        # data/ directory is empty
        self.assertFalse(path.isfile(datafile))

        # data/processed/ directory has file
        self.assertTrue(path.isfile(datafile_processed))

        # clean up the environment
        db.remove_table(table_name)


# testing = TestDataLoader()
# testing.test_exercise_example()

if __name__ == '__main__':
    unittest.main()
