import csv
from datetime import date, datetime
from os import listdir, replace, path

from database import Database


class DataLoader():

    def __init__(self):
        # Configurable application values
        # TODO: Move configurable properties to separate file and retrieve via config
        # -----------------------------------------------------------------------------
        # Create database table if it does not already exist when loading data file
        self.create_table = True

        # Data files are named, e.g., data/testformat1_2018-08-03.txt
        self.data_dir = 'data'
        self.data_hold_dir = 'data/hold'
        self.data_processed_dir = 'data/processed'
        self.data_extension = '.txt'

        # Specification files are named, e.g., specs/testformat1.csv
        self.spec_dir = 'specs'
        self.spec_hold_dir = 'specs/hold'
        self.spec_extension = '.csv'

        # CSV format file header expected in spec files
        self.spec_file_header = '"column name",width,datatype'

        # Capture and write activity messages to log file
        self.log_dir = 'logs'
        self.log_message = []

        # Capture and format current timestamp for log entry
        self.begin_processing_timestamp = str(datetime.now().strftime('%Y-%m-%d %H:%M'))

    def __call__(self):
        while True:

            # Get sorted list of all non-empty data files, by directory and file extension
            data_files = [f for f in sorted(listdir(self.data_dir)) if path.isfile(
                path.join(self.data_dir, f)) and f.endswith(self.data_extension)]

            # If no data files to process
            if len(data_files) == 0:
                # Log the visit and exit
                self.log_message.append(self.begin_processing_timestamp +
                                        ' No data files to process')
            else:
                # Log the visit
                self.log_message.append(self.begin_processing_timestamp +
                                        ' Begin processing data files')

                # Prepare dict of data file lists, grouped by fileformat
                grouped_data_files = {}
                for item in data_files:
                    # Strip last underscore, date and file extension
                    key, value = item.rsplit('_', 1)
                    grouped_data_files.setdefault(key, []).append(key + '_' + value)

                # Run processing methods on eligible data files to format and load into database
                for spec in grouped_data_files:
                    self._process_files(spec, grouped_data_files[spec])

                self.log_message.append('\n' + str(datetime.now().strftime('%Y-%m-%d %H:%M')) + ' Exiting')

            # Output log message
            self._write_log_entry(self.log_message)

            break

    # Harvest the format-grouped data files using corresponding spec file
    def _process_files(self, spec: str, data_files: list):
        # Retrieve the spec's schema for processing matching data files
        spec_schema = self._process_spec_file(spec, data_files)

        # Parse data files using the spec's schema
        if spec_schema is not False:
            parsed_data = self._process_data_files(spec_schema, data_files)

            # Enter data into table name matching 'spec' format (e.g., 'testformat1')
            # (_load_data_into_db returned result is list: [boolean, message])
            db_result = self._load_data_into_db(
                spec, spec_schema['column name'], parsed_data, spec_schema, self.create_table)

            self.log_message.append(db_result[1])

            if db_result[0] is True:
                self._move_files(data_files, 'data_processed')

        return

    # Convert spec file csv rows into schema dictionary
    # Return @spec_schema: dict | False
    def _process_spec_file(self, spec: str, data_files: list):

        spec_file = '{dir}/{filename}{extension}'.format(
            dir=self.spec_dir, filename=spec, extension=self.spec_extension)

        self.log_message.append('\nProcessing spec file: ' + spec_file)

        # Check spec directory for corresponding fileformat.csv file
        # No matching spec file found
        if not path.isfile(spec_file):
            self.log_message.append('No spec file found: ' + spec_file)
            self._move_files(data_files, 'data_hold')

            return False

        else:

            # Convert spec file csv into 'spec_schema' dictionary of arrays
            with open(spec_file, 'r', encoding='utf-8') as csvfile:

                # Check first line for proper header format (removing newline)
                header = csvfile.readline().rstrip('\n')

                # If spec file does not contain proper header, log error and return
                if header != self.spec_file_header:
                    csvfile.close()
                    self.log_message.append('Spec file error: incorrect format headers: ')
                    self._move_files([spec + self.spec_extension], 'spec_hold')
                    self._move_files(data_files, 'data_hold')

                    return False

                # Process spec contents into dictionary
                csvfile.seek(0)  # Return to header line
                reader = csv.DictReader(csvfile, skipinitialspace=True)
                spec_schema = {name: [] for name in reader.fieldnames}
                for row in reader:
                    for name in reader.fieldnames:
                        spec_schema[name].append(row[name])

        return spec_schema

    # Apply spec schema to convert data into SQL-friendly values
    # Return @data_rows: list
    def _process_data_files(self, spec_schema: dict, data_files: list):
        # Sum the column widths to determine expected line length of data rows
        line_length = sum[data_files]

        # Container for parsed data, returned after successful processing
        data_rows = []

        for data_file in data_files:
            with open(self.data_dir + '/' + data_file, 'r', encoding='utf-8') as data_content:
                for line in data_content:
                    line = line.rstrip('\n')

                    # Log error if current line length does not match sum of schema column widths
                    if len(line) != line_length:
                        self.log_message.append('Invalid line length (' + str(len(line)) +
                                                ') in data file: ' + data_file)

                    row = []
                    raw_data = []
                    pointer = 0

                    # Extract each column value from the line
                    for index, width in enumerate(spec_schema['width']):
                        # Calculate string slice
                        raw_data = line[pointer:pointer + int(width)]

                        # Apply datatype conversion as needed
                        if spec_schema['datatype'][index] == 'TEXT':
                            # Format as quoted string data
                            # NOTE: Removing trailing whitespace; verify against business reqmts
                            raw_data = '{}'.format(raw_data.rstrip())

                        elif spec_schema['datatype'][index] == 'BOOLEAN':
                            raw_data = bool(int(raw_data))

                        elif spec_schema['datatype'][index] == 'INTEGER':
                            raw_data = int(raw_data)

                        # TODO: Support additional datatypes (e.g., date, char, varchar, float)

                        else:
                            self.log_message.append('Unsupported datatype')

                        # Build the parsed data row
                        row.append(raw_data)

                        # Increment the slice pointer
                        pointer += int(width)

                    # Capture parsed data row (tuples provide parentheses for use in SQL INSERT)
                    data_rows.append(tuple(row))

        # TODO: Add parsed_data validations before returning data_rows

        return data_rows

    # Insert parsed_data values into database table
    def _load_data_into_db(self, table_name: str, columns: list,
                           parsed_data: list, spec_schema: list, create_table: bool):
        try:
            with Database() as db:
                # Retrieve list with [boolean result of insert, message]
                result = db.insert_rows(table_name, columns, parsed_data, spec_schema, create_table)
                return result

        except Exception as e:
            return [False, 'Database exception: ' + str(e)]

    # Move files to another directory, e.g., after successful processing or detected errors
    def _move_files(self, files: list, action: str):
        if action == 'data_processed':
            msg = 'Moving successfully processed data files:'
            src_dir = self.data_dir
            target_dir = self.data_processed_dir

        elif action == 'data_hold':
            msg = 'Moving unprocessed data files for review:'
            src_dir = self.data_dir
            target_dir = self.data_hold_dir

        elif action == 'spec_hold':
            msg = 'Moving incorrectly formatted spec file for review:'
            src_dir = self.spec_dir
            target_dir = self.spec_hold_dir

        else:
            return

        self.log_message.append(msg)

        for file in files:
            # os.replace forces a replacement of existing files in target directory
            # TODO: Rename files if they already exist in target directory
            # (e.g., filename-2[...].ext, filename-3[...].ext), and log the action
            replace(
                '{dir}/{filename}'.format(dir=src_dir, filename=file),
                '{dir}/{filename}'.format(dir=target_dir, filename=file))

            self.log_message.append(' - ' + target_dir + '/' + file)

        return

    def _write_log_entry(self, log_message: list):
        log_file = self.log_dir + '/dataloader_' + str(date.today()) + '.log'

        with open(log_file, 'a', encoding='utf-8') as f:
                f.write('\n'.join(log_message) + '\n\n')
