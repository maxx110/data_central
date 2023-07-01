import yaml
import psycopg2



class DatabaseConnector:
    def __init__(self):
        self.data = None
    
    def read_db_creds(self):
        try:
            with open('C:/Users/maxwe/OneDrive/Desktop/aicore/mutistuff/data_central/db_creds.yaml', 'r') as file:
                credentials = yaml.safe_load(file)
                print("Credentials loaded successfully from file:")
                #print(credentials)
                return credentials
        except IOError:
            print("Error: Unable to read YAML credentials file:")
        except yaml.YAMLError as e:
            print("Error: Invalid YAML format in file:")
            print("YAML Error:", str(e))

    def init_db_connection(self):
        creds = self.read_db_creds()

        # Extract the required credentials
        db_host = creds['RDS_HOST']
        db_user = creds['RDS_USER']
        db_password = creds['RDS_PASSWORD']
        db_name = creds['RDS_DATABASE']
        db_port = creds['RDS_PORT']

        # Create a connection to the database
        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )

        return connection
    
    def init_db_connection_to_new(self):
        creds = self.read_db_creds()

        # Extract the required credentials
        host = creds['HOST']
        user = creds['USER']
        password = creds['PASSWORD']
        name = creds['DATABASE']
        port = creds['PORT']

        # Create a connection to the database
        new_connection = psycopg2.connect(
            host=host,
            port=port,
            database=name,
            user=user,
            password=password
        )

        return new_connection

    def upload_to_db(self, dataframe, table_name):
        connection=self.init_db_connection_to_new()
        if connection is None or connection.closed != 0:
            print("Error: No active database connection.")
            return

        # Create a cursor to execute SQL queries
        cursor = connection.cursor()

          # Convert all values in the DataFrame to strings
        dataframe = dataframe.astype(str)

        # Check if the table exists
        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            # Generate the SQL query to create the table
            create_table_query = f"CREATE TABLE {table_name} (" \
                                f"column_0 VARCHAR, " \
                                f"column_1 VARCHAR, " \
                                f"column_2 VARCHAR, " \
                                f"column_3 VARCHAR, " \
                                f"column_4 VARCHAR, " \
                                f"column_5 VARCHAR, " \
                                f"column_6 VARCHAR, " \
                                f"column_7 VARCHAR, " \
                                f"column_8 VARCHAR, " \
                                f"column_9 VARCHAR, " \
                                f"column_10 VARCHAR," \
                                f"column_11 VARCHAR" \
                                f")"

            # Execute the CREATE TABLE query
            cursor.execute(create_table_query)
            connection.commit()
            print(f"Table '{table_name}' created successfully.")

        # Convert the DataFrame to a list of tuples for bulk insert
        records = [tuple(row) for row in dataframe.values]

        # Generate the SQL query for bulk insert
        columns = ', '.join('column_' + str(i) for i in range(len(dataframe.columns)))
        values_template = ', '.join(['%s'] * len(dataframe.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_template})"

        try:
            # Execute the bulk insert query
            cursor.executemany(insert_query, records)
            connection.commit()
            print(f"Successfully uploaded {len(records)} records to table '{table_name}'.")
        except psycopg2.Error as e:
            connection.rollback()
            print(f"Error uploading records to table '{table_name}':")
            print(str(e))

        # Close the cursor
        cursor.close()


    def upload_pdf_to_db(self, dataframe, pdf_table_name):
        connection=self.init_db_connection_to_new()
        if connection is None or connection.closed != 0:
            print("Error: No active database connection.")
            return

        # Create a cursor to execute SQL queries
        cursor = connection.cursor()

          # Convert all values in the DataFrame to strings
        dataframe = dataframe.astype(str)

        # Check if the table exists
        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{pdf_table_name}')")
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            # Generate the SQL query to create the table
            create_table_query = f"CREATE TABLE {pdf_table_name} (" \
                                f"column_0 VARCHAR, " \
                                f"column_1 VARCHAR, " \
                                f"column_2 VARCHAR, " \
                                f"column_3 VARCHAR, " \
                                f"column_4 VARCHAR, " \
                                f"column_5 VARCHAR, " \
                                f"column_6 VARCHAR, " \
                                f"column_7 VARCHAR, " \
                                f"column_8 VARCHAR, " \
                                f"column_9 VARCHAR, " \
                                f"column_10 VARCHAR," \
                                f"column_11 VARCHAR" \
                                f")"

            # Execute the CREATE TABLE query
            cursor.execute(create_table_query)
            connection.commit()
            print(f"Table '{pdf_table_name}' created successfully.")

        # Convert the DataFrame to a list of tuples for bulk insert
        records = [tuple(row) for row in dataframe.values]

        # Generate the SQL query for bulk insert
        columns = ', '.join('column_' + str(i) for i in range(len(dataframe.columns)))
        values_template = ', '.join(['%s'] * len(dataframe.columns))
        insert_query = f"INSERT INTO {pdf_table_name} ({columns}) VALUES ({values_template})"

        try:
            # Execute the bulk insert query
            cursor.executemany(insert_query, records)
            connection.commit()
            print(f"Successfully uploaded {len(records)} records to table '{pdf_table_name}'.")
        except psycopg2.Error as e:
            connection.rollback()
            print(f"Error uploading records to table '{pdf_table_name}':")
            print(str(e))

        # Close the cursor
        cursor.close()

    def list_tables(self):
        t_connection = self.init_db_connection()
        if t_connection is None or t_connection.closed != 0:
            print("Error: No active database connection.")
            return

        cursor = t_connection.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = cursor.fetchall()
        cursor.close()

        if tables:
            print("Tables in the database:")
            for table in tables:
                print(table[0])
        else:
            print("No tables found in the database.")    

    def start(self):
        # self.list_tables()
        self.init_db_connection()

# Example usage:
if __name__ == "__main__":
    # Create an instance of DataExtractor
    extractor = DatabaseConnector()

    # Start the extraction process
    #extractor.start()

    extractor.list_tables()