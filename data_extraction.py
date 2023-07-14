from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import json
import boto3
import os

class DataExtractor:
    def __init__(self):
        self.data = None

    def list_number_stores(self,number_of_stores_endpoint,api_key):
        headers = {"x-api-key": api_key}
        result = requests.get(number_of_stores_endpoint,headers=headers)
        data = result.json()
        
        return data['number_stores']

    def retrieve_store_data(self,retrieve_store,api_key,store_numbers):
        #total_store_number= self.list_number_stores()
        store_data = []
        headers = {"x-api-key": api_key}

        for store_number in range(1, store_numbers):
            url = retrieve_store + str(store_number)
            store_data_result = requests.get(url, headers=headers)
            store_json = store_data_result.json()

            # Check if the store data is not empty
            if store_json:
                store_data.append(store_json)

        return store_data
    
    def read_rds_table(self, connection, table_names):
        # Create cursor
        cursor = connection.cursor()

        # Iterate over table names
        for table_name in table_names:
            # SQL query
            query = f"SELECT * FROM {table_name}"

            # Execute query
            cursor.execute(query)

            # Fetch all results
            results = cursor.fetchall()

            # Create DataFrame
            df = pd.DataFrame(results)
            print(f"Table: {table_name}")
            
            print()

        # Close cursor and connection
        cursor.close()
        connection.close()
        
        # Save the cleaned data to a file
        df.to_csv("notcleaned_data1.csv", index=False)  # Replace with the desired path and filename

        return df 
    
    def retrieve_pdf_data(self,pdf_link):
        # Extract all pages from the PDF and return as a DataFrame
        try:
            pdf_df = tabula.read_pdf(pdf_link, pages='all')
            
            return pdf_df
        except Exception as e:
            print(f"Error extracting data from PDF: {str(e)}")

    def extract_from_s3(self, bucket_address):
        # Split the bucket address into bucket name and object key
        bucket_parts = bucket_address.replace("s3://", "").split("/")
        bucket_name = bucket_parts[0]
        object_key = "/".join(bucket_parts[1:])

        # Create an S3 client
        s3_client = boto3.client("s3")

        # Download the object from S3 to a temporary file
        tmp_file_path = os.path.join(os.getcwd(), "products.csv")
        try:
            s3_client.download_file(bucket_name, object_key, tmp_file_path)
        except Exception as e:
            print(f"Error downloading file from S3: {str(e)}")
            return None

        # Read the downloaded CSV file into a Pandas DataFrame
        try:
            df = pd.read_csv(tmp_file_path)
            return df
        except Exception as e:
            print(f"Error reading CSV file: {str(e)}")
            return None






def main():
    # Create an instance of DatabaseConnector
    connector = DatabaseConnector()

    # Initialize the database connection
    connection = connector.init_db_connection()

    # Create an instance of DataExtractor
    extractor = DataExtractor()

    # Define the list of table names
    #table_names = ["legacy_store_details"]#, "legacy_users", "orders_table"]

    table_names = ["orders_table"]

    #Start the extraction process
    extractor.read_rds_table(connection, table_names)

    #create instance of the api
    number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    
    # api_cred_con= DatabaseConnector()
    # api_cred = api_cred_con.read_db_creds()
    # api_key = api_cred['x-api-key']

    # store_numbers = extractor.list_number_stores(number_of_stores_endpoint,api_key)
    # retrieve_store_endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'

    # store_data= extractor.retrieve_store_data(retrieve_store_endpoint,api_key,store_numbers)

    # # Create a dataframe from the store data
    # store_dataframe = pd.json_normalize(store_data)

    # # Save the dataframe to a CSV file
    # store_dataframe.to_csv('store_data.csv', index=False)

    

   



if __name__ == "__main__":
    main()
