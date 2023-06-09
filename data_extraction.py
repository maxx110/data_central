from database_utils import DatabaseConnector
import pandas as pd
import tabula

class DataExtractor:
    def __init__(self):
        self.data = None
    
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
        df.to_csv("notcleaned_data.csv", index=False)  # Replace with the desired path and filename

        return df 
    
    def retrieve_pdf_data(self,pdf_link):
        # Extract all pages from the PDF and return as a DataFrame
        try:
            pdf_df = tabula.read_pdf(pdf_link, pages='all')
            
            return pdf_df
        except Exception as e:
            print(f"Error extracting data from PDF: {str(e)}")
            



def main():
    # Create an instance of DatabaseConnector
    connector = DatabaseConnector()

    # Initialize the database connection
    connection = connector.init_db_connection()

    # Create an instance of DataExtractor
    extractor = DataExtractor()

    # Define the list of table names
    table_names = ["legacy_store_details"]#, "legacy_users", "orders_table"]

    # Start the extraction process
    extractor.read_rds_table(connection, table_names)



if __name__ == "__main__":
    main()
