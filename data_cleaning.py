import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

class DataCleaning:
    def __init__(self):
        self.cleaned_data = None
    
    def remove_null_values(self, df):
        # Remove rows with null values
        cleaned_df = df.fillna("NA")

        # Update the cleaned data attribute
        self.cleaned_data = cleaned_df
        
        # Clean the address column
        cleaned_df[1] = cleaned_df[1].apply(lambda address: " ".join(address.splitlines()))
        
        return cleaned_df
    
    def remove_null_pdf(self, pdf_data):
        # Remove rows with null values
        pdf_data = pdf_data.dropna(thresh=len(pdf_data.columns) - 3)
        # Remove rows with more than 2 "NULL" values
        pdf_data = pdf_data.drop(pdf_data[pdf_data.apply(lambda row: row.str.count("NULL")).sum(axis=1) > 2].index)

        return pdf_data

    def convert_product_weights(self, product_df):
        # Clean up the weight column
        product_df['weight'] = product_df['weight'].astype(str)
        product_df['weight'] = product_df['weight'].str.replace(r'[^0-9.]+', '')
        # Convert weights to decimal values representing kg
        def convert_weight(value):
            try:
                weight = float(value)
                if 'g' in value or 'ml' in value:
                    weight /= 1000
                return weight
            except ValueError:
                return value

        product_df['weight'] = product_df['weight'].apply(convert_weight)

        # # convert to decimal
        # product_df['weight'] = product_df['weight'].apply(lambda x: x.replace('kg', '') if isinstance(x, str) and 'kg' in x else x)
        # product_df['weight'] = product_df['weight'].apply(lambda x: x.replace('g', '') if isinstance(x, str) and 'g' in x else x)
        # product_df['weight'] = product_df['weight'].apply(lambda x: x.replace('ml', '') if isinstance(x, str) and 'ml' in x else x)
        # product_df['weight'] = product_df['weight'].apply(lambda x: float(x) / 1000 if 'g' in x or 'ml' in x else float(x))


        #product_df['weight'] = product_df['weight'].apply(lambda x: float(x) / 1000 if isinstance(x, str) and ('g' in x or 'ml' in x) else float(x))

        

        return product_df 


    
def main():
    # Create an instance of DataExtractor
    connection = DatabaseConnector()

    # Initialize the database connection
    connection = connection.init_db_connection()

    # Create an instance of DataExtractor
    extractor = DataExtractor()
    # Define the list of table names
    table_names = ["legacy_store_details"]  # Modify with the desired table names
    new_table_name = 'dim_users'
    pdf_table_name = 'dim_card_details'
    
    #link to pdf
    pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    
    # Start the extraction process
    df = extractor.read_rds_table(connection,table_names)

    upload_connect = DatabaseConnector()
    
    # Create an instance of DataCleaning
    cleaner = DataCleaning()

    # Perform data cleaning
    cleaned_df = cleaner.remove_null_values(df)

    # Save the cleaned data to a file
    cleaned_df.to_csv("cleaned_data.csv", index=False)  # Replace with the desired path and filename

    #upload to database
    upload = upload_connect.upload_to_db(cleaned_df,new_table_name)

    #extract pdf
    extract_pdf = DataExtractor()
    pdf_upload = extract_pdf.retrieve_pdf_data(pdf_link)

    # Concatenate the list of DataFrames into a single DataFrame
    pdf_data = pd.concat(pdf_upload)

    #clean pdf data
    pdf_cleaned_data = cleaner.remove_null_pdf(pdf_data)

    #upload pdf to database
    upload_pdf = upload_connect.upload_pdf_to_db(pdf_cleaned_data,pdf_table_name)

    # # Save the cleaned data to a file
    # pdf_data.to_csv("pdf_data.csv", index=False)  # Replace with the desired path and filename

    # # Save the cleaned data to a file
    # pdf_cleaned_data.to_csv("pdf_cleaned_data.csv", index=False)  # Replace with the desired path and filename


    #Saving from s3 with bucket name
    bucket_address = 's3://data-handling-public/products.csv'
    product_df = extractor.extract_from_s3(bucket_address)

    product_df = cleaner.convert_product_weights(product_df)
    print(product_df)

if __name__ == "__main__":
    main()
