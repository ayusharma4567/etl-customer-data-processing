import pandas as pd
from database import Database
from utils import validate_row_data, validate_country_data, calculate_derived_columns
import logging
from datetime import datetime

# Set up logging for better error tracking
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_staging_data(csv_file_path):
    """
    Load data from CSV to staging table after validation.
    """
    db = Database()
    try:
        # Create staging table first
        db.create_staging_table()

        # Read CSV data in chunks to avoid memory overload
        chunksize = 100000  # Adjust this based on available memory
        for chunk in pd.read_csv(csv_file_path, delimiter="|", chunksize=chunksize):
            # Convert date columns to proper format (YYYY-MM-DD)
            chunk['Open_Date'] = pd.to_datetime(chunk['Open_Date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
            chunk['Last_Consulted_Date'] = pd.to_datetime(chunk['Last_Consulted_Date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
            chunk['DOB'] = pd.to_datetime(chunk['DOB'], format='%d%m%Y').dt.strftime('%Y-%m-%d')

            # Insert data into staging table with validation
            for _, row in chunk.iterrows():
                if validate_row_data(row, db.cursor):
                    db.insert_staging_data(row)
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise

    return db

def process_country_data(db, df):
    """
    Process the country data and insert it into respective country tables.
    """
    try:
        # Get the Latest Record per Customer based on Last_Consulted_Date
        latest_records = pd.read_sql_query("""
            SELECT 
                Customer_Id, 
                MAX(Last_Consulted_Date) AS Last_Consulted_Date
            FROM staging
            GROUP BY Customer_Id;
        """, db.conn)  # Ensure db.conn is passed

        # Merge to get full data for the latest records
        latest_data = pd.merge(
            latest_records,
            df,
            on=["Customer_Id", "Last_Consulted_Date"],
            how="left"
        ).drop_duplicates(subset=["Customer_Id"])

        # Process and insert data into country-specific tables
        countries = latest_data["Country"].unique()
        for country in countries:
            table_name = f"table_{country}"
            db.create_country_table(table_name)  # Create the table

            # Filter and copy country data
            country_data = latest_data[latest_data["Country"] == country].copy()

            # Add derived columns
            derived_data = country_data.apply(
                lambda row: calculate_derived_columns(row["DOB"], row["Last_Consulted_Date"]),
                axis=1
            )
            country_data["Age"], country_data["Days_Since_Last_Consulted"] = zip(*derived_data)

            # Validate and insert
            for _, row in country_data.iterrows():
                if validate_country_data(row):
                    try:
                        db.insert_country_data(table_name, row)
                    except sqlite3.IntegrityError:
                        logging.warning(f"Duplicate Customer_ID: {row['Customer_Id']}. Skipping...")
                    except Exception as e:
                        logging.error(f"Error inserting data into {table_name}: {e}")

    except Exception as e:
        logging.error(f"Error processing country data: {e}")
        raise


def main():
    try:
        # Load and validate staging data
        csv_file_path = "data/customer_data.csv"
        db = load_staging_data(csv_file_path)

        # Process and insert country-specific data
        df = pd.read_sql("SELECT * FROM staging", db.conn)  # Fetch data from staging
        process_country_data(db, df)  # Pass both db and df to the function

    except Exception as e:
        logging.error(f"ETL process failed: {e}")
    finally:
        if 'db' in locals() and db.conn:
            db.conn.close()


if __name__ == "__main__":
    main()



