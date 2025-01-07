import sqlite3
import logging

class Database:
    def __init__(self):
        try:
            self.conn = sqlite3.connect("etl_process.db")
            self.cursor = self.conn.cursor()
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise

    def create_staging_table(self):
        """
        Creates the staging table if it doesn't exist.
        """
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS staging (
                    Customer_Name TEXT,
                    Customer_Id INTEGER PRIMARY KEY,
                    Open_Date DATE,
                    Last_Consulted_Date DATE,
                    Vaccination_Id TEXT,
                    Dr_Name TEXT,
                    State TEXT,
                    Country TEXT,
                    DOB DATE,
                    Is_Active TEXT
                );
            """)
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error creating staging table: {e}")
            raise

    def create_country_table(self, table_name):
        """
        Creates a country-specific table if it doesn't exist.
        """
        try:
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    Customer_Name TEXT,
                    Customer_Id INTEGER PRIMARY KEY,
                    Open_Date DATE,
                    Last_Consulted_Date DATE,
                    Vaccination_Id TEXT,
                    Dr_Name TEXT,
                    State TEXT,
                    Country TEXT,
                    DOB DATE,
                    Is_Active TEXT,
                    Age INTEGER,
                    Days_Since_Last_Consulted INTEGER
                );
            """)
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error creating table {table_name}: {e}")
            raise

    def insert_staging_data(self, row):
        """
        Inserts data into the staging table.
        """
        try:
            self.cursor.execute("""
                INSERT INTO staging (Customer_Name, Customer_Id, Open_Date, Last_Consulted_Date, Vaccination_Id, Dr_Name, State, Country, DOB, Is_Active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['Customer_Name'],
                row['Customer_Id'],
                row['Open_Date'],
                row['Last_Consulted_Date'],
                row['Vaccination_Id'],
                row['Dr_Name'],
                row['State'],
                row['Country'],
                row['DOB'],
                row['Is_Active']
            ))
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error inserting data into staging: {e}")
            raise

    def insert_country_data(self, table_name, row):
        """
        Inserts data into the country-specific table.
        """
        try:
            self.cursor.execute(f"""
                INSERT INTO {table_name} (Customer_Name, Customer_Id, Open_Date, Last_Consulted_Date, Vaccination_Id, Dr_Name, State, Country, DOB, Is_Active, Age, Days_Since_Last_Consulted)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['Customer_Name'],
                row['Customer_Id'],
                row['Open_Date'],
                row['Last_Consulted_Date'],
                row['Vaccination_Id'],
                row['Dr_Name'],
                row['State'],
                row['Country'],
                row['DOB'],
                row['Is_Active'],
                row['Age'],
                row['Days_Since_Last_Consulted']
            ))
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error inserting data into {table_name}: {e}")
            raise

