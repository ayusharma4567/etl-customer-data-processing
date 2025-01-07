import sqlite3

def read_from_db(db_file):
    # Connect to the SQLite database
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        print("Connected to the database successfully.")

        # Example: Fetch all records from a table
        query = "SELECT * FROM table_AU"  # Replace 'your_table_name' with your actual table name
        cursor.execute(query)

        # Fetch all the rows of the query result
        rows = cursor.fetchall()

        # Iterate through rows and print data
        for row in rows:
            print(row)

    except sqlite3.Error as e:
        print(f"Error while reading from the database: {e}")

    finally:
        if conn:
            conn.close()
            print("Connection closed.")

# Replace with the actual path to your .db file
db_file = 'etl_process.db'
read_from_db(db_file)
