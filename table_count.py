import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('etl_process.db')

# Create a cursor object
cursor = conn.cursor()

# Query to count the number of tables
cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table';")
table_count = cursor.fetchone()[0]

print(f"Number of tables: {table_count}")

# Close the connection
conn.close()
