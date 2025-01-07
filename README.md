# ETL Customer Data Processing

## Description
This project implements an ETL pipeline to partition customer data into country-specific tables. It includes:
- Parsing customer data files.
- Loading data into a staging table.
- Partitioning data into country-specific tables in SQLite.

## File Structure
etl-customer-data-processing/
├── src/
│   ├── main.py               # Your main Python script
│   ├── utils.py              # Helper functions (e.g., age calculation)
│   ├── database.py           # Database setup and queries
├── README.md
└── requirements.txt

##Usage
Place your country-specific CSV files in the data/ directory.
Install the libraries if not install in requirements.txt, run:
 pip install -r requirements.txt
Run the ETL pipeline:
 python src/main.py

