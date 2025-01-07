from datetime import datetime

def validate_staging_data(row, cursor):
    """
    Validates the data before inserting into the staging table.
    """
    try:
        # Validate Date Format
        datetime.strptime(row['Open_Date'], '%Y-%m-%d')
        datetime.strptime(row['Last_Consulted_Date'], '%Y-%m-%d')
        datetime.strptime(row['DOB'], '%Y-%m-%d')
    except ValueError:
        logging.warning(f"Invalid date format for Customer_ID: {row['Customer_Id']}. Skipping...")
        return False
    
    # Validate Is_Active
    if 'Is_Active' not in row:
        logging.warning(f"Missing 'Is_Active' for Customer_ID: {row['Customer_Id']}. Skipping...")
        return False
    if row['Is_Active'] not in ['A', 'I']:
        logging.warning(f"Invalid Is_Active value for Customer_ID: {row['Customer_Id']}. Skipping...")
        return False
    
    # Validate Country
    if not row['Country']:
        logging.warning(f"Missing Country for Customer_ID: {row['Customer_Id']}. Skipping...")
        return False

    # Validate Customer_Id uniqueness in the staging
    existing = cursor.execute("SELECT COUNT(*) FROM staging WHERE Customer_Id = ?", (row['Customer_Id'],)).fetchone()
    if existing[0] > 0:
        logging.warning(f"Duplicate Customer_ID: {row['Customer_Id']}. Skipping...")
        return False
    
    return True

def validate_country_data(row):
    """
    Validates the data before inserting into the country-specific tables.
    """
    # Validate Age (non-negative)
    if row['Age'] < 0:
        logging.warning(f"Invalid Age for Customer_ID: {row['Customer_Id']}. Skipping...")
        return False
    
    # Validate Days_Since_Last_Consulted (non-negative)
    if row['Days_Since_Last_Consulted'] < 0:
        logging.warning(f"Invalid Days_Since_Last_Consulted for Customer_ID: {row['Customer_Id']}. Skipping...")
        return False
    
    # Validate Last_Consulted_Date > Open_Date
    if datetime.strptime(row['Last_Consulted_Date'], '%Y-%m-%d') < datetime.strptime(row['Open_Date'], '%Y-%m-%d'):
        logging.warning(f"Last_Consulted_Date is earlier than Open_Date for Customer_ID: {row['Customer_Id']}. Skipping...")
        return False
    
    return True

def calculate_derived_columns(dob, last_consulted_date):
    """
    Calculates the age and days since last consulted.
    """
    dob_date = datetime.strptime(dob, '%Y-%m-%d')
    last_consulted_date = datetime.strptime(last_consulted_date, '%Y-%m-%d')

    # Calculate Age
    today = datetime.today()
    age = today.year - dob_date.year - ((today.month, today.day) < (dob_date.month, dob_date.day))

    # Calculate Days Since Last Consulted
    days_since_last_consulted = (today - last_consulted_date).days

    return age, days_since_last_consulted
