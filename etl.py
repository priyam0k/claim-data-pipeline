import pandas as pd
import logging
from sqlalchemy import create_engine, exc

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# File Path Configuration
RAW_DATA_PATH = 'Car_Insurance_Claim.csv'

# Database Configuration
DB_USER = 'postgres'
DB_PASSWORD = 'mysecretpassword'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'postgres' # Default database
TABLE_NAME = 'claims_data'

# --- ETL Functions ---

def extract_data(file_path):
    """Extracts data from a CSV file."""
    logging.info(f"Starting data extraction from: {file_path}")
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Successfully extracted {len(df)} rows of data.")
        return df
    except FileNotFoundError:
        logging.error(f"Extraction failed: File not found at {file_path}")
        return None

def transform_data(df):
    """Transforms the raw claims data."""
    if df is None:
        logging.warning("Transformation skipped: Input DataFrame is None.")
        return None
        
    logging.info("Starting data transformation...")
    df = df.copy()
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    logging.info("Standardized column names.")
    
    df['age'] = pd.to_numeric(df['age'], errors='coerce')
    logging.info("Cleaned 'age' column.")

    df['credit_score'] = df['credit_score'].fillna(df['credit_score'].median())
    df['annual_mileage'] = df['annual_mileage'].fillna(df['annual_mileage'].median())
    age_median = df['age'].median()
    if pd.isna(age_median):
        age_median = 30 
    df['age'] = df['age'].fillna(age_median)
    logging.info("Handled missing values.")
    
    df['id'] = df['id'].astype(str)
    df['age'] = df['age'].astype(int)
    logging.info("Corrected data types.")
    
    age_bins = [16, 25, 40, 65, 100]
    age_labels = ['16-25', '26-40', '41-65', '65+']
    df['age_group'] = pd.cut(df['age'], bins=age_bins, labels=age_labels, right=False)
    df['had_past_accidents'] = df['past_accidents'].apply(lambda x: 1 if x > 0 else 0)
    logging.info("Engineered new features.")

    df.drop(columns=['age', 'past_accidents'], inplace=True)
    logging.info("Dropped original columns.")
    
    logging.info("Data transformation complete.")
    return df

def load_to_db(df, table_name, db_conn_str):
    """
    Loads a DataFrame into a PostgreSQL database table.
    Returns True on success, False on failure.
    """
    if df is None:
        logging.warning("Loading skipped: Input DataFrame is None.")
        return False

    logging.info(f"Starting data loading to database table: {table_name}")
    try:
        engine = create_engine(db_conn_str)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Successfully loaded {len(df)} rows into '{table_name}'.")
        return True
    except exc.OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during database loading: {e}")
        return False

# --- Main Pipeline Execution ---

def main():
    """Main function to run the ETL pipeline."""
    logging.info("--- Starting ETL Pipeline ---")
    
    # Step 1: Extract
    raw_df = extract_data(RAW_DATA_PATH)
    
    # Step 2: Transform
    clean_df = transform_data(raw_df)
    
    # Step 3: Load to Database
    if clean_df is not None:
        db_connection_str = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        success = load_to_db(clean_df, TABLE_NAME, db_connection_str)
        
        # Only log success if the load function returns True
        if success:
            logging.info("--- ETL Pipeline Finished Successfully ---")
        else:
            logging.error("--- ETL Pipeline Failed During Database Load ---")
    else:
        logging.error("--- ETL Pipeline Failed During Transformation ---")

if __name__ == "__main__":
    main()
