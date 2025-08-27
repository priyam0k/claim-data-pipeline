import pandas as pd
import logging

# --- Configuration ---
# Set up basic logging to see the pipeline's progress and any potential issues.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define file paths
RAW_DATA_PATH = 'Car_Insurance_Claim.csv'
CLEAN_DATA_PATH = 'clean_car_insurance_claim.csv' 

# --- ETL Functions ---

def extract_data(file_path):
    """
    Extracts data from a CSV file into a pandas DataFrame.
    
    Args:
        file_path (str): The path to the CSV file.
        
    Returns:
        pandas.DataFrame: The extracted data.
    """
    logging.info(f"Starting data extraction from: {file_path}")
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Successfully extracted {len(df)} rows of data.")
        return df
    except FileNotFoundError:
        logging.error(f"Extraction failed: File not found at {file_path}")
        return None

def transform_data(df):
    """
    Transforms the raw claims data by cleaning, standardizing, and feature engineering.
    
    Args:
        df (pandas.DataFrame): The raw data DataFrame.
        
    Returns:
        pandas.DataFrame: The transformed and cleaned data.
    """
    if df is None:
        logging.warning("Transformation skipped: Input DataFrame is None.")
        return None
        
    logging.info("Starting data transformation...")
    
    # Make a copy to avoid SettingWithCopyWarning
    df = df.copy()

    # 1. Standardize Column Names
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    logging.info("Standardized column names.")
    
    # 2. Pre-emptively clean the 'age' column before binning
    # The error occurs because some 'age' values are strings.
    # We convert the column to numeric, coercing errors into 'NaN' (Not a Number).
    df['age'] = pd.to_numeric(df['age'], errors='coerce')
    logging.info("Cleaned 'age' column by coercing non-numeric values to NaN.")

    # 3. Handle Missing Values
    # Fill NaNs in numerical columns with their respective medians.
    # This now includes any NaNs created from the 'age' cleaning step.
    df['credit_score'] = df['credit_score'].fillna(df['credit_score'].median())
    df['annual_mileage'] = df['annual_mileage'].fillna(df['annual_mileage'].median())
    
    age_median = df['age'].median()
    if pd.isna(age_median):
        logging.warning("Could not calculate median age. Filling missing ages with 30.")
        age_median = 30
    df['age'] = df['age'].fillna(age_median)
    
    logging.info("Handled missing values in 'credit_score', 'annual_mileage', and 'age'.")
    
    # 4. Correct Data Types
    df['id'] = df['id'].astype(str)
    # Ensure 'age' is an integer after filling NaNs, for the cutting function.
    df['age'] = df['age'].astype(int)
    logging.info("Corrected data types.")
    
    # 5. Feature Engineering
    # This will now work correctly as the 'age' column is clean and numeric.
    age_bins = [16, 25, 40, 65, 100]
    age_labels = ['16-25', '26-40', '41-65', '65+']
    df['age_group'] = pd.cut(df['age'], bins=age_bins, labels=age_labels, right=False)
    
    df['had_past_accidents'] = df['past_accidents'].apply(lambda x: 1 if x > 0 else 0)
    logging.info("Engineered new features: 'age_group' and 'had_past_accidents'.")

    # 6. Drop the original columns that we've replaced or are no longer needed
    df.drop(columns=['age', 'past_accidents'], inplace=True)
    logging.info("Dropped original columns: 'age', 'past_accidents'.")
    
    logging.info("Data transformation complete.")
    return df

# --- Main Pipeline Execution ---

def main():
    """
    Main function to run the ETL pipeline.
    """
    logging.info("--- Starting ETL Pipeline ---")
    
    # Step 1: Extract
    raw_df = extract_data(RAW_DATA_PATH)
    
    # Step 2: Transform
    clean_df = transform_data(raw_df)
    
    # Step 3: Load (for now, to a CSV)
    if clean_df is not None:
        logging.info(f"Starting data loading to: {CLEAN_DATA_PATH}")
        clean_df.to_csv(CLEAN_DATA_PATH, index=False)
        logging.info("Data loading complete.")
        logging.info("--- ETL Pipeline Finished Successfully ---")
        
        # Display the first 5 rows of the new, clean DataFrame to verify
        print("\n--- First 5 Rows of Cleaned Data ---")
        print(clean_df.head())
    else:
        logging.error("--- ETL Pipeline Failed ---")

if __name__ == "__main__":
    main()
