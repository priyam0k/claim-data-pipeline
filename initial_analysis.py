import pandas as pd

# Load the dataset
# Make sure 'Car_Insurance_Claim.csv' is in the same folder as this script
try:
    df = pd.read_csv('Car_Insurance_Claim.csv')

    print("--- First 5 Rows of the Dataset ---")
    print(df.head())
    print("\n" + "="*50 + "\n")

    print("--- Dataset Information (Data Types & Non-Null Counts) ---")
    df.info()
    print("\n" + "="*50 + "\n")

    print("--- Summary Statistics for Numerical Columns ---")
    print(df.describe())
    print("\n" + "="*50 + "\n")

    print("--- Missing Value Counts ---")
    print(df.isnull().sum())
    print("\n" + "="*50 + "\n")

except FileNotFoundError:
    print("Error: 'Car_Insurance_Claim.csv' not found.")
    print("Please make sure the CSV file is in the same directory as the script.")