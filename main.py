import os
import pandas as pd
from supabase import create_client, Client
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Supabase credentials from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Set up logging
# Create reports directory if it doesn't exist
if not os.path.exists('reports'):
    os.makedirs('reports')

# Generate a timestamped filename for the log file
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f'reports/update_products_log_{timestamp}.txt'

# Set up logging
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(message)s')

def read_excel(file_path):
    df = pd.read_excel(file_path)
    return df[['id', 'sku']]

def update_products(dataframe):
    ids = dataframe['id'].tolist()
    skus = dataframe['sku'].tolist()
    
    # Fetch products from Supabase
    response = supabase.table('Products').select('id, sku').in_('id', ids).execute()
    supabase_data = response.data
    supabase_ids = [item['id'] for item in supabase_data]
    supabase_skus = [item['sku'] for item in supabase_data]
    
    found_ids = []
    not_found_ids = []
    sku_mismatch = []

    for idx, row in dataframe.iterrows():
        if row['id'] in supabase_ids:
            found_ids.append(row['id'])
            if row['sku'] in supabase_skus:
                supabase.table('Products').update({
                    'preorder': False,
                    'preorder_discount': None,
                    'preorder_date': None
                }).eq('id', row['id']).execute()
                print(f"Updated ID: {row['id']}, SKU: {row['sku']}")
            else:
                sku_mismatch.append(row['sku'])
                print(f"SKU mismatch for ID: {row['id']}, SKU: {row['sku']}")
        else:
            not_found_ids.append(row['id'])
            print(f"ID not found: {row['id']}")
    
    log_report(found_ids, not_found_ids, sku_mismatch)
    print("Program finished. Logging report.")

def log_report(found_ids, not_found_ids, sku_mismatch):
    logging.info(f"Found IDs: {found_ids}")
    logging.info(f"Not Found IDs: {not_found_ids}")
    logging.info(f"SKU Mismatches: {sku_mismatch}")

def main():
    # Path to the Excel file
    file_path = 'Excels/F-10.xlsx'
    
    # Read the Excel file
    dataframe = read_excel(file_path)
    
    # Update products in Supabase
    update_products(dataframe)

if __name__ == "__main__":
    main()
